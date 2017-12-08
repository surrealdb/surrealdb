// Copyright © 2016 Abcum Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package db

import (
	"context"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
)

func (d *document) check(ctx context.Context, cond sql.Expr) (ok bool, err error) {

	val, err := d.i.e.fetch(ctx, cond, d.current)
	if val, ok := val.(bool); ok {
		return val, err
	}

	return true, err

}

// Grant checks to see if the table permissions allow
// this record to be accessed for live queries, and
// if not then it errors accordingly.
func (d *document) grant(ctx context.Context, when method) (ok bool, err error) {

	var val interface{}

	// If we are authenticated using DB, NS,
	// or KV permissions level, then we can
	// ignore all permissions checks, but we
	// must ensure the TB, DB, and NS exist.

	if k, ok := ctx.Value(ctxKeyKind).(cnf.Kind); ok {
		if k < cnf.AuthSC {
			return true, nil
		}
	}

	// Otherwise, get the table definition
	// so we can check if the permissions
	// allow us to view this document.

	tb, err := d.getTB()
	if err != nil {
		return false, err
	}

	// Once we have the table we reset the
	// context to DB level so that no other
	// embedded permissions are checked on
	// records within these permissions.

	ctx = context.WithValue(ctx, ctxKeyKind, cnf.AuthDB)

	// We then try to process the relevant
	// permissions dependent on the query
	// that we are currently processing. If
	// there are no permissions specified
	// for this table, then because this is
	// a scoped request, return an error.

	switch p := tb.Perms.(type) {
	case *sql.PermExpression:
		switch when {
		case _CREATE:
			val, err = d.i.e.fetch(ctx, p.Select, d.current)
		case _UPDATE:
			val, err = d.i.e.fetch(ctx, p.Select, d.current)
		case _DELETE:
			val, err = d.i.e.fetch(ctx, p.Select, d.initial)
		}
	}

	// If the permissions expressions
	// returns a boolean value, then we
	// return this, dictating whether the
	// document is able to be viewed.

	if val, ok := val.(bool); ok {
		return val, err
	}

	// Otherwise as this request is scoped,
	// return an error, so that the
	// document is unable to be viewed.

	return false, err

}

// Query checks to see if the table permissions allow
// this record to be accessed for normal queries, and
// if not then it errors accordingly.
func (d *document) allow(ctx context.Context, when method) (ok bool, err error) {

	var val interface{}

	// If we are authenticated using DB, NS,
	// or KV permissions level, then we can
	// ignore all permissions checks, but we
	// must ensure the TB, DB, and NS exist.

	if k, ok := ctx.Value(ctxKeyKind).(cnf.Kind); ok {
		if k < cnf.AuthSC {
			return true, nil
		}
	}

	// Otherwise, get the table definition
	// so we can check if the permissions
	// allow us to view this document.

	tb, err := d.getTB()
	if err != nil {
		return false, err
	}

	// Once we have the table we reset the
	// context to DB level so that no other
	// embedded permissions are checked on
	// records within these permissions.

	ctx = context.WithValue(ctx, ctxKeyKind, cnf.AuthDB)

	// We then try to process the relevant
	// permissions dependent on the query
	// that we are currently processing. If
	// there are no permissions specified
	// for this table, then because this is
	// a scoped request, return an error.

	switch p := tb.Perms.(type) {
	case *sql.PermExpression:
		switch when {
		case _SELECT:
			val, err = d.i.e.fetch(ctx, p.Select, d.current)
		case _CREATE:
			val, err = d.i.e.fetch(ctx, p.Create, d.current)
		case _UPDATE:
			val, err = d.i.e.fetch(ctx, p.Update, d.current)
		case _DELETE:
			val, err = d.i.e.fetch(ctx, p.Delete, d.current)
		}
	}

	// If the permissions expressions
	// returns a boolean value, then we
	// return this, dictating whether the
	// document is able to be viewed.

	if val, ok := val.(bool); ok {
		return val, err
	}

	// Otherwise as this request is scoped,
	// return an error, so that the
	// document is unable to be viewed.

	return false, err

}

// Event checks if any triggers are specified for this
// table, and executes them in name order.
func (d *document) event(ctx context.Context, when method) (err error) {

	// Get the index values specified
	// for this table, loop through
	// them, and compute the changes.

	evs, err := d.getEV()
	if err != nil {
		return err
	}

	if len(evs) > 0 {

		vars := data.New()
		vars.Set(d.id, varKeyThis)
		vars.Set(d.current.Data(), varKeyAfter)
		vars.Set(d.initial.Data(), varKeyBefore)
		ctx = context.WithValue(ctx, ctxKeySpec, vars)

		for _, ev := range evs {

			val, err := d.i.e.fetch(ctx, ev.When, d.current)
			if err != nil {
				return err
			}

			switch v := val.(type) {
			case bool:
				switch v {
				case true:
					_, err = d.i.e.fetch(ctx, ev.Then, d.current)
					if err != nil {
						return err
					}
				}
			}

		}

	}

	return

}

func (d *document) yield(ctx context.Context, stm sql.Statement, output sql.Token) (interface{}, error) {

	switch stm := stm.(type) {

	case *sql.SelectStatement:

		var doc *data.Doc

		for _, v := range stm.Expr {
			if _, ok := v.Expr.(*sql.All); ok {
				doc = d.current
				break
			}
		}

		if doc == nil {
			doc = data.New()
		}

		for _, e := range stm.Expr {

			switch v := e.Expr.(type) {
			case *sql.All:
				break
			default:

				// If the query has a GROUP BY expression
				// then let's check if this is an aggregate
				// function, and if it is then pass the
				// first argument directly through.

				if len(stm.Group) > 0 {
					if f, ok := e.Expr.(*sql.FuncExpression); ok && f.Aggr {
						v, err := d.i.e.fetch(ctx, f.Args[0], d.current)
						if err != nil {
							return nil, err
						}
						doc.Set(v, f.String())
						continue
					}
				}

				// Otherwise treat the field normally, and
				// calculate the value to be inserted into
				// the final output document.

				v, err := d.i.e.fetch(ctx, v, d.current)
				if err != nil {
					return nil, err
				}

				switch v {
				case d.current:
					doc.Set(nil, e.Field)
				default:
					doc.Set(v, e.Field)
				}

			}

		}

		return doc.Data(), nil

	default:

		switch output {
		default:
			return nil, nil
		case sql.DIFF:
			return d.diff().Data(), nil
		case sql.AFTER:
			return d.current.Data(), nil
		case sql.BEFORE:
			return d.initial.Data(), nil
		case sql.BOTH:
			return map[string]interface{}{
				"after":  d.current.Data(),
				"before": d.initial.Data(),
			}, nil
		}

	}

}
