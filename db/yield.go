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
	"github.com/abcum/surreal/util/diff"
)

func (d *document) cold(ctx context.Context) (doc *data.Doc, err error) {

	// We need to copy the document so that
	// we can add and remove the fields which
	// are relevant to the particular query.

	doc = d.initial.Copy()

	// If we are not authenticated using DB,
	// NS, or KV level, then we need to check
	// document permissions for this query.

	if k, ok := ctx.Value(ctxKeyKind).(cnf.Kind); ok {
		if k == cnf.AuthSC {
			if err = d.perms(ctx, doc); err != nil {
				return nil, err
			}
		}
	}

	return

}

func (d *document) cnow(ctx context.Context) (doc *data.Doc, err error) {

	// We need to copy the document so that
	// we can add and remove the fields which
	// are relevant to the particular query.

	doc = d.current.Copy()

	// If we are not authenticated using DB,
	// NS, or KV level, then we need to check
	// document permissions for this query.

	if k, ok := ctx.Value(ctxKeyKind).(cnf.Kind); ok {
		if k == cnf.AuthSC {
			if err = d.perms(ctx, doc); err != nil {
				return nil, err
			}
		}
	}

	return

}

func (d *document) diffs(initial, current *data.Doc) *data.Doc {

	a, _ := initial.Data().(map[string]interface{})
	b, _ := current.Data().(map[string]interface{})

	if c := diff.Diff(a, b); len(c) > 0 {
		return data.Consume(c)
	}

	return data.Consume(nil)

}

func (d *document) yield(ctx context.Context, stm sql.Statement, output sql.Token) (interface{}, error) {

	var exps sql.Fields
	var grps sql.Groups
	var fchs sql.Fetchs

	switch stm := stm.(type) {
	case *sql.LiveStatement:
		exps = stm.Expr
		fchs = stm.Fetch
	case *sql.SelectStatement:
		exps = stm.Expr
		grps = stm.Group
		fchs = stm.Fetch
	}

	// If there are no field expressions
	// then this was not a LIVE or SELECT
	// query, and therefore the query will
	// have an output format specified.

	if len(exps) == 0 {

		switch output {
		default:
			return nil, nil
		case sql.DIFF:

			old, err := d.cold(ctx)
			if err != nil {
				return nil, err
			}

			now, err := d.cnow(ctx)
			if err != nil {
				return nil, err
			}

			return d.diffs(old, now).Data(), nil

		case sql.AFTER:

			doc, err := d.cnow(ctx)
			if err != nil {
				return nil, err
			}
			return doc.Data(), nil

		case sql.BEFORE:

			doc, err := d.cold(ctx)
			if err != nil {
				return nil, err
			}
			return doc.Data(), nil

		case sql.BOTH:

			old, err := d.cold(ctx)
			if err != nil {
				return nil, err
			}

			now, err := d.cnow(ctx)
			if err != nil {
				return nil, err
			}

			return map[string]interface{}{
				"after":  now.Data(),
				"before": old.Data(),
			}, nil

		}

	}

	// But if there are field expresions
	// then this query is a LIVE or SELECT
	// query, and we must output only the
	// desired fields in the output.

	var out = data.New()

	doc, err := d.cnow(ctx)
	if err != nil {
		return nil, err
	}

	// First of all, check to see if an ALL
	// expression has been specified, and if
	// it has then use the full document.

	for _, e := range exps {
		if _, ok := e.Expr.(*sql.All); ok {
			out = doc
			break
		}
	}

	// Next let's see the field expressions
	// which have been requested, and add
	// these to the output document.

	for _, e := range exps {

		switch v := e.Expr.(type) {
		case *sql.All:
			break
		default:

			// If the query has a GROUP BY expression
			// then let's check if this is an aggregate
			// function, and if it is then pass the
			// first argument directly through.

			if len(grps) > 0 {
				if f, ok := e.Expr.(*sql.FuncExpression); ok && f.Aggr {
					v, err := d.i.e.fetch(ctx, f.Args[0], doc)
					if err != nil {
						return nil, err
					}
					out.Set(v, f.String())
					continue
				}
			}

			// Otherwise treat the field normally, and
			// calculate the value to be inserted into
			// the final output document.

			o, err := d.i.e.fetch(ctx, v, doc)
			if err != nil {
				return nil, err
			}

			switch o {
			case doc:
				out.Set(nil, e.Field)
			default:
				out.Set(o, e.Field)
			}

		}

	}

	// Finally let's see if there are any
	// FETCH expressions, so that we can
	// follow links to other records.

	for _, e := range fchs {

		switch v := e.Expr.(type) {
		case *sql.All:
			break
		case *sql.Ident:

			out.Walk(func(key string, val interface{}, exi bool) error {

				switch res := val.(type) {
				case []interface{}:
					val, _ = d.i.e.fetchArray(ctx, res, doc)
					out.Set(val, key)
				case *sql.Thing:
					val, _ = d.i.e.fetchThing(ctx, res, doc)
					out.Set(val, key)
				}

				return nil

			}, v.ID)

		}

	}

	return out.Data(), nil

}
