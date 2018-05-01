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
)

func (d *document) check(ctx context.Context, cond sql.Expr) (ok bool, err error) {

	// If no condition expression has been
	// defined then we can ignore this, and
	// process the current document.

	if cond == nil {
		return true, nil
	}

	// If a condition expression has been
	// defined then let's process it to see
	// what value it returns or error.

	val, err := d.i.e.fetch(ctx, cond, d.current)

	// If the condition expression result is
	// not a boolean value, then let's see
	// if the value can be equated to a bool.

	return calcAsBool(val), err

}

// Grant checks to see if the table permissions allow
// this record to be accessed for live queries, and
// if not then it errors accordingly.
func (d *document) grant(ctx context.Context, met method) (ok bool, err error) {

	var val interface{}

	// If this is a document loaded from
	// a subquery or data param, and not
	// from the KV store, then there is
	// no need to check permissions.

	if d.key == nil {
		return false, nil
	}

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

	if p, ok := tb.Perms.(*sql.PermExpression); ok {
		switch met {
		case _SELECT:
			val, err = d.i.e.fetch(ctx, p.Select, d.current)
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

	if v, ok := val.(bool); ok {
		return v, err
	}

	// Otherwise as this request is scoped,
	// return an error, so that the
	// document is unable to be viewed.

	return false, err

}

// Query checks to see if the table permissions allow
// this record to be accessed for normal queries, and
// if not then it errors accordingly.
func (d *document) allow(ctx context.Context, met method) (ok bool, err error) {

	var val interface{}

	// If this is a document loaded from
	// a subquery or data param, and not
	// from the KV store, then there is
	// no need to check permissions.

	if d.key == nil {
		return true, nil
	}

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

	if p, ok := tb.Perms.(*sql.PermExpression); ok {
		switch met {
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

	if v, ok := val.(bool); ok {
		return v, err
	}

	// Otherwise as this request is scoped,
	// return an error, so that the
	// document is unable to be viewed.

	return false, err

}
