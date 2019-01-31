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
	"github.com/abcum/surreal/util/keys"
)

func (e *executor) executeRemoveNamespace(ctx context.Context, ast *sql.RemoveNamespaceStatement) (out []interface{}, err error) {

	if err := e.access(ctx, cnf.AuthKV); err != nil {
		return nil, err
	}

	e.dbo.DelNS(ast.Name.VA)

	// Remove the namespace definition
	nkey := &keys.NS{KV: KV, NS: ast.Name.VA}
	_, err = e.dbo.Clr(ctx, nkey.Encode())

	// Remove the namespace resource data
	akey := &keys.Namespace{KV: KV, NS: ast.Name.VA}
	_, err = e.dbo.ClrP(ctx, akey.Encode(), 0)

	return

}

func (e *executor) executeRemoveDatabase(ctx context.Context, ast *sql.RemoveDatabaseStatement) (out []interface{}, err error) {

	if err := e.access(ctx, cnf.AuthNS); err != nil {
		return nil, err
	}

	e.dbo.DelDB(e.ns, ast.Name.VA)

	// Remove the database definition
	dkey := &keys.DB{KV: KV, NS: e.ns, DB: ast.Name.VA}
	_, err = e.dbo.Clr(ctx, dkey.Encode())

	// Remove the database resource data
	akey := &keys.Database{KV: KV, NS: e.ns, DB: ast.Name.VA}
	_, err = e.dbo.ClrP(ctx, akey.Encode(), 0)

	return

}

func (e *executor) executeRemoveLogin(ctx context.Context, ast *sql.RemoveLoginStatement) (out []interface{}, err error) {

	switch ast.Kind {
	case sql.NAMESPACE:

		if err := e.access(ctx, cnf.AuthNS); err != nil {
			return nil, err
		}

		// Remove the login definition
		ukey := &keys.NU{KV: KV, NS: e.ns, US: ast.User.VA}
		_, err = e.dbo.ClrP(ctx, ukey.Encode(), 0)

	case sql.DATABASE:

		if err := e.access(ctx, cnf.AuthDB); err != nil {
			return nil, err
		}

		// Remove the login definition
		ukey := &keys.DU{KV: KV, NS: e.ns, DB: e.db, US: ast.User.VA}
		_, err = e.dbo.ClrP(ctx, ukey.Encode(), 0)

	}

	return

}

func (e *executor) executeRemoveToken(ctx context.Context, ast *sql.RemoveTokenStatement) (out []interface{}, err error) {

	if err := e.access(ctx, cnf.AuthNO); err != nil {
		return nil, err
	}

	switch ast.Kind {
	case sql.NAMESPACE:

		if err := e.access(ctx, cnf.AuthNS); err != nil {
			return nil, err
		}

		// Remove the token definition
		tkey := &keys.NT{KV: KV, NS: e.ns, TK: ast.Name.VA}
		_, err = e.dbo.ClrP(ctx, tkey.Encode(), 0)

	case sql.DATABASE:

		if err := e.access(ctx, cnf.AuthDB); err != nil {
			return nil, err
		}

		// Remove the token definition
		tkey := &keys.DT{KV: KV, NS: e.ns, DB: e.db, TK: ast.Name.VA}
		_, err = e.dbo.ClrP(ctx, tkey.Encode(), 0)

	case sql.SCOPE:

		if err := e.access(ctx, cnf.AuthDB); err != nil {
			return nil, err
		}

		// Remove the token definition
		tkey := &keys.ST{KV: KV, NS: e.ns, DB: e.db, SC: ast.What.VA, TK: ast.Name.VA}
		_, err = e.dbo.ClrP(ctx, tkey.Encode(), 0)

	}

	return

}

func (e *executor) executeRemoveScope(ctx context.Context, ast *sql.RemoveScopeStatement) (out []interface{}, err error) {

	if err := e.access(ctx, cnf.AuthDB); err != nil {
		return nil, err
	}

	// Remove the scope definition
	skey := &keys.SC{KV: KV, NS: e.ns, DB: e.db, SC: ast.Name.VA}
	_, err = e.dbo.ClrP(ctx, skey.Encode(), 0)

	return

}

func (e *executor) executeRemoveEvent(ctx context.Context, ast *sql.RemoveEventStatement) (out []interface{}, err error) {

	if err := e.access(ctx, cnf.AuthDB); err != nil {
		return nil, err
	}

	for _, TB := range ast.What {

		e.dbo.DelEV(e.ns, e.db, TB.TB, ast.Name.VA)

		// Remove the event definition
		ekey := &keys.EV{KV: KV, NS: e.ns, DB: e.db, TB: TB.TB, EV: ast.Name.VA}
		if _, err = e.dbo.ClrP(ctx, ekey.Encode(), 0); err != nil {
			return nil, err
		}

	}

	return

}

func (e *executor) executeRemoveField(ctx context.Context, ast *sql.RemoveFieldStatement) (out []interface{}, err error) {

	if err := e.access(ctx, cnf.AuthDB); err != nil {
		return nil, err
	}

	for _, TB := range ast.What {

		e.dbo.DelFD(e.ns, e.db, TB.TB, ast.Name.VA)

		// Remove the field definition
		fkey := &keys.FD{KV: KV, NS: e.ns, DB: e.db, TB: TB.TB, FD: ast.Name.VA}
		if _, err = e.dbo.ClrP(ctx, fkey.Encode(), 0); err != nil {
			return nil, err
		}

	}

	return

}

func (e *executor) executeRemoveIndex(ctx context.Context, ast *sql.RemoveIndexStatement) (out []interface{}, err error) {

	if err := e.access(ctx, cnf.AuthDB); err != nil {
		return nil, err
	}

	for _, TB := range ast.What {

		e.dbo.DelIX(e.ns, e.db, TB.TB, ast.Name.VA)

		// Remove the index definition
		ikey := &keys.IX{KV: KV, NS: e.ns, DB: e.db, TB: TB.TB, IX: ast.Name.VA}
		if _, err = e.dbo.ClrP(ctx, ikey.Encode(), 0); err != nil {
			return nil, err
		}

		// Remove the index resource data
		dkey := &keys.Index{KV: KV, NS: e.ns, DB: e.db, TB: TB.TB, IX: ast.Name.VA, FD: keys.Ignore}
		if _, err = e.dbo.ClrP(ctx, dkey.Encode(), 0); err != nil {
			return nil, err
		}

	}

	return

}

func (e *executor) executeRemoveTable(ctx context.Context, ast *sql.RemoveTableStatement) (out []interface{}, err error) {

	if err := e.access(ctx, cnf.AuthDB); err != nil {
		return nil, err
	}

	for _, TB := range ast.What {

		e.dbo.DelTB(e.ns, e.db, TB.TB)

		tb, err := e.dbo.GetTB(ctx, e.ns, e.db, TB.TB)
		if err != nil {
			return nil, err
		}

		// Remove the table definition
		tkey := &keys.TB{KV: KV, NS: e.ns, DB: e.db, TB: TB.TB}
		_, err = e.dbo.Clr(ctx, tkey.Encode())
		if err != nil {
			return nil, err
		}

		// Remove the table resource data
		dkey := &keys.Table{KV: KV, NS: e.ns, DB: e.db, TB: TB.TB}
		_, err = e.dbo.ClrP(ctx, dkey.Encode(), 0)
		if err != nil {
			return nil, err
		}

		if tb.Lock {

			for _, FT := range tb.From {

				// Remove the foreign table definition
				tkey := &keys.FT{KV: KV, NS: e.ns, DB: e.db, TB: FT.TB, FT: TB.TB}
				if _, err = e.dbo.ClrP(ctx, tkey.Encode(), 0); err != nil {
					return nil, err
				}

			}

		}

	}

	return

}
