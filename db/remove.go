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

	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/keys"
)

func (e *executor) executeRemoveNamespace(ctx context.Context, ast *sql.RemoveNamespaceStatement) (out []interface{}, err error) {

	e.dbo.DelNS(ast.Name.ID)

	nkey := &keys.NS{KV: ast.KV, NS: ast.Name.ID}
	_, err = e.dbo.ClrP(nkey.Encode(), 0)

	return

}

func (e *executor) executeRemoveDatabase(ctx context.Context, ast *sql.RemoveDatabaseStatement) (out []interface{}, err error) {

	e.dbo.DelDB(ast.NS, ast.Name.ID)

	dkey := &keys.DB{KV: ast.KV, NS: ast.NS, DB: ast.Name.ID}
	_, err = e.dbo.ClrP(dkey.Encode(), 0)

	return

}

func (e *executor) executeRemoveLogin(ctx context.Context, ast *sql.RemoveLoginStatement) (out []interface{}, err error) {

	switch ast.Kind {
	case sql.NAMESPACE:
		ukey := &keys.NU{KV: ast.KV, NS: ast.NS, US: ast.User.ID}
		_, err = e.dbo.ClrP(ukey.Encode(), 0)
	case sql.DATABASE:
		ukey := &keys.DU{KV: ast.KV, NS: ast.NS, DB: ast.DB, US: ast.User.ID}
		_, err = e.dbo.ClrP(ukey.Encode(), 0)
	}

	return

}

func (e *executor) executeRemoveToken(ctx context.Context, ast *sql.RemoveTokenStatement) (out []interface{}, err error) {

	switch ast.Kind {
	case sql.NAMESPACE:
		tkey := &keys.NT{KV: ast.KV, NS: ast.NS, TK: ast.Name.ID}
		_, err = e.dbo.ClrP(tkey.Encode(), 0)
	case sql.DATABASE:
		tkey := &keys.DT{KV: ast.KV, NS: ast.NS, DB: ast.DB, TK: ast.Name.ID}
		_, err = e.dbo.ClrP(tkey.Encode(), 0)
	}

	return

}

func (e *executor) executeRemoveScope(ctx context.Context, ast *sql.RemoveScopeStatement) (out []interface{}, err error) {

	skey := &keys.SC{KV: ast.KV, NS: ast.NS, DB: ast.DB, SC: ast.Name.ID}
	_, err = e.dbo.ClrP(skey.Encode(), 0)

	return

}

func (e *executor) executeRemoveTable(ctx context.Context, ast *sql.RemoveTableStatement) (out []interface{}, err error) {

	for _, TB := range ast.What {

		e.dbo.DelTB(ast.NS, ast.DB, TB.TB)

		tb, err := e.dbo.GetTB(ast.NS, ast.DB, TB.TB)
		if err != nil {
			return nil, err
		}

		tkey := &keys.TB{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB.TB}
		_, err = e.dbo.ClrP(tkey.Encode(), 0)
		if err != nil {
			return nil, err
		}

		if tb.Lock {
			for _, FT := range tb.From {
				tkey := &keys.FT{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: FT.TB, FT: TB.TB}
				if _, err = e.dbo.ClrP(tkey.Encode(), 0); err != nil {
					return nil, err
				}
			}
		}

	}

	return

}

func (e *executor) executeRemoveEvent(ctx context.Context, ast *sql.RemoveEventStatement) (out []interface{}, err error) {

	for _, TB := range ast.What {

		e.dbo.DelEV(ast.NS, ast.DB, TB.TB, ast.Name.ID)

		ekey := &keys.EV{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB.TB, EV: ast.Name.ID}
		if _, err = e.dbo.ClrP(ekey.Encode(), 0); err != nil {
			return nil, err
		}

	}

	return

}

func (e *executor) executeRemoveField(ctx context.Context, ast *sql.RemoveFieldStatement) (out []interface{}, err error) {

	for _, TB := range ast.What {

		e.dbo.DelFD(ast.NS, ast.DB, TB.TB, ast.Name.ID)

		fkey := &keys.FD{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB.TB, FD: ast.Name.ID}
		if _, err = e.dbo.ClrP(fkey.Encode(), 0); err != nil {
			return nil, err
		}

	}

	return

}

func (e *executor) executeRemoveIndex(ctx context.Context, ast *sql.RemoveIndexStatement) (out []interface{}, err error) {

	for _, TB := range ast.What {

		e.dbo.DelIX(ast.NS, ast.DB, TB.TB, ast.Name.ID)

		ikey := &keys.IX{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB.TB, IX: ast.Name.ID}
		if _, err = e.dbo.ClrP(ikey.Encode(), 0); err != nil {
			return nil, err
		}

		dkey := &keys.Index{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB.TB, IX: ast.Name.ID, FD: keys.Ignore}
		if _, err = e.dbo.ClrP(dkey.Encode(), 0); err != nil {
			return nil, err
		}

	}

	return

}
