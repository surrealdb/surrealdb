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

	"golang.org/x/crypto/bcrypt"

	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/item"
	"github.com/abcum/surreal/util/keys"
	"github.com/abcum/surreal/util/pack"
	"github.com/abcum/surreal/util/rand"
)

func (e *executor) executeDefineNamespaceStatement(ctx context.Context, ast *sql.DefineNamespaceStatement) (out []interface{}, err error) {

	// Set the namespace
	nkey := &keys.NS{KV: ast.KV, NS: ast.Name.ID}
	_, err = e.txn.Put(0, nkey.Encode(), ast.Encode())

	return

}

func (e *executor) executeDefineDatabaseStatement(ctx context.Context, ast *sql.DefineDatabaseStatement) (out []interface{}, err error) {

	// Set the namespace
	nkey := &keys.NS{KV: ast.KV, NS: ast.NS}
	nval := &sql.DefineNamespaceStatement{Name: sql.NewIdent(ast.NS)}
	e.txn.PutC(0, nkey.Encode(), nval.Encode(), nil)

	// Set the database
	dkey := &keys.DB{KV: ast.KV, NS: ast.NS, DB: ast.Name.ID}
	_, err = e.txn.Put(0, dkey.Encode(), ast.Encode())

	return

}

func (e *executor) executeDefineLoginStatement(ctx context.Context, ast *sql.DefineLoginStatement) (out []interface{}, err error) {

	ast.Code = rand.New(128)

	ast.Pass, _ = bcrypt.GenerateFromPassword(ast.Pass, bcrypt.DefaultCost)

	if ast.Kind == sql.NAMESPACE {

		// Set the namespace
		nkey := &keys.NS{KV: ast.KV, NS: ast.NS}
		nval := &sql.DefineNamespaceStatement{Name: sql.NewIdent(ast.NS)}
		e.txn.PutC(0, nkey.Encode(), nval.Encode(), nil)

		// Set the login
		ukey := &keys.NU{KV: ast.KV, NS: ast.NS, US: ast.User.ID}
		_, err = e.txn.Put(0, ukey.Encode(), ast.Encode())

	}

	if ast.Kind == sql.DATABASE {

		// Set the namespace
		nkey := &keys.NS{KV: ast.KV, NS: ast.NS}
		nval := &sql.DefineNamespaceStatement{Name: sql.NewIdent(ast.NS)}
		e.txn.PutC(0, nkey.Encode(), nval.Encode(), nil)

		// Set the database
		dkey := &keys.DB{KV: ast.KV, NS: ast.NS, DB: ast.DB}
		dval := &sql.DefineDatabaseStatement{Name: sql.NewIdent(ast.DB)}
		e.txn.PutC(0, dkey.Encode(), dval.Encode(), nil)

		// Set the login
		ukey := &keys.DU{KV: ast.KV, NS: ast.NS, DB: ast.DB, US: ast.User.ID}
		_, err = e.txn.Put(0, ukey.Encode(), ast.Encode())

	}

	return

}

func (e *executor) executeDefineTokenStatement(ctx context.Context, ast *sql.DefineTokenStatement) (out []interface{}, err error) {

	if ast.Kind == sql.NAMESPACE {

		// Set the namespace
		nkey := &keys.NS{KV: ast.KV, NS: ast.NS}
		nval := &sql.DefineNamespaceStatement{Name: sql.NewIdent(ast.NS)}
		e.txn.PutC(0, nkey.Encode(), nval.Encode(), nil)

		// Set the token
		tkey := &keys.NT{KV: ast.KV, NS: ast.NS, TK: ast.Name.ID}
		_, err = e.txn.Put(0, tkey.Encode(), ast.Encode())

	}

	if ast.Kind == sql.DATABASE {

		// Set the namespace
		nkey := &keys.NS{KV: ast.KV, NS: ast.NS}
		nval := &sql.DefineNamespaceStatement{Name: sql.NewIdent(ast.NS)}
		e.txn.PutC(0, nkey.Encode(), nval.Encode(), nil)

		// Set the database
		dkey := &keys.DB{KV: ast.KV, NS: ast.NS, DB: ast.DB}
		dval := &sql.DefineDatabaseStatement{Name: sql.NewIdent(ast.DB)}
		e.txn.PutC(0, dkey.Encode(), dval.Encode(), nil)

		// Set the token
		tkey := &keys.DT{KV: ast.KV, NS: ast.NS, DB: ast.DB, TK: ast.Name.ID}
		_, err = e.txn.Put(0, tkey.Encode(), ast.Encode())

	}

	return

}

func (e *executor) executeDefineScopeStatement(ctx context.Context, ast *sql.DefineScopeStatement) (out []interface{}, err error) {

	ast.Code = rand.New(128)

	// Set the namespace
	nkey := &keys.NS{KV: ast.KV, NS: ast.NS}
	nval := &sql.DefineNamespaceStatement{Name: sql.NewIdent(ast.NS)}
	e.txn.PutC(0, nkey.Encode(), nval.Encode(), nil)

	// Set the database
	dkey := &keys.DB{KV: ast.KV, NS: ast.NS, DB: ast.DB}
	dval := &sql.DefineDatabaseStatement{Name: sql.NewIdent(ast.DB)}
	e.txn.PutC(0, dkey.Encode(), dval.Encode(), nil)

	// Set the scope
	skey := &keys.SC{KV: ast.KV, NS: ast.NS, DB: ast.DB, SC: ast.Name.ID}
	_, err = e.txn.Put(0, skey.Encode(), ast.Encode())

	return

}

func (e *executor) executeDefineTableStatement(ctx context.Context, ast *sql.DefineTableStatement) (out []interface{}, err error) {

	// Set the namespace
	nkey := &keys.NS{KV: ast.KV, NS: ast.NS}
	nval := &sql.DefineNamespaceStatement{Name: sql.NewIdent(ast.NS)}
	e.txn.PutC(0, nkey.Encode(), nval.Encode(), nil)

	// Set the database
	dkey := &keys.DB{KV: ast.KV, NS: ast.NS, DB: ast.DB}
	dval := &sql.DefineDatabaseStatement{Name: sql.NewIdent(ast.DB)}
	e.txn.PutC(0, dkey.Encode(), dval.Encode(), nil)

	for _, TB := range ast.What {

		// Set the table
		tkey := &keys.TB{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB.TB}
		if _, err = e.txn.Put(0, tkey.Encode(), ast.Encode()); err != nil {
			return nil, err
		}

	}

	return

}

func (e *executor) executeDefineFieldStatement(ctx context.Context, ast *sql.DefineFieldStatement) (out []interface{}, err error) {

	// Set the namespace
	nkey := &keys.NS{KV: ast.KV, NS: ast.NS}
	nval := &sql.DefineNamespaceStatement{Name: sql.NewIdent(ast.NS)}
	e.txn.PutC(0, nkey.Encode(), nval.Encode(), nil)

	// Set the database
	dkey := &keys.DB{KV: ast.KV, NS: ast.NS, DB: ast.DB}
	dval := &sql.DefineDatabaseStatement{Name: sql.NewIdent(ast.DB)}
	e.txn.PutC(0, dkey.Encode(), dval.Encode(), nil)

	for _, TB := range ast.What {

		// Set the table
		tkey := &keys.TB{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB.TB}
		tval := &sql.DefineTableStatement{What: ast.What}
		e.txn.PutC(0, tkey.Encode(), tval.Encode(), nil)

		// Set the field
		fkey := &keys.FD{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB.TB, FD: ast.Name.ID}
		if _, err = e.txn.Put(0, fkey.Encode(), pack.Encode(ast)); err != nil {
			return nil, err
		}

	}

	return

}

func (e *executor) executeDefineIndexStatement(ctx context.Context, ast *sql.DefineIndexStatement) (out []interface{}, err error) {

	// Set the namespace
	nkey := &keys.NS{KV: ast.KV, NS: ast.NS}
	nval := &sql.DefineNamespaceStatement{Name: sql.NewIdent(ast.NS)}
	e.txn.PutC(0, nkey.Encode(), nval.Encode(), nil)

	// Set the database
	dkey := &keys.DB{KV: ast.KV, NS: ast.NS, DB: ast.DB}
	dval := &sql.DefineDatabaseStatement{Name: sql.NewIdent(ast.DB)}
	e.txn.PutC(0, dkey.Encode(), dval.Encode(), nil)

	for _, TB := range ast.What {

		// Set the table
		tkey := &keys.TB{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB.TB}
		tval := &sql.DefineTableStatement{What: ast.What}
		e.txn.PutC(0, tkey.Encode(), tval.Encode(), nil)

		// Set the index
		ikey := &keys.IX{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB.TB, IX: ast.Name.ID}
		if _, err = e.txn.Put(0, ikey.Encode(), ast.Encode()); err != nil {
			return nil, err
		}

		// Remove all index data
		dbeg := &keys.Index{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB.TB, IX: keys.Prefix, FD: keys.Ignore}
		dend := &keys.Index{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB.TB, IX: keys.Suffix, FD: keys.Ignore}
		if _, err = e.txn.DelR(0, dbeg.Encode(), dend.Encode(), 0); err != nil {
			return nil, err
		}

		// Fetch the items
		ibeg := &keys.Thing{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB.TB, ID: keys.Prefix}
		iend := &keys.Thing{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB.TB, ID: keys.Suffix}
		kvs, _ := e.txn.GetR(0, ibeg.Encode(), iend.Encode(), 0)
		for _, kv := range kvs {
			doc := item.New(kv, e.txn, nil, e.ctx)
			if err := doc.StoreIndex(); err != nil {
				return nil, err
			}
		}

	}

	return

}
