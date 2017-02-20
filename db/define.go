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
	"github.com/abcum/surreal/log"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/item"
	"github.com/abcum/surreal/util/keys"
	"github.com/abcum/surreal/util/pack"
)

func (e *executor) executeDefineNamespaceStatement(ast *sql.DefineNamespaceStatement) (out []interface{}, err error) {

	log.WithPrefix("sql").WithFields(map[string]interface{}{
		"ns": ast.NS,
		"db": ast.DB,
	}).Debugln(ast)

	// Set the namespace
	nkey := &keys.NS{KV: ast.KV, NS: ast.Name}
	_, err = e.txn.Put(0, nkey.Encode(), ast.Encode())

	return

}

func (e *executor) executeDefineDatabaseStatement(ast *sql.DefineDatabaseStatement) (out []interface{}, err error) {

	log.WithPrefix("sql").WithFields(map[string]interface{}{
		"ns": ast.NS,
		"db": ast.DB,
	}).Debugln(ast)

	// Set the namespace
	nkey := &keys.NS{KV: ast.KV, NS: ast.NS}
	nval := &sql.DefineNamespaceStatement{Name: ast.NS}
	e.txn.PutC(0, nkey.Encode(), nval.Encode(), nil)

	// Set the database
	dkey := &keys.DB{KV: ast.KV, NS: ast.NS, DB: ast.Name}
	_, err = e.txn.Put(0, dkey.Encode(), ast.Encode())

	return

}

func (e *executor) executeDefineLoginStatement(ast *sql.DefineLoginStatement) (out []interface{}, err error) {

	log.WithPrefix("sql").WithFields(map[string]interface{}{
		"ns": ast.NS,
		"db": ast.DB,
	}).Debugln(ast)

	if ast.Kind == sql.NAMESPACE {

		// Set the namespace
		nkey := &keys.NS{KV: ast.KV, NS: ast.NS}
		nval := &sql.DefineNamespaceStatement{Name: ast.NS}
		e.txn.PutC(0, nkey.Encode(), nval.Encode(), nil)

		// Set the login
		ukey := &keys.NU{KV: ast.KV, NS: ast.NS, US: ast.User}
		_, err = e.txn.Put(0, ukey.Encode(), ast.Encode())

	}

	if ast.Kind == sql.DATABASE {

		// Set the namespace
		nkey := &keys.NS{KV: ast.KV, NS: ast.NS}
		nval := &sql.DefineNamespaceStatement{Name: ast.NS}
		e.txn.PutC(0, nkey.Encode(), nval.Encode(), nil)

		// Set the database
		dkey := &keys.DB{KV: ast.KV, NS: ast.NS, DB: ast.DB}
		dval := &sql.DefineDatabaseStatement{Name: ast.DB}
		e.txn.PutC(0, dkey.Encode(), dval.Encode(), nil)

		// Set the login
		ukey := &keys.DU{KV: ast.KV, NS: ast.NS, DB: ast.DB, US: ast.User}
		_, err = e.txn.Put(0, ukey.Encode(), ast.Encode())

	}

	return

}

func (e *executor) executeDefineTokenStatement(ast *sql.DefineTokenStatement) (out []interface{}, err error) {

	log.WithPrefix("sql").WithFields(map[string]interface{}{
		"ns": ast.NS,
		"db": ast.DB,
	}).Debugln(ast)

	if ast.Kind == sql.NAMESPACE {

		// Set the namespace
		nkey := &keys.NS{KV: ast.KV, NS: ast.NS}
		nval := &sql.DefineNamespaceStatement{Name: ast.NS}
		e.txn.PutC(0, nkey.Encode(), nval.Encode(), nil)

		// Set the token
		tkey := &keys.NT{KV: ast.KV, NS: ast.NS, TK: ast.Name}
		_, err = e.txn.Put(0, tkey.Encode(), ast.Encode())

	}

	if ast.Kind == sql.DATABASE {

		// Set the namespace
		nkey := &keys.NS{KV: ast.KV, NS: ast.NS}
		nval := &sql.DefineNamespaceStatement{Name: ast.NS}
		e.txn.PutC(0, nkey.Encode(), nval.Encode(), nil)

		// Set the database
		dkey := &keys.DB{KV: ast.KV, NS: ast.NS, DB: ast.DB}
		dval := &sql.DefineDatabaseStatement{Name: ast.DB}
		e.txn.PutC(0, dkey.Encode(), dval.Encode(), nil)

		// Set the token
		tkey := &keys.DT{KV: ast.KV, NS: ast.NS, DB: ast.DB, TK: ast.Name}
		_, err = e.txn.Put(0, tkey.Encode(), ast.Encode())

	}

	return

}

func (e *executor) executeDefineScopeStatement(ast *sql.DefineScopeStatement) (out []interface{}, err error) {

	log.WithPrefix("sql").WithFields(map[string]interface{}{
		"ns": ast.NS,
		"db": ast.DB,
	}).Debugln(ast)

	// Set the namespace
	nkey := &keys.NS{KV: ast.KV, NS: ast.NS}
	nval := &sql.DefineNamespaceStatement{Name: ast.NS}
	e.txn.PutC(0, nkey.Encode(), nval.Encode(), nil)

	// Set the database
	dkey := &keys.DB{KV: ast.KV, NS: ast.NS, DB: ast.DB}
	dval := &sql.DefineDatabaseStatement{Name: ast.DB}
	e.txn.PutC(0, dkey.Encode(), dval.Encode(), nil)

	// Set the scope
	skey := &keys.SC{KV: ast.KV, NS: ast.NS, DB: ast.DB, SC: ast.Name}
	_, err = e.txn.Put(0, skey.Encode(), ast.Encode())

	return

}

func (e *executor) executeDefineTableStatement(ast *sql.DefineTableStatement) (out []interface{}, err error) {

	log.WithPrefix("sql").WithFields(map[string]interface{}{
		"ns": ast.NS,
		"db": ast.DB,
	}).Debugln(ast)

	// Set the namespace
	nkey := &keys.NS{KV: ast.KV, NS: ast.NS}
	nval := &sql.DefineNamespaceStatement{Name: ast.NS}
	e.txn.PutC(0, nkey.Encode(), nval.Encode(), nil)

	// Set the database
	dkey := &keys.DB{KV: ast.KV, NS: ast.NS, DB: ast.DB}
	dval := &sql.DefineDatabaseStatement{Name: ast.DB}
	e.txn.PutC(0, dkey.Encode(), dval.Encode(), nil)

	for _, TB := range ast.What {

		// Set the table
		tkey := &keys.TB{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB}
		if _, err = e.txn.Put(0, tkey.Encode(), ast.Encode()); err != nil {
			return nil, err
		}

	}

	return

}

func (e *executor) executeDefineFieldStatement(ast *sql.DefineFieldStatement) (out []interface{}, err error) {

	log.WithPrefix("sql").WithFields(map[string]interface{}{
		"ns": ast.NS,
		"db": ast.DB,
	}).Debugln(ast)

	// Set the namespace
	nkey := &keys.NS{KV: ast.KV, NS: ast.NS}
	nval := &sql.DefineNamespaceStatement{Name: ast.NS}
	e.txn.PutC(0, nkey.Encode(), nval.Encode(), nil)

	// Set the database
	dkey := &keys.DB{KV: ast.KV, NS: ast.NS, DB: ast.DB}
	dval := &sql.DefineDatabaseStatement{Name: ast.DB}
	e.txn.PutC(0, dkey.Encode(), dval.Encode(), nil)

	for _, TB := range ast.What {

		// Set the table
		tkey := &keys.TB{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB}
		tval := &sql.DefineTableStatement{What: ast.What}
		e.txn.PutC(0, tkey.Encode(), tval.Encode(), nil)

		// Set the field
		fkey := &keys.FD{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB, FD: ast.Name}
		if _, err = e.txn.Put(0, fkey.Encode(), pack.Encode(ast)); err != nil {
			return nil, err
		}

	}

	return

}

func (e *executor) executeDefineIndexStatement(ast *sql.DefineIndexStatement) (out []interface{}, err error) {

	log.WithPrefix("sql").WithFields(map[string]interface{}{
		"ns": ast.NS,
		"db": ast.DB,
	}).Debugln(ast)

	// Set the namespace
	nkey := &keys.NS{KV: ast.KV, NS: ast.NS}
	nval := &sql.DefineNamespaceStatement{Name: ast.NS}
	e.txn.PutC(0, nkey.Encode(), nval.Encode(), nil)

	// Set the database
	dkey := &keys.DB{KV: ast.KV, NS: ast.NS, DB: ast.DB}
	dval := &sql.DefineDatabaseStatement{Name: ast.DB}
	e.txn.PutC(0, dkey.Encode(), dval.Encode(), nil)

	for _, TB := range ast.What {

		// Set the table
		tkey := &keys.TB{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB}
		tval := &sql.DefineTableStatement{What: ast.What}
		e.txn.PutC(0, tkey.Encode(), tval.Encode(), nil)

		// Set the index
		ikey := &keys.IX{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB, IX: ast.Name}
		if _, err = e.txn.Put(0, ikey.Encode(), ast.Encode()); err != nil {
			return nil, err
		}

		// Remove all index data
		dbeg := &keys.Index{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB, IX: keys.Prefix, FD: keys.Ignore}
		dend := &keys.Index{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB, IX: keys.Suffix, FD: keys.Ignore}
		if _, err = e.txn.DelR(0, dbeg.Encode(), dend.Encode(), 0); err != nil {
			return nil, err
		}

		// Fetch the items
		ibeg := &keys.Thing{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB, ID: keys.Prefix}
		iend := &keys.Thing{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB, ID: keys.Suffix}
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
