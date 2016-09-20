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
	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/item"
	"github.com/abcum/surreal/util/keys"
	"github.com/abcum/surreal/util/pack"
)

func executeDefineTableStatement(txn kvs.TX, ast *sql.DefineTableStatement) (out []interface{}, err error) {

	var local bool

	if txn == nil {
		local = true
		txn, err = db.Txn(true)
		if err != nil {
			return
		}
		defer txn.Rollback()
	}

	for _, TB := range ast.What {

		// Set the database definition
		dkey := &keys.DB{KV: ast.KV, NS: ast.NS, DB: ast.DB}
		if err := txn.Put(dkey.Encode(), nil); err != nil {
			return nil, err
		}

		// Set the table definition
		tkey := &keys.TB{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB}
		if err := txn.Put(tkey.Encode(), nil); err != nil {
			return nil, err
		}

	}

	if local {
		txn.Commit()
	}

	return

}

func executeDefineRulesStatement(txn kvs.TX, ast *sql.DefineRulesStatement) (out []interface{}, err error) {

	var local bool

	if txn == nil {
		local = true
		txn, err = db.Txn(true)
		if err != nil {
			return
		}
		defer txn.Rollback()
	}

	for _, TB := range ast.What {

		for _, RU := range ast.When {

			// Set the database definition
			dkey := &keys.DB{KV: ast.KV, NS: ast.NS, DB: ast.DB}
			if err := txn.Put(dkey.Encode(), nil); err != nil {
				return nil, err
			}

			// Set the table definition
			tkey := &keys.TB{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB}
			if err := txn.Put(tkey.Encode(), nil); err != nil {
				return nil, err
			}

			// Set the field definition
			rkey := &keys.RU{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB, RU: RU}
			if err := txn.Put(rkey.Encode(), pack.Encode(ast)); err != nil {
				return nil, err
			}

		}

	}

	if local {
		txn.Commit()
	}

	return

}

func executeDefineFieldStatement(txn kvs.TX, ast *sql.DefineFieldStatement) (out []interface{}, err error) {

	var local bool

	if txn == nil {
		local = true
		txn, err = db.Txn(true)
		if err != nil {
			return
		}
		defer txn.Rollback()
	}

	for _, TB := range ast.What {

		// Set the database definition
		dkey := &keys.DB{KV: ast.KV, NS: ast.NS, DB: ast.DB}
		if err := txn.Put(dkey.Encode(), nil); err != nil {
			return nil, err
		}

		// Set the table definition
		tkey := &keys.TB{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB}
		if err := txn.Put(tkey.Encode(), nil); err != nil {
			return nil, err
		}

		// Set the field definition
		fkey := &keys.FD{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB, FD: ast.Name}
		if err := txn.Put(fkey.Encode(), pack.Encode(ast)); err != nil {
			return nil, err
		}

	}

	if local {
		txn.Commit()
	}

	return

}

func executeDefineIndexStatement(txn kvs.TX, ast *sql.DefineIndexStatement) (out []interface{}, err error) {

	var local bool

	if txn == nil {
		local = true
		txn, err = db.Txn(true)
		if err != nil {
			return
		}
		defer txn.Rollback()
	}

	for _, TB := range ast.What {

		// Set the database definition
		dkey := &keys.DB{KV: ast.KV, NS: ast.NS, DB: ast.DB}
		if err := txn.Put(dkey.Encode(), nil); err != nil {
			return nil, err
		}

		// Set the table definition
		tkey := &keys.TB{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB}
		if err := txn.Put(tkey.Encode(), nil); err != nil {
			return nil, err
		}

		// Set the index definition
		ikey := &keys.IX{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB, IX: ast.Name}
		if err := txn.Put(ikey.Encode(), pack.Encode(ast)); err != nil {
			return nil, err
		}

		// Remove all index data
		dbeg := &keys.Index{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB, IX: keys.Prefix, FD: keys.Ignore}
		dend := &keys.Index{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB, IX: keys.Suffix, FD: keys.Ignore}
		if err := txn.RDel(dbeg.Encode(), dend.Encode(), 0); err != nil {
			return nil, err
		}

		// Fetch the items
		ibeg := &keys.Thing{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB, ID: keys.Prefix}
		iend := &keys.Thing{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB, ID: keys.Suffix}
		kvs, _ := txn.RGet(ibeg.Encode(), iend.Encode(), 0)
		for _, kv := range kvs {
			doc := item.New(kv, txn, nil)
			if err := doc.StoreIndex(); err != nil {
				return nil, err
			}
		}

	}

	if local {
		txn.Commit()
	}

	return

}
