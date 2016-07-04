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
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
	"github.com/abcum/surreal/util/keys"
)

func executeDefineTableStatement(ast *sql.DefineTableStatement) (out []interface{}, err error) {

	txn, err := db.Txn(true)
	if err != nil {
		return
	}

	defer txn.Rollback()

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

	txn.Commit()

	return

}

func executeDefineFieldStatement(ast *sql.DefineFieldStatement) (out []interface{}, err error) {

	txn, err := db.Txn(true)
	if err != nil {
		return
	}

	defer txn.Rollback()

	doc := data.New()
	doc.Set(ast.Name, "name")
	doc.Set(ast.Type, "type")
	doc.Set(ast.Code, "code")
	doc.Set(ast.Min, "min")
	doc.Set(ast.Max, "max")
	doc.Set(ast.Default, "default")
	doc.Set(ast.Notnull, "notnull")
	doc.Set(ast.Readonly, "readonly")
	doc.Set(ast.Mandatory, "mandatory")

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
		if err := txn.Put(fkey.Encode(), doc.ToPACK()); err != nil {
			return nil, err
		}

	}

	txn.Commit()

	return

}

func executeDefineIndexStatement(ast *sql.DefineIndexStatement) (out []interface{}, err error) {

	txn, err := db.Txn(true)
	if err != nil {
		return
	}

	defer txn.Rollback()

	doc := data.New()
	// doc.Set(ast.Name, "name")
	// doc.Set(ast.Type, "type")
	// doc.Set(ast.Code, "code")
	// doc.Set(ast.Min, "min")
	// doc.Set(ast.Max, "max")
	// doc.Set(ast.Default, "default")
	// doc.Set(ast.Notnull, "notnull")
	// doc.Set(ast.Readonly, "readonly")
	// doc.Set(ast.Mandatory, "mandatory")

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
		if err := txn.Put(ikey.Encode(), doc.ToPACK()); err != nil {
			return nil, err
		}

	}

	txn.Commit()

	return

}
