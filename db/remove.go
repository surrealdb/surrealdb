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
	"github.com/abcum/surreal/util/keys"
)

func executeRemoveTableStatement(txn kvs.TX, ast *sql.RemoveTableStatement) (out []interface{}, err error) {

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

		// Remove the table config
		tkey := &keys.TB{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB}
		if err := txn.Del(tkey.Encode()); err != nil {
			return nil, err
		}

		// Remove the rules config
		rkey := &keys.RU{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB, RU: keys.Ignore}
		if err := txn.PDel(rkey.Encode()); err != nil {
			return nil, err
		}

		// Remove the field config
		fkey := &keys.FD{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB, FD: keys.Ignore}
		if err := txn.PDel(fkey.Encode()); err != nil {
			return nil, err
		}

		// Remove the index config
		ikey := &keys.IX{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB, IX: keys.Ignore}
		if err := txn.PDel(ikey.Encode()); err != nil {
			return nil, err
		}

		// Remove all table data
		dkey := &keys.Table{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB}
		if err := txn.PDel(dkey.Encode()); err != nil {
			return nil, err
		}

	}

	if local {
		txn.Commit()
	}

	return

}

func executeRemoveRulesStatement(txn kvs.TX, ast *sql.RemoveRulesStatement) (out []interface{}, err error) {

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

			// Remove the rules config
			ckey := &keys.RU{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB, RU: RU}
			if err := txn.Del(ckey.Encode()); err != nil {
				return nil, err
			}

		}

	}

	if local {
		txn.Commit()
	}

	return

}

func executeRemoveFieldStatement(txn kvs.TX, ast *sql.RemoveFieldStatement) (out []interface{}, err error) {

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

		// Remove the field config
		ckey := &keys.FD{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB, FD: ast.Name}
		if err := txn.Del(ckey.Encode()); err != nil {
			return nil, err
		}

	}

	if local {
		txn.Commit()
	}

	return

}

func executeRemoveIndexStatement(txn kvs.TX, ast *sql.RemoveIndexStatement) (out []interface{}, err error) {

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

		// Remove the index config
		ckey := &keys.IX{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB, IX: ast.Name}
		if err := txn.Del(ckey.Encode()); err != nil {
			return nil, err
		}

		// Remove all index data
		dkey := &keys.Index{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: TB, IX: ast.Name, FD: keys.Ignore}
		if err := txn.PDel(dkey.Encode()); err != nil {
			return nil, err
		}

	}

	if local {
		txn.Commit()
	}

	return

}
