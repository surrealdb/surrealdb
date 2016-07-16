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
)

func executeResyncIndexStatement(ast *sql.ResyncIndexStatement) (out []interface{}, err error) {

	txn, err := db.Txn(true)
	if err != nil {
		return
	}

	defer txn.Rollback()

	for _, TB := range ast.What {

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
			doc := item.New(kv, nil)
			if err := resync(txn, doc, ast); err != nil {
				return nil, err
			}
		}

	}

	txn.Commit()

	return

}

func resync(txn kvs.TX, doc *item.Doc, ast *sql.ResyncIndexStatement) (err error) {

	if err = doc.StoreIndex(txn); err != nil {
		return err
	}

	return

}
