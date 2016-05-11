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
	"github.com/abcum/surreal/kv"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/json"
	"github.com/abcum/surreal/util/keys"
	"github.com/cockroachdb/cockroach/client"
)

func executeDeleteStatement(ast *sql.DeleteStatement) (out []interface{}, err error) {

	db.Txn(func(txn *client.Txn) error {

		b := txn.NewBatch()

		for _, w := range ast.What {

			switch what := w.(type) {

			case *sql.Thing: // Delete a thing

				var res interface{}

				key := &keys.Thing{
					KV: ast.KV,
					NS: ast.NS,
					DB: ast.DB,
					TB: what.Table,
					ID: what.ID,
				}

				if res, err = delete(txn, key, ast); err != nil {
					return err
				}

				out = append(out, res)

			case *sql.Table: // Delete a table

				var res []interface{}

				beg := &keys.Thing{
					KV: ast.KV,
					NS: ast.NS,
					DB: ast.DB,
					TB: what.Name,
					ID: keys.Prefix,
				}

				end := &keys.Thing{
					KV: ast.KV,
					NS: ast.NS,
					DB: ast.DB,
					TB: what.Name,
					ID: keys.Suffix,
				}

				if res, err = deleteMany(txn, beg, end, ast); err != nil {
					return err
				}

				out = append(out, res...)

			}

		}

		return txn.CommitInBatch(b)

	})

	return

}

// delete deletes a single record from the database. Before the record is deleted, all
// conditions are checked, and if successful, the record, all edges, trail data, and
// event data are deleted aswell.
func delete(txn *client.Txn, key *keys.Thing, ast *sql.DeleteStatement) (res interface{}, err error) {

	var kv *kv.KV
	var old *json.Doc
	var doc *json.Doc

	// Check conditions
	if !match(txn, key, ast.Cond) {
		return
	}

	// Select the record
	if kv, err = get(txn, key); err != nil {
		return
	}

	// Parse the record
	if old, doc, err = new(txn, key, kv); err != nil {
		return
	}

	// Delete the record
	if err = del(txn, key); err != nil {
		return
	}

	doc.Reset()

	res = echo(key, old, doc, nil, ast.Echo, sql.ID)

	return

}

// deleteMany deletes multiple records from the database. Before the records are deleted,
// all conditions are checked, and if successful, the records, all edges, trail data,
// and event data are deleted aswell.
func deleteMany(txn *client.Txn, beg, end *keys.Thing, ast *sql.DeleteStatement) (res []interface{}, err error) {

	kvs, err := rget(txn, beg, end, -1)
	if err != nil {
		return
	}

	for _, kv := range kvs {

		var ret interface{}

		key := &keys.Thing{}
		key.Decode(kv.Key)

		if ret, err = delete(txn, key, ast); err != nil {
			return
		}

		res = append(res, ret)

	}

	return

}
