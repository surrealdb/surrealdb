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

func executeUpdateStatement(ast *sql.UpdateStatement) (out []interface{}, err error) {

	db.Txn(func(txn *client.Txn) error {

		bch := txn.NewBatch()

		for _, w := range ast.What {

			// var res []interface{}

			switch what := w.(type) {

			case *sql.Thing: // Update a thing

				var res interface{}

				key := &keys.Thing{
					KV: ast.KV,
					NS: ast.NS,
					DB: ast.DB,
					TB: what.Table,
					ID: what.ID,
				}

				if res, err = update(txn, key, ast); err != nil {
					return err
				}

				out = append(out, res)

			case *sql.Table: // Update a table

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

				if res, err = updateMany(txn, beg, end, ast); err != nil {
					return err
				}

				out = append(out, res...)

			}

		}

		return txn.CommitInBatch(bch)

	})

	return

}

// update updates a single record in the database. Before the record is updated, all
// conditions are checked, and if successful, data is merged, and fields are computed.
func update(txn *client.Txn, key *keys.Thing, ast *sql.UpdateStatement) (res interface{}, err error) {

	var kv *kv.KV
	var old *json.Doc
	var doc *json.Doc
	// var dif *diff.DocumentChange

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

	// Modify the record
	if err = mrg(txn, key, old, doc, ast.Data); err != nil {
		return
	}

	// differ := diff.New()
	// dif, _ = differ.Diff(old.Search("data").Data().(map[string]interface{}), doc.Search("data").Data().(map[string]interface{}))

	// Update the record
	if err = cput(txn, key, doc.Bytes(), kv.Actual()); err != nil {
		return
	}

	// trl := &keys.Trail{KV: key.KV, NS: key.NS, DB: key.DB, TB: key.TB, ID: key.ID}
	// if err = cput(txn, trl, "dif", nil); err != nil {
	// 	return
	// }

	res = echo(key, old, doc, nil, ast.Echo, sql.AFTER)

	return

}

// updateMany updates multiple records in the database. Before records are updated, all
// conditions are checked, and if successful, data is merged, and fields are computed.
func updateMany(txn *client.Txn, beg, end *keys.Thing, ast *sql.UpdateStatement) (res []interface{}, err error) {

	kvs, err := rget(txn, beg, end, -1)
	if err != nil {
		return
	}

	for _, kv := range kvs {

		var ret interface{}

		key := &keys.Thing{}
		key.Decode(kv.Key)

		if ret, err = update(txn, key, ast); err != nil {
			return
		}

		res = append(res, ret)

	}

	return

}
