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
	"github.com/abcum/surreal/util/uuid"
	"github.com/cockroachdb/cockroach/client"
)

func executeCreateStatement(ast *sql.CreateStatement) (out []interface{}, err error) {

	db.Txn(func(txn *client.Txn) error {

		bch := txn.NewBatch()

		for _, w := range ast.What {

			switch what := w.(type) {

			case *sql.Thing: // Create a thing

				var res interface{}

				key := &keys.Thing{
					KV: ast.KV,
					NS: ast.NS,
					DB: ast.DB,
					TB: what.Table,
					ID: what.ID,
				}

				if res, err = create(txn, key, ast); err != nil {
					return err
				}

				out = append(out, res)

			case *sql.Table: // Create a table

				var res interface{}

				key := &keys.Thing{
					KV: ast.KV,
					NS: ast.NS,
					DB: ast.DB,
					TB: what.Name,
					ID: uuid.NewV5(uuid.NewV4().UUID, ast.KV).String(),
				}

				if res, err = create(txn, key, ast); err != nil {
					return err
				}

				out = append(out, res)

			}

		}

		return txn.CommitInBatch(bch)

	})

	return

}

// create creates a single record in the database. The new record will only be created
// if no such record already exists in the database. If no record exists, then the
// record is created from the supplied data, and fields are computed.
func create(txn *client.Txn, key *keys.Thing, ast *sql.CreateStatement) (res interface{}, err error) {

	var kv *kv.KV
	var old *json.Doc
	var doc *json.Doc

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

	// Create the record
	if err = cput(txn, key, doc.Bytes(), nil); err != nil {
		return
	}

	res = echo(key, old, doc, nil, ast.Echo, sql.AFTER)

	return

}
