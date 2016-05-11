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
	// "github.com/abcum/surreal/util/json"
	"github.com/abcum/surreal/util/keys"
	"github.com/cockroachdb/cockroach/client"
)

func executeModifyStatement(ast *sql.ModifyStatement) (out []interface{}, err error) {

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

				if res, err = modify(txn, key, ast); err != nil {
					return err
				}

				out = append(out, res)

			}

		}

		return txn.CommitInBatch(bch)

	})

	return

}

// modify modifies a record in the database using jsondiffpatch
func modify(txn *client.Txn, key *keys.Thing, ast *sql.ModifyStatement) (res interface{}, err error) {

	return

}
