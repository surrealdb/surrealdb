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
	"fmt"
	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/item"
	"github.com/abcum/surreal/util/keys"
)

func (e *executor) executeDeleteStatement(txn kvs.TX, ast *sql.DeleteStatement) (out []interface{}, err error) {

	for k, w := range ast.What {
		if what, ok := w.(*sql.Param); ok {
			ast.What[k] = e.ctx.Get(what.ID).Data()
		}
	}

	for _, w := range ast.What {

		switch what := w.(type) {

		default:
			return out, fmt.Errorf("Can not execute DELETE query using value '%v' with type '%T'", what, what)

		case *sql.Thing:
			key := &keys.Thing{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: what.TB, ID: what.ID}
			kv, _ := txn.Get(key.Encode())
			doc := item.New(kv, txn, key, e.ctx)
			if ret, err := delete(doc, ast); err != nil {
				return nil, err
			} else if ret != nil {
				out = append(out, ret)
			}

		case *sql.Table:
			beg := &keys.Thing{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: what.TB, ID: keys.Prefix}
			end := &keys.Thing{KV: ast.KV, NS: ast.NS, DB: ast.DB, TB: what.TB, ID: keys.Suffix}
			kvs, _ := txn.RGet(beg.Encode(), end.Encode(), 0)
			for _, kv := range kvs {
				doc := item.New(kv, txn, nil, e.ctx)
				if ret, err := delete(doc, ast); err != nil {
					return nil, err
				} else if ret != nil {
					out = append(out, ret)
				}
			}

		}

	}

	return

}

func delete(doc *item.Doc, ast *sql.DeleteStatement) (out interface{}, err error) {

	if !doc.Check(ast.Cond) {
		return
	}

	if !doc.Allow("DELETE") {
		return
	}

	if err = doc.Erase(); err != nil {
		return
	}

	if err = doc.PurgeIndex(); err != nil {
		return
	}

	if err = doc.PurgeThing(); err != nil {
		return
	}

	if ast.Hard {

		if err = doc.PurgePatch(); err != nil {
			return
		}

	}

	out = doc.Yield(ast.Echo, sql.NONE)

	return

}
