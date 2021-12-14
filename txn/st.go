// Copyright © 2016 SurrealDB Ltd.
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

package txn

import (
	"context"

	"github.com/surrealdb/surrealdb/cnf"
	"github.com/surrealdb/surrealdb/kvs"
	"github.com/surrealdb/surrealdb/sql"
	"github.com/surrealdb/surrealdb/util/keys"
)

func (t *TX) AllST(ctx context.Context, ns, db, sc string) (out []*sql.DefineTokenStatement, err error) {

	var kvs []kvs.KV

	key := &keys.ST{KV: cnf.Settings.DB.Base, NS: ns, DB: db, SC: sc, TK: keys.Ignore}
	if kvs, err = t.GetP(ctx, 0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineTokenStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	return

}

func (t *TX) GetST(ctx context.Context, ns, db, sc, tk string) (val *sql.DefineTokenStatement, err error) {

	var kv kvs.KV

	key := &keys.ST{KV: cnf.Settings.DB.Base, NS: ns, DB: db, SC: sc, TK: tk}
	if kv, err = t.Get(ctx, 0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorSTNotFound
	}

	val = &sql.DefineTokenStatement{}
	val.Decode(kv.Val())

	return

}
