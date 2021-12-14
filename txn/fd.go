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

func (t *TX) AllFD(ctx context.Context, ns, db, tb string) (out []*sql.DefineFieldStatement, err error) {

	if out, ok := t.get(_fd, tb); ok {
		return out.([]*sql.DefineFieldStatement), nil
	}

	var kvs []kvs.KV

	key := &keys.FD{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, FD: keys.Ignore}

	if kvs, err = t.GetP(ctx, 0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineFieldStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	t.set(_fd, tb, out)

	return

}

func (t *TX) DelFD(ns, db, tb, fd string) {

	t.del(_fd, tb)

}
