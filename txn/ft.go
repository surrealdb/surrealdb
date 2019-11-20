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

package txn

import (
	"context"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/keys"
)

func (t *TX) AllFT(ctx context.Context, ns, db, tb string) (out []*sql.DefineTableStatement, err error) {

	if out, ok := t.get(_ft, tb); ok {
		return out.([]*sql.DefineTableStatement), nil
	}

	var kvs []kvs.KV

	key := &keys.FT{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb, FT: keys.Ignore}

	if kvs, err = t.GetP(ctx, 0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineTableStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	t.set(_ft, tb, out)

	return

}

func (t *TX) DelFT(ns, db, tb, ft string) {

	t.del(_ft, tb)

}
