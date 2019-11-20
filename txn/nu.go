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

func (t *TX) AllNU(ctx context.Context, ns string) (out []*sql.DefineLoginStatement, err error) {

	var kvs []kvs.KV

	key := &keys.NU{KV: cnf.Settings.DB.Base, NS: ns, US: keys.Ignore}
	if kvs, err = t.GetP(ctx, 0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineLoginStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	return

}

func (t *TX) GetNU(ctx context.Context, ns, us string) (val *sql.DefineLoginStatement, err error) {

	var kv kvs.KV

	key := &keys.NU{KV: cnf.Settings.DB.Base, NS: ns, US: us}
	if kv, err = t.Get(ctx, 0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorNUNotFound
	}

	val = &sql.DefineLoginStatement{}
	val.Decode(kv.Val())

	return

}
