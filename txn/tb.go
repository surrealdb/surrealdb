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

func (t *TX) AllTB(ctx context.Context, ns, db string) (out []*sql.DefineTableStatement, err error) {

	var kvs []kvs.KV

	key := &keys.TB{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: keys.Ignore}

	if kvs, err = t.GetP(ctx, 0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineTableStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	return

}

func (t *TX) GetTB(ctx context.Context, ns, db, tb string) (val *sql.DefineTableStatement, err error) {

	if out, ok := t.get(_tb, tb); ok {
		return out.(*sql.DefineTableStatement), nil
	}

	var kv kvs.KV

	key := &keys.TB{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb}

	if kv, err = t.Get(ctx, 0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorTBNotFound
	}

	val = &sql.DefineTableStatement{}
	val.Decode(kv.Val())

	t.set(_tb, tb, val)

	return

}

func (t *TX) AddTB(ctx context.Context, ns, db, tb string) (val *sql.DefineTableStatement, err error) {

	if out, ok := t.get(_tb, tb); ok {
		return out.(*sql.DefineTableStatement), nil
	}

	if _, err = t.AddDB(ctx, ns, db); err != nil {
		return
	}

	var kv kvs.KV

	key := &keys.TB{KV: cnf.Settings.DB.Base, NS: ns, DB: db, TB: tb}

	if kv, err = t.Get(ctx, 0, key.Encode()); err != nil {
		return
	}

	if kv != nil && kv.Exi() {
		val = &sql.DefineTableStatement{}
		val.Decode(kv.Val())
		t.set(_tb, tb, val)
		return
	}

	val = &sql.DefineTableStatement{Name: sql.NewIdent(tb)}
	t.PutC(ctx, 0, key.Encode(), val.Encode(), nil)

	t.set(_tb, tb, val)

	return

}

func (t *TX) DelTB(ns, db, tb string) {

	t.del(_tb, tb)

}
