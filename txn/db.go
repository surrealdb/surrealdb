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

func (t *TX) AllDB(ctx context.Context, ns string) (out []*sql.DefineDatabaseStatement, err error) {

	var kvs []kvs.KV

	key := &keys.DB{KV: cnf.Settings.DB.Base, NS: ns, DB: keys.Ignore}

	if kvs, err = t.GetP(ctx, 0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineDatabaseStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	return

}

func (t *TX) GetDB(ctx context.Context, ns, db string) (val *sql.DefineDatabaseStatement, err error) {

	if out, ok := t.get(_db, db); ok {
		return out.(*sql.DefineDatabaseStatement), nil
	}

	var kv kvs.KV

	key := &keys.DB{KV: cnf.Settings.DB.Base, NS: ns, DB: db}

	if kv, err = t.Get(ctx, 0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorDBNotFound
	}

	val = &sql.DefineDatabaseStatement{}
	val.Decode(kv.Val())

	t.set(_db, db, val)

	return

}

func (t *TX) AddDB(ctx context.Context, ns, db string) (val *sql.DefineDatabaseStatement, err error) {

	if out, ok := t.get(_db, db); ok {
		return out.(*sql.DefineDatabaseStatement), nil
	}

	if _, err = t.AddNS(ctx, ns); err != nil {
		return
	}

	var kv kvs.KV

	key := &keys.DB{KV: cnf.Settings.DB.Base, NS: ns, DB: db}

	if kv, _ = t.Get(ctx, 0, key.Encode()); kv.Exi() {
		val = &sql.DefineDatabaseStatement{}
		val.Decode(kv.Val())
		t.set(_db, db, val)
		return
	}

	val = &sql.DefineDatabaseStatement{Name: sql.NewIdent(db)}
	t.PutC(ctx, 0, key.Encode(), val.Encode(), nil)

	t.set(_db, db, val)

	return

}

func (t *TX) DelDB(ns, db string) {

	t.del(_db, db)

}
