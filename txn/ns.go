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

func (t *TX) AllNS(ctx context.Context) (out []*sql.DefineNamespaceStatement, err error) {

	var kvs []kvs.KV

	key := &keys.NS{KV: cnf.Settings.DB.Base, NS: keys.Ignore}

	if kvs, err = t.GetP(ctx, 0, key.Encode(), 0); err != nil {
		return
	}

	for _, kv := range kvs {
		val := &sql.DefineNamespaceStatement{}
		val.Decode(kv.Val())
		out = append(out, val)
	}

	return

}

func (t *TX) GetNS(ctx context.Context, ns string) (val *sql.DefineNamespaceStatement, err error) {

	if out, ok := t.get(_ns, ns); ok {
		return out.(*sql.DefineNamespaceStatement), nil
	}

	var kv kvs.KV

	key := &keys.NS{KV: cnf.Settings.DB.Base, NS: ns}

	if kv, err = t.Get(ctx, 0, key.Encode()); err != nil {
		return nil, err
	}

	if !kv.Exi() {
		return nil, ErrorNSNotFound
	}

	val = &sql.DefineNamespaceStatement{}
	val.Decode(kv.Val())

	t.set(_ns, ns, val)

	return

}

func (t *TX) AddNS(ctx context.Context, ns string) (val *sql.DefineNamespaceStatement, err error) {

	if out, ok := t.get(_ns, ns); ok {
		return out.(*sql.DefineNamespaceStatement), nil
	}

	var kv kvs.KV

	key := &keys.NS{KV: cnf.Settings.DB.Base, NS: ns}

	if kv, err = t.Get(ctx, 0, key.Encode()); err != nil {
		return
	}

	if kv != nil && kv.Exi() {
		val = &sql.DefineNamespaceStatement{}
		val.Decode(kv.Val())
		t.set(_ns, ns, val)
		return
	}

	val = &sql.DefineNamespaceStatement{Name: sql.NewIdent(ns)}
	t.PutC(ctx, 0, key.Encode(), val.Encode(), nil)

	t.set(_ns, ns, val)

	return

}

func (t *TX) DelNS(ns string) {

	t.del(_ns, ns)

}
