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

package item

import (
	"fmt"

	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
	"github.com/abcum/surreal/util/keys"
	"github.com/abcum/surreal/util/pack"
)

type Doc struct {
	kv      kvs.KV
	id      string
	txn     kvs.TX
	key     *keys.Thing
	initial *data.Doc
	current *data.Doc
	fields  []*sql.DefineFieldStatement
	indexs  []*sql.DefineIndexStatement
	rules   map[string]*sql.DefineRulesStatement
}

func New(kv kvs.KV, txn kvs.TX, key *keys.Thing) (this *Doc) {

	this = &Doc{kv: kv, key: key, txn: txn}

	if key == nil {
		this.key = &keys.Thing{}
		this.key.Decode(kv.Key())
	}

	if kv.Exists() == false {
		this.initial = data.New()
		this.current = data.New()
	}

	if kv.Exists() == true {
		this.initial = data.New().Decode(kv.Val())
		this.current = data.New().Decode(kv.Val())
	}

	if !this.current.Exists("meta") {
		this.initial.Object("meta")
		this.current.Object("meta")
	}

	if !this.current.Exists("data") {
		this.initial.Object("data")
		this.current.Object("data")
	}

	if !this.current.Exists("time") {
		this.initial.Object("time")
		this.current.Object("time")
	}

	this.id = fmt.Sprintf("@%v:%v", this.key.TB, this.key.ID)

	return this

}

func (this *Doc) getRules() {

	this.rules = make(map[string]*sql.DefineRulesStatement)

	beg := &keys.RU{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, RU: keys.Prefix}
	end := &keys.RU{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, RU: keys.Suffix}
	rng, _ := this.txn.RGet(beg.Encode(), end.Encode(), 0)

	for _, kv := range rng {
		var rul sql.DefineRulesStatement
		key := new(keys.RU)
		key.Decode(kv.Key())
		if str, ok := key.RU.(string); ok {
			pack.FromPACK(kv.Val(), &rul)
			this.rules[str] = &rul
		}
	}

	return

}

func (this *Doc) getFields() {

	beg := &keys.FD{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, FD: keys.Prefix}
	end := &keys.FD{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, FD: keys.Suffix}
	rng, _ := this.txn.RGet(beg.Encode(), end.Encode(), 0)

	for _, kv := range rng {
		var fld sql.DefineFieldStatement
		pack.FromPACK(kv.Val(), &fld)
		this.fields = append(this.fields, &fld)
	}

	return

}

func (this *Doc) getIndexs() {

	beg := &keys.IX{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, IX: keys.Prefix}
	end := &keys.IX{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, IX: keys.Suffix}
	rng, _ := this.txn.RGet(beg.Encode(), end.Encode(), 0)

	for _, kv := range rng {
		var idx sql.DefineIndexStatement
		pack.FromPACK(kv.Val(), &idx)
		this.indexs = append(this.indexs, &idx)
	}

	return

}

func (this *Doc) StartThing() (err error) {

	dkey := &keys.DB{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB}
	if err := this.txn.Put(dkey.Encode(), nil); err != nil {
		return err
	}

	tkey := &keys.TB{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB}
	if err := this.txn.Put(tkey.Encode(), nil); err != nil {
		return err
	}

	return this.txn.CPut(this.key.Encode(), this.current.Encode(), nil)

}

func (this *Doc) PurgeThing() (err error) {

	return this.txn.Del(this.key.Encode())

}

func (this *Doc) StoreThing() (err error) {

	dkey := &keys.DB{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB}
	if err := this.txn.Put(dkey.Encode(), nil); err != nil {
		return err
	}

	tkey := &keys.TB{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB}
	if err := this.txn.Put(tkey.Encode(), nil); err != nil {
		return err
	}

	return this.txn.CPut(this.key.Encode(), this.current.Encode(), this.kv.Val())

}

func (this *Doc) PurgePatch() (err error) {

	beg := &keys.Patch{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, ID: this.key.ID, AT: keys.StartOfTime}
	end := &keys.Patch{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, ID: this.key.ID, AT: keys.EndOfTime}
	return this.txn.RDel(beg.Encode(), end.Encode(), 0)

}

func (this *Doc) StorePatch() (err error) {

	key := &keys.Patch{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, ID: this.key.ID}
	return this.txn.CPut(key.Encode(), this.diff().Encode(), nil)

}

func (this *Doc) PurgeIndex() (err error) {

	for _, index := range this.indexs {

		if index.Uniq == true {
			for _, o := range buildIndex(index.Cols, this.initial) {
				key := &keys.Index{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, IX: index.Name, FD: o}
				this.txn.CDel(key.Encode(), []byte(this.id))
			}
		}

		if index.Uniq == false {
			for _, o := range buildIndex(index.Cols, this.initial) {
				key := &keys.Point{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, IX: index.Name, FD: o, ID: this.key.ID}
				this.txn.CDel(key.Encode(), []byte(this.id))
			}
		}

	}

	return

}

func (this *Doc) StoreIndex() (err error) {

	for _, index := range this.indexs {

		if index.Uniq == true {
			for _, o := range buildIndex(index.Cols, this.initial) {
				oidx := &keys.Index{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, IX: index.Name, FD: o}
				this.txn.CDel(oidx.Encode(), []byte(this.id))
			}
			for _, n := range buildIndex(index.Cols, this.current) {
				nidx := &keys.Index{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, IX: index.Name, FD: n}
				if err = this.txn.CPut(nidx.Encode(), []byte(this.id), nil); err != nil {
					return fmt.Errorf("Duplicate entry for %v in index '%s' on %s", n, index.Name, this.key.TB)
				}
			}
		}

		if index.Uniq == false {
			for _, o := range buildIndex(index.Cols, this.initial) {
				oidx := &keys.Point{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, IX: index.Name, FD: o, ID: this.key.ID}
				this.txn.CDel(oidx.Encode(), []byte(this.id))
			}
			for _, n := range buildIndex(index.Cols, this.current) {
				nidx := &keys.Point{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, IX: index.Name, FD: n, ID: this.key.ID}
				if err = this.txn.CPut(nidx.Encode(), []byte(this.id), nil); err != nil {
					return fmt.Errorf("Multiple items with id %s in index '%s' on %s", this.key.ID, index.Name, this.key.TB)
				}
			}
		}

	}

	return

}

func buildIndex(cols []string, item *data.Doc) (out [][]interface{}) {

	if len(cols) == 0 {
		return [][]interface{}{nil}
	}

	col, cols := cols[0], cols[1:]

	sub := buildIndex(cols, item)

	if arr, ok := item.Get("data", col).Data().([]interface{}); ok {
		for _, s := range sub {
			for _, a := range arr {
				idx := []interface{}{}
				idx = append(idx, a)
				idx = append(idx, s...)
				out = append(out, idx)
			}
		}
	} else {
		for _, s := range sub {
			idx := []interface{}{}
			idx = append(idx, item.Get("data", col).Data())
			idx = append(idx, s...)
			out = append(out, idx)
		}
	}

	return

}
