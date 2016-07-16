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
	"time"

	"github.com/imdario/mergo"
	"github.com/robertkrimen/otto"

	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
	// "github.com/abcum/surreal/util/diff"
	"github.com/abcum/surreal/util/keys"
)

type field struct {
	Type      string
	Name      string
	Code      string
	Enum      []interface{}
	Min       int64
	Max       int64
	Default   interface{}
	Notnull   bool
	Readonly  bool
	Mandatory bool
}

type index struct {
	uniq bool
	name string
	code string
	cols []string
}

type Doc struct {
	kv      kvs.KV
	id      string
	key     *keys.Thing
	initial *data.Doc
	current *data.Doc
	fieldes []*field
	indexes []*index
}

func New(kv kvs.KV, key *keys.Thing) (this *Doc) {

	this = &Doc{kv: kv, key: key}

	if key == nil {
		this.key = &keys.Thing{}
		this.key.Decode(kv.Key())
	}

	if kv.Exists() == false {
		this.initial = data.New()
		this.current = data.New()
	}

	if kv.Exists() == true {
		this.initial = data.NewFromPACK(kv.Val())
		this.current = data.NewFromPACK(kv.Val())
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

func (this *Doc) Allow(txn kvs.TX, cond string) (val bool) {
	return true
}

func (this *Doc) Check(txn kvs.TX, cond []sql.Expr) (val bool) {
	return true
}

func (this *Doc) Erase(txn kvs.TX, data []sql.Expr) (err error) {
	this.current.Reset()
	return
}

func (this *Doc) Merge(txn kvs.TX, data []sql.Expr) (err error) {

	now := time.Now()

	for _, part := range data {

		switch expr := part.(type) {
		case *sql.DiffExpression:
			this.mrgDpm(expr)
		case *sql.BinaryExpression:
			this.mrgOne(expr)
		case *sql.MergeExpression:
			this.mrgAny(expr)
		case *sql.ContentExpression:
			this.mrgAll(expr)
		}

	}

	// Set meta
	this.current.Set(this.key.TB, "meta", "table")
	this.current.Set(this.key.ID, "meta", "ident")

	// Set time
	this.current.New(now, "time", "created")
	this.current.Set(now, "time", "updated")

	// Set data
	this.current.Set(this.id, "id")
	this.current.Set(this.id, "data", "id")

	// Set fields
	err = this.mrgFld(txn)

	// Set data
	this.current.Set(this.id, "id")
	this.current.Set(this.id, "data", "id")

	// Set time
	this.current.New(now, "time", "created")
	this.current.Set(now, "time", "updated")

	// Set meta
	this.current.Set(this.key.TB, "meta", "table")
	this.current.Set(this.key.ID, "meta", "ident")

	return

}

func (this *Doc) StartThing(txn kvs.TX) (err error) {

	dkey := &keys.DB{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB}
	if err := txn.Put(dkey.Encode(), nil); err != nil {
		return err
	}

	tkey := &keys.TB{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB}
	if err := txn.Put(tkey.Encode(), nil); err != nil {
		return err
	}

	return txn.CPut(this.key.Encode(), this.current.ToPACK(), nil)

}

func (this *Doc) PurgeThing(txn kvs.TX) (err error) {

	return txn.Del(this.key.Encode())

}

func (this *Doc) StoreThing(txn kvs.TX) (err error) {

	dkey := &keys.DB{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB}
	if err := txn.Put(dkey.Encode(), nil); err != nil {
		return err
	}

	tkey := &keys.TB{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB}
	if err := txn.Put(tkey.Encode(), nil); err != nil {
		return err
	}

	return txn.CPut(this.key.Encode(), this.current.ToPACK(), this.kv.Val())

}

func (this *Doc) PurgePatch(txn kvs.TX) (err error) {

	beg := &keys.Patch{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, ID: this.key.ID, AT: keys.StartOfTime}
	end := &keys.Patch{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, ID: this.key.ID, AT: keys.EndOfTime}
	return txn.RDel(beg.Encode(), end.Encode(), 0)

}

func (this *Doc) StorePatch(txn kvs.TX) (err error) {

	key := &keys.Patch{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, ID: this.key.ID}
	return txn.CPut(key.Encode(), this.diff(), nil)

}

func (this *Doc) PurgeTrail(txn kvs.TX) (err error) {

	beg := &keys.Trail{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, AT: keys.StartOfTime}
	end := &keys.Trail{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, AT: keys.EndOfTime}
	return txn.RDel(beg.Encode(), end.Encode(), 0)

}

func (this *Doc) StoreTrail(txn kvs.TX) (err error) {

	key := &keys.Trail{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB}
	return txn.CPut(key.Encode(), this.diff(), nil)

}

func (this *Doc) PurgeIndex(txn kvs.TX) (err error) {

	for _, index := range this.indexes {

		old := []interface{}{}

		for _, col := range index.cols {
			old = append(old, this.initial.Get("data", col).Data())
		}

		if index.uniq == true {
			key := &keys.Index{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, IX: index.name, FD: old}
			txn.CDel(key.Encode(), []byte(this.id))
		}

		if index.uniq == false {
			key := &keys.Point{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, IX: index.name, FD: old, ID: this.key.ID}
			txn.CDel(key.Encode(), []byte(this.id))
		}

	}

	return

}

func (this *Doc) StoreIndex(txn kvs.TX) (err error) {

	for _, index := range this.indexes {

		old := []interface{}{}
		now := []interface{}{}

		for _, col := range index.cols {
			old = append(old, this.initial.Get("data", col).Data())
			now = append(now, this.current.Get("data", col).Data())
		}

		if index.uniq == true {
			oidx := &keys.Index{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, IX: index.name, FD: old}
			txn.CDel(oidx.Encode(), []byte(this.id))
			nidx := &keys.Index{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, IX: index.name, FD: now}
			if err = txn.CPut(nidx.Encode(), []byte(this.id), nil); err != nil {
				return fmt.Errorf("Duplicate entry %v in index '%s.%s'", now, this.key.TB, index.name)
			}
		}

		if index.uniq == false {
			oidx := &keys.Point{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, IX: index.name, FD: old, ID: this.key.ID}
			txn.CDel(oidx.Encode(), []byte(this.id))
			nidx := &keys.Point{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, IX: index.name, FD: now, ID: this.key.ID}
			if err = txn.CPut(nidx.Encode(), []byte(this.id), nil); err != nil {
				return fmt.Errorf("Multiple items with id %s in index '%s.%s'", this.key.ID, this.key.TB, index.name)
			}
		}

	}

	return

}

func (this *Doc) Yield(output sql.Token, fallback sql.Token) (res interface{}) {

	if output == 0 {
		output = fallback
	}

	switch output {
	default:
		res = nil
	case sql.ID:
		res = fmt.Sprintf("@%v:%v", this.key.TB, this.key.ID)
	case sql.DIFF:
		res = this.diff()
	case sql.FULL:
		res = this.current.Data()
	case sql.AFTER:
		res = this.current.Get("data").Data()
	case sql.BEFORE:
		res = this.initial.Get("data").Data()
	case sql.BOTH:
		res = map[string]interface{}{
			"After":  this.current.Get("data").Data(),
			"Before": this.initial.Get("data").Data(),
		}
	}

	return

}

// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------

func (this *Doc) diff() []byte {
	// *diff.Diff
	return []byte("DIFF")
}

func (this *Doc) getFlds(txn kvs.TX) (out []*field) {

	beg := &keys.FD{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, FD: keys.Prefix}
	end := &keys.FD{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, FD: keys.Suffix}
	rng, _ := txn.RGet(beg.Encode(), end.Encode(), 0)

	for _, kv := range rng {

		inf := data.NewFromPACK(kv.Val())

		fld := &field{}

		fld.Type, _ = inf.Get("type").Data().(string)
		fld.Name, _ = inf.Get("name").Data().(string)
		fld.Code, _ = inf.Get("code").Data().(string)
		fld.Min, _ = inf.Get("min").Data().(int64)
		fld.Max, _ = inf.Get("max").Data().(int64)
		fld.Default = inf.Get("default").Data()
		fld.Notnull = inf.Get("notnull").Data().(bool)
		fld.Readonly = inf.Get("readonly").Data().(bool)
		fld.Mandatory = inf.Get("mandatory").Data().(bool)

		out = append(out, fld)

	}

	return

}

func (this *Doc) getIdxs(txn kvs.TX) (out []*data.Doc) {

	beg := &keys.IX{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, IX: keys.Prefix}
	end := &keys.IX{KV: this.key.KV, NS: this.key.NS, DB: this.key.DB, TB: this.key.TB, IX: keys.Suffix}
	rng, _ := txn.RGet(beg.Encode(), end.Encode(), 0)

	for _, kv := range rng {
		idx := data.NewFromPACK(kv.Val())
		out = append(out, idx)
	}

	return

}

// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------

func (this *Doc) mrgFld(txn kvs.TX) (err error) {

	vm := otto.New()

	vm.Set("doc", this.current.Data())
	vm.Set("data", this.current.Get("data").Data())
	vm.Set("meta", this.current.Get("meta").Data())
	vm.Set("time", this.current.Get("time").Data())

	for _, fld := range this.getFlds(txn) {

		initial := this.initial.Get("data", fld.Name).Data()

		if fld.Readonly && initial != nil {
			this.current.Set(initial, "data", fld.Name)
			return
		}

		if fld.Code != "" {

			ret, err := vm.Run("(function() { " + fld.Code + " })()")
			if err != nil {
				return fmt.Errorf("Problem executing code: %v %v", fld.Code, err.Error())
			}

			val, err := ret.Export()
			if err != nil {
				return fmt.Errorf("Problem executing code: %v %v", fld.Code, err.Error())
			}

			if ret.IsDefined() {
				this.current.Set(val, "data", fld.Name)
			}

			if ret.IsUndefined() {
				this.current.Del("data", fld.Name)
			}

		}

		current := this.current.Get("data", fld.Name).Data()
		exists := this.current.Exists("data", fld.Name)

		if fld.Default != nil && exists == false {
			this.current.Set(fld.Default, "data", fld.Name)
		}

		if fld.Notnull && exists == true && current == nil {
			return fmt.Errorf("Can't be null field '%v'", fld.Name)
		}

		if fld.Mandatory && exists == false {
			return fmt.Errorf("Need to set field '%v'", fld.Name)
		}

		if fld.Type != "" {

		}

	}

	return

}

func (this *Doc) mrgAll(expr *sql.ContentExpression) {

	val := data.Consume(expr.JSON)

	this.current.Set(val.Data(), "data")

}

func (this *Doc) mrgAny(expr *sql.MergeExpression) {

	lhs, _ := this.current.Get("data").Data().(map[string]interface{})
	rhs, _ := expr.JSON.(map[string]interface{})

	err := mergo.MapWithOverwrite(&lhs, rhs)
	if err != nil {
		return
	}

	this.current.Set(lhs, "data")

}

func (this *Doc) mrgDpm(expr *sql.DiffExpression) {

}

func (this *Doc) mrgOne(expr *sql.BinaryExpression) {

	lhs := getDataItemLHS(this.current, expr.LHS)
	rhs := getDataItemRHS(this.current, expr.RHS)

	if expr.Op == "=" {
		switch expr.RHS.(type) {
		default:
			this.current.Set(rhs, "data", lhs)
		case *sql.Void:
			this.current.Del("data", lhs)
		}
	}

	if expr.Op == "+=" {
		this.current.ArrayAdd(rhs, "data", lhs)
	}

	if expr.Op == "-=" {
		this.current.ArrayDel(rhs, "data", lhs)
	}

}

// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------

func getDataItemLHS(doc *data.Doc, expr sql.Expr) string {

	switch val := expr.(type) {
	default:
		return ""
	case sql.Ident:
		return string(val)
	}

}

func getDataItemRHS(doc *data.Doc, expr sql.Expr) interface{} {

	switch val := expr.(type) {
	default:
		return nil
	case time.Time:
		return val
	case bool, int64, float64, string:
		return val
	case []interface{}, map[string]interface{}:
		return val
	case sql.Ident:
		return doc.Get("data", string(val)).Data()
	}

}
