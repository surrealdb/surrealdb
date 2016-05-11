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
	"time"

	"github.com/imdario/mergo"
	"github.com/robertkrimen/otto"

	"github.com/abcum/surreal/err"
	"github.com/abcum/surreal/kv"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/json"
	"github.com/abcum/surreal/util/keys"
	"github.com/abcum/surreal/util/uuid"
	"github.com/cockroachdb/cockroach/client"
)

func put(txn *client.Txn, key keys.Key, val interface{}) (err error) {

	if rer := txn.Put(key.Encode(), val); rer != nil {
		return &errors.DBError{Err: rer, Key: key}
	}

	return

}

// CPut conditionally puts an item into the backend store
func cput(txn *client.Txn, key keys.Key, val interface{}, was interface{}) (err error) {

	if rer := txn.CPut(key.Encode(), val, was); rer != nil {
		return &errors.ExistsError{Err: rer, Key: key}
	}

	return

}

func get(txn *client.Txn, key keys.Key) (res *kv.KV, err error) {

	ckv, rer := txn.Get(key.Encode())
	if rer != nil {
		return nil, &errors.DBError{Err: rer, Key: key}
	}

	res = &kv.KV{
		Exi: ckv.Exists(),
		Key: []byte(ckv.Key.String()),
		Val: ckv.ValueBytes(),
	}

	return

}

func rget(txn *client.Txn, beg, end keys.Key, max int64) (res []*kv.KV, err error) {

	mul, rer := txn.Scan(beg.Encode(), end.Encode(), max)
	if rer != nil {
		return nil, &errors.DBError{Err: rer, Beg: beg, End: end}
	}

	for _, ckv := range mul {
		res = append(res, &kv.KV{
			Exi: ckv.Exists(),
			Key: []byte(ckv.Key.String()),
			Val: ckv.ValueBytes(),
		})
	}

	return

}

func del(txn *client.Txn, key keys.Key) error {

	if rer := txn.Del(key.Encode()); rer != nil {
		return &errors.DBError{Err: rer, Key: key}
	}

	return nil

}

func rdel(txn *client.Txn, beg, end keys.Key) error {

	if rer := txn.DelRange(beg.Encode(), end.Encode()); rer != nil {
		return &errors.DBError{Err: rer, Beg: beg, End: end}
	}

	return nil

}

func new(txn *client.Txn, key *keys.Thing, kv *kv.KV) (old *json.Doc, doc *json.Doc, err error) {

	if len(kv.Val) == 0 {
		old, err = json.Setup()
		doc, err = json.Setup()
		if err != nil {
			err = &errors.DataError{Err: err, Data: kv.Val}
			return
		}
	}

	if len(kv.Val) >= 1 {
		old, err = json.Parse(kv.Val)
		doc, err = json.Parse(kv.Val)
		if err != nil {
			err = &errors.DataError{Err: err, Data: kv.Val}
			return
		}
	}

	if !doc.Exists("meta") {
		old.Object("meta")
		doc.Object("meta")
	}

	if !doc.Exists("data") {
		old.Object("data")
		doc.Object("data")
	}

	if !doc.Exists("time") {
		old.Object("time")
		doc.Object("time")
	}

	return

}

func mrg(txn *client.Txn, key *keys.Thing, old, doc *json.Doc, data []sql.Expr) (err error) {

	now := time.Now()

	for _, part := range data {

		switch expr := part.(type) {
		case *sql.BinaryExpression:
			updateOne(doc, expr)
		case *sql.MergeExpression:
			updateAny(doc, expr)
		case *sql.ContentExpression:
			updateAll(doc, expr)
		}

	}

	if err = fld(txn, key, old, doc); err != nil {
		return
	}

	// Set time
	doc.New(now, "time", "created")
	doc.Set(now, "time", "updated")

	// Set meta
	doc.Set(key.TB, "meta", "table")
	doc.Set(key.ID, "meta", "ident")

	// Set data
	doc.Fmt("@%v:%v", key.TB, key.ID).Set("id")
	doc.Fmt("@%v:%v", key.TB, key.ID).Set("data", "id")

	return

}

func fld(txn *client.Txn, key keys.Key, old, doc *json.Doc) (err error) {

	fld := "fullname"

	vm := otto.New()

	vm.Set("doc", doc.Data())

	vm.Set("UUID", func(call otto.FunctionCall) otto.Value {
		result, _ := vm.ToValue(uuid.NewV4())
		return result
	})

	fnc := `
    if (doc.data.firstname && doc.data.lastname) {
        return doc.data.firstname + ' ' + doc.data.lastname;
    } else if (doc.data.firstname) {
        return doc.data.firstname;
    } else if (doc.data.lastname) {
        return doc.data.lastname;
    } else {
        return undefined
    }
    `

	ret, err := vm.Run("(function() { " + fnc + " })()")
	if err != nil {
		return &errors.CodeError{Err: err, Name: fld, Code: fnc}
	}

	val, err := ret.Export()
	if err != nil {
		return &errors.CodeError{Err: err, Name: fld, Code: fnc}
	}

	if ret.IsDefined() {
		doc.Set(val, "data", fld)
	}

	if ret.IsUndefined() {
		doc.Del("data", fld)
	}

	return nil

}

func echo(key *keys.Thing, old *json.Doc, doc *json.Doc, dif interface{}, output sql.Token, fallback sql.Token) (res interface{}) {

	if output == 0 {
		output = fallback
	}

	switch output {
	case sql.NONE:
		res = nil
	case sql.ID:
		res = fmt.Sprintf("@%v:%v", key.TB, key.ID)
	case sql.DIFF:
		res = dif
	case sql.FULL:
		res = doc.Data()
	case sql.AFTER:
		res = doc.Search("data").Data()
	case sql.BEFORE:
		res = old.Search("data").Data()
	case sql.BOTH:
		res = map[string]interface{}{
			"After":  doc.Search("data").Data(),
			"Before": old.Search("data").Data(),
		}
	}

	return

}

func match(txn *client.Txn, key keys.Key, cond []sql.Expr) bool {
	return true
}

func updateAll(doc *json.Doc, expr *sql.ContentExpression) {

	val, err := json.Consume(expr.JSON.Val)
	if err != nil {
		return
	}

	doc.Set(val.Data(), "data")

}

func updateAny(doc *json.Doc, expr *sql.MergeExpression) {

	lhs, _ := doc.Search("data").Data().(map[string]interface{})
	rhs, _ := expr.JSON.Val.(map[string]interface{})

	err := mergo.MapWithOverwrite(&lhs, rhs)
	if err != nil {
		return
	}

	doc.Set(lhs, "data")

}

func updateOne(doc *json.Doc, expr *sql.BinaryExpression) {

	lhs := getDataItemLHS(doc, expr.LHS)
	rhs := getDataItemRHS(doc, expr.RHS)

	if expr.Op == "=" {
		switch expr.RHS.(type) {
		default:
			doc.Set(rhs, "data", lhs)
		case *sql.Void:
			doc.Del("data", lhs)
		}
	}

	if expr.Op == "+=" {
		doc.ArrayAdd(rhs, "data", lhs)
	}

	if expr.Op == "-=" {
		doc.ArrayDel(rhs, "data", lhs)
	}

}

func getDataItemLHS(doc *json.Doc, expr sql.Expr) string {

	switch part := expr.(type) {
	default:
		return ""
	case *sql.IdentLiteral:
		return part.Val
	}

}

func getDataItemRHS(doc *json.Doc, expr sql.Expr) interface{} {

	switch part := expr.(type) {
	default:
		return nil
	case *sql.Null:
		return nil
	case *sql.JSONLiteral:
		return part.Val
	case *sql.ArrayLiteral:
		return part.Val
	case *sql.BytesLiteral:
		return part.Val
	case *sql.NumberLiteral:
		return part.Val
	case *sql.DoubleLiteral:
		return part.Val
	case *sql.StringLiteral:
		return part.Val
	case *sql.BooleanLiteral:
		return part.Val
	case *sql.DatetimeLiteral:
		return part.Val
	case *sql.IdentLiteral:
		return doc.Search(part.Val).Data()
	}

}
