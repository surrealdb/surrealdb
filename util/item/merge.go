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
	"time"

	"github.com/imdario/mergo"

	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
)

func (this *Doc) Merge(data []sql.Expr) (err error) {

	this.getFields()
	this.getIndexs()

	if err = this.setFld(); err != nil {
		return
	}

	if err = this.defFld(); err != nil {
		return
	}

	if err = this.setFld(); err != nil {
		return
	}

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

	if err = this.setFld(); err != nil {
		return
	}

	if err = this.mrgFld(); err != nil {
		return
	}

	if err = this.setFld(); err != nil {
		return
	}

	return

}

func (this *Doc) setFld() (err error) {

	this.current.Set(this.key.ID, "id")
	this.current.Set(this.key.TB, "tb")

	return

}

func (this *Doc) defFld() (err error) {

	for _, fld := range this.fields {

		this.current.Walk(func(key string, val interface{}) error {

			v := this.current.Valid(key)
			e := this.current.Exists(key)

			if fld.Default != nil && (!e || !v && fld.Notnull) {
				switch val := fld.Default.(type) {
				case sql.Void, *sql.Void:
					this.current.Del(key)
				case sql.Null, *sql.Null:
					this.current.Set(nil, key)
				default:
					this.current.Set(fld.Default, key)
				case sql.Ident:
					this.current.Set(this.current.Get(val.ID).Data(), key)
				case *sql.Ident:
					this.current.Set(this.current.Get(val.ID).Data(), key)
				}
			}

			return nil

		}, fld.Name)

	}

	return

}

func (this *Doc) mrgFld() (err error) {

	for _, fld := range this.fields {
		if err = this.each(fld); err != nil {
			return
		}
	}

	return

}

func (this *Doc) mrgAll(expr *sql.ContentExpression) {

	this.current = data.Consume(expr.JSON)

}

func (this *Doc) mrgAny(expr *sql.MergeExpression) {

	lhs, _ := this.current.Data().(map[string]interface{})
	rhs, _ := expr.JSON.(map[string]interface{})

	err := mergo.MapWithOverwrite(&lhs, rhs)
	if err != nil {
		return
	}

	this.current = data.Consume(lhs)

}

func (this *Doc) mrgDpm(expr *sql.DiffExpression) {

}

func (this *Doc) mrgOne(expr *sql.BinaryExpression) {

	lhs := getMrgItemLHS(this.current, expr.LHS)
	rhs := getMrgItemRHS(this.current, expr.RHS)

	if expr.Op == sql.EQ {
		switch expr.RHS.(type) {
		default:
			this.current.Set(rhs, lhs)
		case *sql.Void:
			this.current.Del(lhs)
		}
	}

	if expr.Op == sql.INC {
		this.current.Inc(rhs, lhs)
	}

	if expr.Op == sql.DEC {
		this.current.Dec(rhs, lhs)
	}

}

func getMrgItemLHS(doc *data.Doc, expr sql.Expr) string {

	switch val := expr.(type) {
	default:
		return ""
	case sql.Ident:
		return val.ID
	case *sql.Ident:
		return val.ID
	}

}

func getMrgItemRHS(doc *data.Doc, expr sql.Expr) interface{} {

	switch val := expr.(type) {
	default:
		return nil
	case time.Time:
		return val
	case bool, int64, float64, string:
		return val
	case []interface{}, map[string]interface{}:
		return val
	case *sql.Thing:
		return val
	case *sql.Ident:
		return doc.Get(val.ID).Data()
	}

}
