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
	"context"

	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/conv"
	"github.com/abcum/surreal/util/data"
	"github.com/abcum/surreal/util/diff"
)

var main = map[string]struct{}{
	"id":      {},
	"meta":    {},
	"meta.tb": {},
	"meta.id": {},
}

func (d *document) merge(ctx context.Context, data sql.Expr) (err error) {

	if err = d.defFld(ctx); err != nil {
		return
	}

	switch expr := data.(type) {
	case *sql.DataExpression:
		if err = d.mrgSet(ctx, expr); err != nil {
			return err
		}
	case *sql.DiffExpression:
		if err = d.mrgDpm(ctx, expr); err != nil {
			return err
		}
	case *sql.MergeExpression:
		if err = d.mrgAny(ctx, expr); err != nil {
			return err
		}
	case *sql.ContentExpression:
		if err = d.mrgAll(ctx, expr); err != nil {
			return err
		}
	}

	if err = d.defFld(ctx); err != nil {
		return
	}

	if err = d.mrgFld(ctx); err != nil {
		return
	}

	if err = d.defFld(ctx); err != nil {
		return
	}

	if err = d.delFld(ctx); err != nil {
		return
	}

	return

}

func (d *document) defFld(ctx context.Context) (err error) {

	d.current.Set(d.id, "id")
	d.current.Set(d.md, "meta")

	return

}

func (d *document) delFld(ctx context.Context) (err error) {

	tb, err := d.getTB()
	if err != nil {
		return err
	}

	if tb.Full {

		var keys = map[string]struct{}{}

		// Get the defined fields

		fds, err := d.getFD()
		if err != nil {
			return err
		}

		// Loop over the allowed keys

		for _, fd := range fds {
			d.current.Walk(func(key string, val interface{}) (err error) {
				keys[key] = struct{}{}
				return
			}, fd.Name.ID)
		}

		// Delete any keys which aren't allowed

		d.current.Each(func(key string, val interface{}) (err error) {
			if _, ok := main[key]; !ok {
				if _, ok := keys[key]; !ok {
					d.current.Del(key)
				}
			}
			return
		})

	}

	return

}

func (d *document) mrgAll(ctx context.Context, expr *sql.ContentExpression) (err error) {

	var obj map[string]interface{}

	switch v := expr.Data.(type) {
	case map[string]interface{}:
		obj = v
	case *sql.Param:

		p, err := d.i.e.fetch(ctx, v, d.current)
		if err != nil {
			return err
		}

		switch v := p.(type) {
		case map[string]interface{}:
			obj = v
		}

	}

	d.current.Reset()

	for k, v := range obj {
		d.current.Set(v, k)
	}

	return

}

func (d *document) mrgAny(ctx context.Context, expr *sql.MergeExpression) (err error) {

	var obj map[string]interface{}

	switch v := expr.Data.(type) {
	case map[string]interface{}:
		obj = v
	case *sql.Param:

		p, err := d.i.e.fetch(ctx, v, d.current)
		if err != nil {
			return err
		}

		switch v := p.(type) {
		case map[string]interface{}:
			obj = v
		}

	}

	for k, v := range obj {
		d.current.Set(v, k)
	}

	return

}

func (d *document) mrgDpm(ctx context.Context, expr *sql.DiffExpression) (err error) {

	var obj []interface{}
	var old map[string]interface{}
	var now map[string]interface{}

	switch v := expr.Data.(type) {
	case []interface{}:
		obj = v
	case *sql.Param:

		p, err := d.i.e.fetch(ctx, v, d.current)
		if err != nil {
			return err
		}

		switch v := p.(type) {
		case []interface{}:
			obj = v
		}

	}

	old = d.current.Data().(map[string]interface{})
	now = diff.Patch(old, obj)

	d.current = data.Consume(now)

	return

}

func (d *document) mrgSet(ctx context.Context, expr *sql.DataExpression) (err error) {

	for _, v := range expr.Data {

		if i, ok := v.LHS.(*sql.Ident); ok {

			n, err := d.i.e.fetch(ctx, v.RHS, d.current)
			if err != nil {
				return err
			}

			switch v.Op {
			case sql.EQ:
				switch n.(type) {
				default:
					d.current.Set(n, i.ID)
				case *sql.Void:
					d.current.Del(i.ID)
				}
			case sql.INC:
				d.current.Inc(n, i.ID)
			case sql.DEC:
				d.current.Dec(n, i.ID)
			}

		}

	}

	return

}

func (d *document) mrgFld(ctx context.Context) (err error) {

	fds, err := d.getFD()
	if err != nil {
		return err
	}

	// TODO need a way of arranging the fields
	// This is so that a field can depend on
	// another field if it uses the data from
	// the other field. Perhaps a DEPENDS ON
	// command can be used to specify other
	// fields that a field relies on.

	for _, fd := range fds {

		err = d.current.Walk(func(key string, val interface{}) (err error) {

			vars := data.New()

			var old = d.initial.Get(key).Data()

			// Ensure the field is the correct type

			if val != nil {
				if val, err = conv.ConvertTo(fd.Type, fd.Kind, val); err != nil {
					val = old
				}
			}

			// Reset the variables

			vars.Set(val, varKeyValue)
			vars.Set(val, varKeyAfter)
			vars.Set(old, varKeyBefore)
			ctx = context.WithValue(ctx, ctxKeySubs, vars)

			// We are setting the value of the field

			if fd.Value != nil {
				if val, err = d.i.e.fetch(ctx, fd.Value, d.current); err != nil {
					return err
				}
			}

			// Reset the variables

			vars.Set(val, varKeyValue)
			vars.Set(val, varKeyAfter)
			vars.Set(old, varKeyBefore)
			ctx = context.WithValue(ctx, ctxKeySubs, vars)

			// We are checking the value of the field

			if fd.Assert != nil {
				if chk, err := d.i.e.fetch(ctx, fd.Assert, d.current); err != nil {
					return err
				} else if chk, ok := chk.(bool); ok && !chk {
					return &FieldError{field: key, found: val, check: fd.Assert}
				}
			}

			switch val.(type) {
			default:
				d.current.Iff(val, key)
			case *sql.Void:
				d.current.Del(key)
			}

			return

		}, fd.Name.ID)

		if err != nil {
			return
		}

	}

	return

}
