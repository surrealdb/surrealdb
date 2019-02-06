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
	"sort"

	"context"

	"github.com/abcum/surreal/cnf"
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

func (d *document) merge(ctx context.Context, met method, data sql.Expr) (err error) {

	if err = d.defFld(ctx, met); err != nil {
		return
	}

	switch expr := data.(type) {
	case *sql.DataExpression:
		if err = d.mrgSet(ctx, met, expr); err != nil {
			return err
		}
	case *sql.DiffExpression:
		if err = d.mrgDpm(ctx, met, expr); err != nil {
			return err
		}
	case *sql.MergeExpression:
		if err = d.mrgAny(ctx, met, expr); err != nil {
			return err
		}
	case *sql.ContentExpression:
		if err = d.mrgAll(ctx, met, expr); err != nil {
			return err
		}
	}

	if err = d.defFld(ctx, met); err != nil {
		return
	}

	if err = d.mrgFld(ctx, met); err != nil {
		return
	}

	if err = d.defFld(ctx, met); err != nil {
		return
	}

	if err = d.delFld(ctx, met); err != nil {
		return
	}

	d.changed = d.hasChanged(ctx)

	return

}

func (d *document) defFld(ctx context.Context, met method) (err error) {

	switch d.i.vir {
	case true:
		d.current.Set(d.id, "id")
		d.current.Set(d.id.TB, "meta.tb")
		d.current.Set(d.id.ID, "meta.id")
	case false:
		d.current.Del("meta")
		d.current.Set(d.id, "id")
		d.current.Set(d.id.TB, "meta.tb")
		d.current.Set(d.id.ID, "meta.id")
	}

	return

}

func (d *document) delFld(ctx context.Context, met method) (err error) {

	tb, err := d.i.e.dbo.GetTB(ctx, d.key.NS, d.key.DB, d.key.TB)
	if err != nil {
		return err
	}

	if tb.Full {

		var keys = map[string]struct{}{}

		// Get the defined fields

		fds, err := d.i.e.dbo.AllFD(ctx, d.key.NS, d.key.DB, d.key.TB)
		if err != nil {
			return err
		}

		// Loop over the allowed keys

		for _, fd := range fds {
			d.current.Walk(func(key string, val interface{}, ok bool) (err error) {
				keys[key] = struct{}{}
				return
			}, fd.Name.VA)
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

func (d *document) mrgAll(ctx context.Context, met method, expr *sql.ContentExpression) (err error) {

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

func (d *document) mrgAny(ctx context.Context, met method, expr *sql.MergeExpression) (err error) {

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

func (d *document) mrgDpm(ctx context.Context, met method, expr *sql.DiffExpression) (err error) {

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

func (d *document) mrgSet(ctx context.Context, met method, expr *sql.DataExpression) (err error) {

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
					d.current.Set(n, i.VA)
				case *sql.Void:
					d.current.Del(i.VA)
				}
			case sql.INC:
				d.current.Inc(n, i.VA)
			case sql.DEC:
				d.current.Dec(n, i.VA)
			}

		}

	}

	return

}

func (d *document) mrgFld(ctx context.Context, met method) (err error) {

	fds, err := d.i.e.dbo.AllFD(ctx, d.key.NS, d.key.DB, d.key.TB)
	if err != nil {
		return err
	}

	// Sort the fields according to their
	// priority so that fields which depend
	// on another field can be processed
	// after that field in a specific order.

	sort.Slice(fds, func(i, j int) bool {
		return fds[i].Priority < fds[j].Priority
	})

	// Loop through each field and check to
	// see if it might be a specific type.
	// This is because when updating records
	// using json, there is no specific type
	// for a 'datetime' and 'record'.

	d.current.Each(func(key string, val interface{}) (err error) {
		if val, ok := conv.MightBe(val); ok {
			d.current.Iff(val, key)
		}
		return nil
	})

	// Loop over each of the defined fields
	// and process them fully, applying the
	// VALUE and ASSERT clauses sequentially.

	for _, fd := range fds {

		err = d.current.Walk(func(key string, val interface{}, exi bool) error {

			var old = d.initial.Get(key).Data()

			// Ensure object and arrays are set

			val = conv.MustBe(fd.Type, val)

			// Ensure the field is the correct type

			if val != nil {
				if now, err := conv.ConvertTo(fd.Type, fd.Kind, val); err != nil {
					val = nil
				} else {
					val = now
				}
			}

			// We are setting the value of the field

			if fd.Value != nil && d.i.e.opts.fields {

				// Reset the variables

				vars := data.New()
				vars.Set(val, varKeyValue)
				vars.Set(val, varKeyAfter)
				vars.Set(old, varKeyBefore)
				ctx = context.WithValue(ctx, ctxKeySpec, vars)

				if now, err := d.i.e.fetch(ctx, fd.Value, d.current); err != nil {
					return err
				} else {
					val = now
				}

			}

			// We are checking the value of the field

			if fd.Assert != nil && d.i.e.opts.fields {

				// Reset the variables

				vars := data.New()
				vars.Set(val, varKeyValue)
				vars.Set(val, varKeyAfter)
				vars.Set(old, varKeyBefore)
				ctx = context.WithValue(ctx, ctxKeySpec, vars)

				if chk, err := d.i.e.fetch(ctx, fd.Assert, d.current); err != nil {
					return err
				} else if chk, ok := chk.(bool); ok && !chk {
					return &FieldError{field: key, found: val, check: fd.Assert}
				}

			}

			// We are checking the permissions of the field

			if fd.Perms != nil && perm(ctx) > cnf.AuthDB {

				// Reset the variables

				vars := data.New()
				vars.Set(val, varKeyValue)
				vars.Set(val, varKeyAfter)
				vars.Set(old, varKeyBefore)
				ctx = context.WithValue(ctx, ctxKeySpec, vars)

				switch p := fd.Perms.(type) {
				case *sql.PermExpression:
					switch met {
					case _CREATE:
						if v, err := d.i.e.fetch(ctx, p.Create, d.current); err != nil {
							return err
						} else {
							if b, ok := v.(bool); !ok || !b {
								val = old
							}
						}
					case _UPDATE:
						if v, err := d.i.e.fetch(ctx, p.Update, d.current); err != nil {
							return err
						} else {
							if b, ok := v.(bool); !ok || !b {
								val = old
							}
						}
					case _DELETE:
						if v, err := d.i.e.fetch(ctx, p.Delete, d.current); err != nil {
							return err
						} else {
							if b, ok := v.(bool); !ok || !b {
								val = old
							}
						}
					}
				}

			}

			// We are setting the value of the field

			switch val.(type) {
			default:
				if exi {
					d.current.Set(val, key)
				} else {
					d.current.Iff(val, key)
				}
			case *sql.Void:
				d.current.Del(key)
			}

			return nil

		}, fd.Name.VA)

		if err != nil {
			return
		}

	}

	return

}
