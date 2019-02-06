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

	"context"

	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
	"github.com/abcum/surreal/util/diff"
	"github.com/abcum/surreal/util/indx"
	"github.com/abcum/surreal/util/keys"
)

type document struct {
	i       *iterator
	id      *sql.Thing
	key     *keys.Thing
	val     kvs.KV
	lck     bool
	doc     *data.Doc
	initial *data.Doc
	current *data.Doc
	changed bool
}

func newDocument(i *iterator, key *keys.Thing, val kvs.KV, doc *data.Doc) (d *document) {

	d = documentPool.Get().(*document)

	d.i = i
	d.id = nil
	d.key = key
	d.val = val
	d.doc = doc
	d.lck = false

	return

}

func (d *document) close() {
	documentPool.Put(d)
}

func (d *document) erase() (err error) {
	d.changed, d.current = true, data.Consume(nil)
	return
}

func (d *document) query(ctx context.Context, stm sql.Statement) (val interface{}, err error) {

	defer func() {

		if r := recover(); r != nil {
			var ok bool
			if err, ok = r.(error); !ok {
				err = fmt.Errorf("%v", r)
			}
		}

		d.ulock(ctx)

		d.close()

	}()

	switch stm := stm.(type) {
	default:
		return nil, nil
	case *sql.SelectStatement:
		return d.runSelect(ctx, stm)
	case *sql.CreateStatement:
		return d.runCreate(ctx, stm)
	case *sql.UpdateStatement:
		return d.runUpdate(ctx, stm)
	case *sql.DeleteStatement:
		return d.runDelete(ctx, stm)
	case *sql.RelateStatement:
		return d.runRelate(ctx, stm)
	case *sql.InsertStatement:
		return d.runInsert(ctx, stm)
	case *sql.UpsertStatement:
		return d.runUpsert(ctx, stm)
	}

}

func (d *document) init(ctx context.Context) (err error) {

	// A table of records were requested
	// so we have the values, but no key
	// yet, so we need to decode the KV
	// store key into a Thing key.

	if d.key == nil && d.val != nil {
		d.key = &keys.Thing{}
		d.key.Decode(d.val.Key())
	}

	return

}

func (d *document) lock(ctx context.Context) (err error) {

	if d.key != nil {
		d.lck = true
		d.i.e.lock.Lock(ctx, d.key)
	}

	return

}

func (d *document) ulock(ctx context.Context) (err error) {

	if d.key != nil && d.lck {
		d.lck = false
		d.i.e.lock.Unlock(ctx, d.key)
	}

	return

}

func (d *document) setup(ctx context.Context) (err error) {

	// A specific record has been requested
	// and we have a key, but no value has
	// been loaded yet, so the record needs
	// to be loaded from the KV store.

	if d.key != nil && d.val == nil {
		d.val, err = d.i.e.dbo.Get(ctx, d.i.versn, d.key.Encode())
		if err != nil {
			return
		}
	}

	// A subquery or data param has been
	// loaded, and we might not have a key
	// or a value, so let's load the data
	// into a document, so that we can
	// maniuplate the virtual document.

	if d.doc != nil {
		enc := d.doc.Encode()
		d.initial = data.New().Decode(enc)
		d.current = data.New().Decode(enc)
	}

	// The requested record has been loaded
	// from the KV store (and not from a
	// subquery or data variable), but does
	// not exist. So we'll create a document
	// for processing any record changes.

	if d.doc == nil && d.val != nil && d.val.Exi() == false {
		d.initial = data.New()
		d.current = data.New()
	}

	// The requested record has been loaded
	// from the KV store (and not from a
	// subquery or data variable). So we'll
	// load the KV data into a document for
	// processing any record changes.

	if d.doc == nil && d.val != nil && d.val.Exi() == true {
		d.initial = data.New().Decode(d.val.Val())
		d.current = data.New().Decode(d.val.Val())
	}

	// Finally if we are dealing with a record
	// which is not data from the result of a
	// subquery, then generate the ID.

	if d.key != nil {
		d.id = sql.NewThing(d.key.TB, d.key.ID)
	}

	return

}

func (d *document) forced(ctx context.Context) bool {
	if val := ctx.Value(ctxKeyForce); val != nil {
		return val.(bool)
	}
	return false
}

func (d *document) hasChanged(ctx context.Context) bool {
	a, _ := d.initial.Data().(map[string]interface{})
	b, _ := d.current.Data().(map[string]interface{})
	c := diff.Diff(a, b)
	return len(c) > 0
}

func (d *document) shouldDrop(ctx context.Context) (bool, error) {

	// Check whether it is specified
	// that the table should drop
	// writes, and if so, then return.

	tb, err := d.i.e.dbo.GetTB(ctx, d.key.NS, d.key.DB, d.key.TB)
	if err != nil {
		return false, err
	}

	return tb.Drop, err

}

func (d *document) shouldVersn(ctx context.Context) (bool, error) {

	// Check whether it is specified
	// that the table should keep
	// all document versions.

	tb, err := d.i.e.dbo.GetTB(ctx, d.key.NS, d.key.DB, d.key.TB)
	if err != nil {
		return false, err
	}

	return tb.Vers, err

}

func (d *document) storeThing(ctx context.Context) (err error) {

	defer d.ulock(ctx)

	// Check that the record has been
	// changed, and if not, return.

	if !d.changed {
		return
	}

	// Check that the table should
	// drop data being written.

	if ok, err := d.shouldDrop(ctx); ok {
		return err
	}

	// Write the value to the data
	// layer and return any errors.

	if ok, err := d.shouldVersn(ctx); err != nil {
		return err
	} else if ok == true {
		_, err = d.i.e.dbo.Put(ctx, d.i.e.time, d.key.Encode(), d.current.Encode())
	} else if ok == false {
		_, err = d.i.e.dbo.Put(ctx, 0, d.key.Encode(), d.current.Encode())
	}

	return

}

func (d *document) purgeThing(ctx context.Context) (err error) {

	defer d.ulock(ctx)

	// Check that the table should
	// drop data being written.

	if ok, err := d.shouldDrop(ctx); ok {
		return err
	}

	// Reset the item by writing a
	// nil value to the storage.

	if ok, err := d.shouldVersn(ctx); err != nil {
		return err
	} else if ok == true {
		_, err = d.i.e.dbo.Put(ctx, d.i.e.time, d.key.Encode(), nil)
	} else if ok == false {
		_, err = d.i.e.dbo.Clr(ctx, d.key.Encode())
	}

	return

}

func (d *document) storeIndex(ctx context.Context) (err error) {

	// Check if this query has been run
	// in forced mode, or return.

	forced := d.forced(ctx)

	// Check that the rcord has been
	// changed, and if not, return.

	if !forced && !d.changed {
		return
	}

	// Check that the table should
	// drop data being written.

	if ok, err := d.shouldDrop(ctx); ok {
		return err
	}

	// Get the index values specified
	// for this table, loop through
	// them, and compute the changes.

	ixs, err := d.i.e.dbo.AllIX(ctx, d.key.NS, d.key.DB, d.key.TB)
	if err != nil {
		return err
	}

	for _, ix := range ixs {

		del := indx.Build(ix.Cols, d.initial)
		add := indx.Build(ix.Cols, d.current)

		if !forced {
			del, add = indx.Diff(del, add)
		}

		if ix.Uniq == true {
			for _, f := range del {
				enfd := data.Consume(f).Encode()
				didx := &keys.Index{KV: d.key.KV, NS: d.key.NS, DB: d.key.DB, TB: d.key.TB, IX: ix.Name.VA, FD: enfd}
				d.i.e.dbo.DelC(ctx, d.i.e.time, didx.Encode(), d.id.Bytes())
			}
			for _, f := range add {
				enfd := data.Consume(f).Encode()
				aidx := &keys.Index{KV: d.key.KV, NS: d.key.NS, DB: d.key.DB, TB: d.key.TB, IX: ix.Name.VA, FD: enfd}
				if _, err = d.i.e.dbo.PutC(ctx, 0, aidx.Encode(), d.id.Bytes(), nil); err != nil {
					return &IndexError{tb: d.key.TB, name: ix.Name, cols: ix.Cols, vals: f}
				}
			}
		}

		if ix.Uniq == false {
			for _, f := range del {
				enfd := data.Consume(f).Encode()
				didx := &keys.Point{KV: d.key.KV, NS: d.key.NS, DB: d.key.DB, TB: d.key.TB, IX: ix.Name.VA, FD: enfd, ID: d.key.ID}
				d.i.e.dbo.DelC(ctx, d.i.e.time, didx.Encode(), d.id.Bytes())
			}
			for _, f := range add {
				enfd := data.Consume(f).Encode()
				aidx := &keys.Point{KV: d.key.KV, NS: d.key.NS, DB: d.key.DB, TB: d.key.TB, IX: ix.Name.VA, FD: enfd, ID: d.key.ID}
				if _, err = d.i.e.dbo.PutC(ctx, 0, aidx.Encode(), d.id.Bytes(), nil); err != nil {
					return &IndexError{tb: d.key.TB, name: ix.Name, cols: ix.Cols, vals: f}
				}
			}
		}

	}

	return

}

func (d *document) purgeIndex(ctx context.Context) (err error) {

	// Check if this query has been run
	// in forced mode, or return.

	forced := d.forced(ctx)

	// Check that the rcord has been
	// changed, and if not, return.

	if !forced && !d.changed {
		return
	}

	// Check that the table should
	// drop data being written.

	if ok, err := d.shouldDrop(ctx); ok {
		return err
	}

	// Get the index values specified
	// for this table, loop through
	// them, and compute the changes.

	ixs, err := d.i.e.dbo.AllIX(ctx, d.key.NS, d.key.DB, d.key.TB)
	if err != nil {
		return err
	}

	for _, ix := range ixs {

		del := indx.Build(ix.Cols, d.initial)

		if ix.Uniq == true {
			for _, v := range del {
				key := &keys.Index{KV: d.key.KV, NS: d.key.NS, DB: d.key.DB, TB: d.key.TB, IX: ix.Name.VA, FD: v}
				d.i.e.dbo.DelC(ctx, 0, key.Encode(), d.id.Bytes())
			}
		}

		if ix.Uniq == false {
			for _, v := range del {
				key := &keys.Point{KV: d.key.KV, NS: d.key.NS, DB: d.key.DB, TB: d.key.TB, IX: ix.Name.VA, FD: v, ID: d.key.ID}
				d.i.e.dbo.DelC(ctx, 0, key.Encode(), d.id.Bytes())
			}
		}

	}

	return

}
