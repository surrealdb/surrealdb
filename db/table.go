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

	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/keys"
)

// Table checks if any table views are specified for
// this table, and executes them in name order.
func (d *document) table(ctx context.Context, when method) (err error) {

	// If this document has not changed
	// then there is no need to update
	// any registered foreign tables.

	if !d.changed() {
		return nil
	}

	// Get the foreign read-only tables
	// specified for this table, and
	// update values which have changed.

	fts, err := d.getFT()
	if err != nil {
		return err
	}

	if len(fts) > 0 {

		for _, ft := range fts {

			var ok bool
			var doc *sql.Thing

			ok, err = d.check(ctx, ft.Cond)
			if err != nil {
				return err
			}

			if len(ft.Group) > 0 {

				// If there are GROUP BY clauses then
				// let's calculate the

				return errFeatureNotImplemented

				ats := make([]interface{}, len(ft.Group))

				for k, e := range ft.Group {
					ats[k], _ = d.i.e.fetch(ctx, e.Expr, d.current)
				}

				rec := fmt.Sprintf("%v", ats)

				doc = sql.NewThing(ft.Name.ID, rec)

			} else {

				// Otherwise let's use the id of the
				// current record as the basis of the
				// new record in the other table.

				doc = sql.NewThing(ft.Name.ID, d.id.ID)

			}

			switch ok {

			// If the document does not match the table
			// WHERE condition, then remove it from
			// the table, or remove it from the aggregate.

			case false:

				if len(ft.Group) > 0 {
					err = d.tableModify(ctx, doc, nil)
					if err != nil {
						return err
					}
				} else {
					err = d.tableDelete(ctx, doc, nil)
					if err != nil {
						return err
					}
				}

			// If the document does match the table
			// WHERE condition, then add it to the
			// table, or add it to the aggregate.

			case true:

				if len(ft.Group) > 0 {
					err = d.tableModify(ctx, doc, nil)
					if err != nil {
						return err
					}
				} else {
					err = d.tableUpdate(ctx, doc, ft.Expr)
					if err != nil {
						return err
					}
				}

			}

		}

	}

	return

}

func (d *document) tableDelete(ctx context.Context, id *sql.Thing, exp sql.Fields) (err error) {

	stm := &sql.DeleteStatement{
		KV:       d.key.KV,
		NS:       d.key.NS,
		DB:       d.key.DB,
		What:     sql.Exprs{id},
		Hard:     true,
		Parallel: 1,
	}

	key := &keys.Thing{KV: stm.KV, NS: stm.NS, DB: stm.DB, TB: id.TB, ID: id.ID}

	i := newIterator(d.i.e, ctx, stm, true)

	i.processThing(ctx, key)

	_, err = i.Yield(ctx)

	return err

}

func (d *document) tableUpdate(ctx context.Context, id *sql.Thing, exp sql.Fields) (err error) {

	res, err := d.yield(ctx, &sql.SelectStatement{Expr: exp}, sql.ILLEGAL)
	if err != nil {
		return err
	}

	stm := &sql.UpdateStatement{
		KV:       d.key.KV,
		NS:       d.key.NS,
		DB:       d.key.DB,
		What:     sql.Exprs{id},
		Data:     &sql.ContentExpression{Data: res},
		Parallel: 1,
	}

	key := &keys.Thing{KV: stm.KV, NS: stm.NS, DB: stm.DB, TB: id.TB, ID: id.ID}

	i := newIterator(d.i.e, ctx, stm, true)

	i.processThing(ctx, key)

	_, err = i.Yield(ctx)

	return err

}

func (d *document) tableModify(ctx context.Context, id *sql.Thing, exp sql.Fields) error {

	return nil

}
