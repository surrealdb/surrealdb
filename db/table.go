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
	"github.com/abcum/surreal/util/data"
	"github.com/abcum/surreal/util/fncs"
	"github.com/abcum/surreal/util/keys"
)

// Table checks if any table views are specified for
// this table, and executes them in name order.
func (d *document) table(ctx context.Context, when method) (err error) {

	// Check if this query has been run
	// in forced mode, because of an
	// index or foreign table update.

	forced := d.forced(ctx)

	// If this document has not changed
	// then there is no need to update
	// any registered foreign tables.

	if !forced && !d.changed(ctx) {
		return nil
	}

	// Get the foreign read-only tables
	// specified for this table, and
	// update values which have changed.

	fts, err := d.getFT(ctx)
	if err != nil {
		return err
	}

	for _, ft := range fts {

		var ok bool
		var prv *sql.Thing
		var doc *sql.Thing

		ok, err = d.check(ctx, ft.Cond)
		if err != nil {
			return err
		}

		if len(ft.Group) > 0 {

			// If there are GROUP BY clauses then
			// let's calculate the

			old := make([]interface{}, len(ft.Group))
			now := make([]interface{}, len(ft.Group))

			for k, e := range ft.Group {
				old[k], _ = d.i.e.fetch(ctx, e.Expr, d.initial)
				now[k], _ = d.i.e.fetch(ctx, e.Expr, d.current)
			}

			prv = sql.NewThing(ft.Name.VA, fmt.Sprintf("%v", old))
			doc = sql.NewThing(ft.Name.VA, fmt.Sprintf("%v", now))

		} else {

			// Otherwise let's use the id of the
			// current record as the basis of the
			// new record in the other table.

			doc = sql.NewThing(ft.Name.VA, d.id.ID)

		}

		switch ok {

		// If the document does not match the table
		// WHERE condition, then remove it from
		// the table, or remove it from the aggregate.

		case false:

			if len(ft.Group) > 0 {

				if !forced && when != _CREATE {
					err = d.tableModify(ctx, prv, ft.Expr, _REMOVE)
					if err != nil {
						return err
					}
				}

			} else {

				err = d.tableDelete(ctx, doc, ft.Expr)
				if err != nil {
					return err
				}

			}

		// If the document does match the table
		// WHERE condition, then add it to the
		// table, or add it to the aggregate.

		case true:

			if len(ft.Group) > 0 {

				if !forced && when != _CREATE {
					err = d.tableModify(ctx, prv, ft.Expr, _REMOVE)
					if err != nil {
						return err
					}
				}

				if when != _DELETE {
					err = d.tableModify(ctx, doc, ft.Expr, _CHANGE)
					if err != nil {
						return err
					}
				}

			} else {

				err = d.tableUpdate(ctx, doc, ft.Expr)
				if err != nil {
					return err
				}

			}

		}

	}

	return

}

func (d *document) tableDelete(ctx context.Context, tng *sql.Thing, exp sql.Fields) (err error) {

	stm := &sql.DeleteStatement{
		KV:       d.key.KV,
		NS:       d.key.NS,
		DB:       d.key.DB,
		What:     sql.Exprs{tng},
		Hard:     false,
		Parallel: 1,
	}

	key := &keys.Thing{KV: stm.KV, NS: stm.NS, DB: stm.DB, TB: tng.TB, ID: tng.ID}

	i := newIterator(d.i.e, ctx, stm, true)

	i.processThing(ctx, key)

	_, err = i.Yield(ctx)

	return err

}

func (d *document) tableUpdate(ctx context.Context, tng *sql.Thing, exp sql.Fields) (err error) {

	res, err := d.yield(ctx, &sql.SelectStatement{Expr: exp}, sql.ILLEGAL)
	if err != nil {
		return err
	}

	stm := &sql.UpdateStatement{
		KV:       d.key.KV,
		NS:       d.key.NS,
		DB:       d.key.DB,
		What:     sql.Exprs{tng},
		Data:     &sql.ContentExpression{Data: res},
		Parallel: 1,
	}

	key := &keys.Thing{KV: stm.KV, NS: stm.NS, DB: stm.DB, TB: tng.TB, ID: tng.ID}

	i := newIterator(d.i.e, ctx, stm, true)

	i.processThing(ctx, key)

	_, err = i.Yield(ctx)

	return err

}

func (d *document) tableModify(ctx context.Context, tng *sql.Thing, exp sql.Fields, when modify) (err error) {

	var doc *data.Doc

	switch when {
	case _REMOVE:
		doc = d.initial
	case _CHANGE:
		doc = d.current
	}

	set := &sql.DataExpression{}

	for _, e := range exp {

		if f, ok := e.Expr.(*sql.FuncExpression); ok && f.Aggr {

			var v interface{}

			args := make([]interface{}, len(f.Args))
			for x := 0; x < len(f.Args); x++ {
				args[x], _ = d.i.e.fetch(ctx, f.Args[x], doc)
			}

			// If the function is math.stddev() or
			// math.variance(), then we need to work
			// out the value as a whole, and not the
			// result of each record separately.

			switch f.Name {
			default:
				v, err = fncs.Run(ctx, f.Name, args...)
			case "math.stddev":
				v = args[0]
			case "math.variance":
				v = args[0]
			}

			if err != nil {
				return err
			}

			switch f.Name {
			case "distinct":
				tableChg(set, e.Field, v, when)
			case "count":
				tableChg(set, e.Field, v, when)
			case "count.if":
				tableChg(set, e.Field, v, when)
			case "count.not":
				tableChg(set, e.Field, v, when)
			case "math.sum":
				tableChg(set, e.Field, v, when)
			case "math.min":
				tableMin(set, e.Field, v, when)
			case "math.max":
				tableMax(set, e.Field, v, when)
			case "math.mean":
				tableMean(set, e.Field, v, when)
			case "math.stddev":
				switch a := v.(type) {
				case []interface{}:
					for _, v := range a {
						tableStddev(set, e.Field, v, when)
					}
				default:
					tableStddev(set, e.Field, v, when)
				}
			case "math.variance":
				switch a := v.(type) {
				case []interface{}:
					for _, v := range a {
						tableVariance(set, e.Field, v, when)
					}
				default:
					tableVariance(set, e.Field, v, when)
				}
			}

			continue

		}

		o, err := d.i.e.fetch(ctx, e.Expr, doc)
		if err != nil {
			return err
		}

		tableSet(set, e.Field, o, when)

	}

	stm := &sql.UpdateStatement{
		KV:       d.key.KV,
		NS:       d.key.NS,
		DB:       d.key.DB,
		What:     sql.Exprs{tng},
		Data:     set,
		Parallel: 1,
	}

	key := &keys.Thing{KV: stm.KV, NS: stm.NS, DB: stm.DB, TB: tng.TB, ID: tng.ID}

	i := newIterator(d.i.e, ctx, stm, true)

	i.processThing(ctx, key)

	_, err = i.Yield(ctx)

	return err

}

func tableSet(set *sql.DataExpression, key string, val interface{}, when modify) {

	set.Data = append(set.Data, &sql.ItemExpression{
		LHS: sql.NewIdent(key),
		Op:  sql.EQ,
		RHS: val,
	})

}

func tableChg(set *sql.DataExpression, key string, val interface{}, when modify) {

	var op sql.Token

	switch when {
	case _REMOVE:
		op = sql.DEC
	case _CHANGE:
		op = sql.INC
	}

	set.Data = append(set.Data, &sql.ItemExpression{
		LHS: sql.NewIdent(key),
		Op:  op,
		RHS: val,
	})

}

func tableMin(set *sql.DataExpression, key string, val interface{}, when modify) {

	if when == _CHANGE {

		set.Data = append(set.Data, &sql.ItemExpression{
			LHS: sql.NewIdent(key),
			Op:  sql.EQ,
			RHS: &sql.IfelExpression{
				Cond: sql.Exprs{
					&sql.BinaryExpression{
						LHS: &sql.BinaryExpression{
							LHS: sql.NewIdent(key),
							Op:  sql.EQ,
							RHS: &sql.Empty{},
						},
						Op: sql.OR,
						RHS: &sql.BinaryExpression{
							LHS: sql.NewIdent(key),
							Op:  sql.GT,
							RHS: val,
						},
					},
				},
				Then: sql.Exprs{
					val,
				},
				Else: sql.NewIdent(key),
			},
		})

	}

}

func tableMax(set *sql.DataExpression, key string, val interface{}, when modify) {

	if when == _CHANGE {

		set.Data = append(set.Data, &sql.ItemExpression{
			LHS: sql.NewIdent(key),
			Op:  sql.EQ,
			RHS: &sql.IfelExpression{
				Cond: sql.Exprs{
					&sql.BinaryExpression{
						LHS: &sql.BinaryExpression{
							LHS: sql.NewIdent(key),
							Op:  sql.EQ,
							RHS: &sql.Empty{},
						},
						Op: sql.OR,
						RHS: &sql.BinaryExpression{
							LHS: sql.NewIdent(key),
							Op:  sql.LT,
							RHS: val,
						},
					},
				},
				Then: sql.Exprs{
					val,
				},
				Else: sql.NewIdent(key),
			},
		})

	}

}

func tableMean(set *sql.DataExpression, key string, val interface{}, when modify) {

	var op sql.Token

	switch when {
	case _REMOVE:
		op = sql.DEC
	case _CHANGE:
		op = sql.INC
	}

	set.Data = append(set.Data, &sql.ItemExpression{
		LHS: sql.NewIdent("meta.__." + key + ".c"),
		Op:  op,
		RHS: 1,
	})

	switch when {
	case _REMOVE:
		op = sql.SUB
	case _CHANGE:
		op = sql.ADD
	}

	set.Data = append(set.Data, &sql.ItemExpression{
		LHS: sql.NewIdent(key),
		Op:  sql.EQ,
		RHS: &sql.BinaryExpression{
			LHS: &sql.SubExpression{
				Expr: &sql.BinaryExpression{
					LHS: &sql.BinaryExpression{
						LHS: sql.NewIdent(key),
						Op:  sql.MUL,
						RHS: &sql.BinaryExpression{
							LHS: sql.NewIdent("meta.__." + key + ".c"),
							Op:  sql.SUB,
							RHS: 1,
						},
					},
					Op:  op,
					RHS: val,
				},
			},
			Op:  sql.DIV,
			RHS: sql.NewIdent("meta.__." + key + ".c"),
		},
	})

	set.Data = append(set.Data, &sql.ItemExpression{
		LHS: sql.NewIdent(key),
		Op:  sql.EQ,
		RHS: &sql.IfelExpression{
			Cond: sql.Exprs{
				&sql.BinaryExpression{
					LHS: sql.NewIdent(key),
					Op:  sql.EQ,
					RHS: &sql.Empty{},
				},
			},
			Then: sql.Exprs{0},
			Else: sql.NewIdent(key),
		},
	})

}

func tableStddev(set *sql.DataExpression, key string, val interface{}, when modify) {

	var op sql.Token

	switch when {
	case _REMOVE:
		op = sql.DEC
	case _CHANGE:
		op = sql.INC
	}

	set.Data = append(set.Data, &sql.ItemExpression{
		LHS: sql.NewIdent("meta.__." + key + ".c"),
		Op:  op,
		RHS: 1,
	})

	set.Data = append(set.Data, &sql.ItemExpression{
		LHS: sql.NewIdent("meta.__." + key + ".t"),
		Op:  op,
		RHS: val,
	})

	set.Data = append(set.Data, &sql.ItemExpression{
		LHS: sql.NewIdent("meta.__." + key + ".m"),
		Op:  op,
		RHS: &sql.BinaryExpression{
			LHS: val,
			Op:  sql.MUL,
			RHS: val,
		},
	})

	// FIXME Need to ensure removed values update correctly

	switch when {
	case _REMOVE:
		op = sql.SUB // FIXME This is incorrect
	case _CHANGE:
		op = sql.ADD
	}

	set.Data = append(set.Data, &sql.ItemExpression{
		LHS: sql.NewIdent(key),
		Op:  sql.EQ,
		RHS: &sql.FuncExpression{
			Name: "math.sqrt",
			Args: sql.Exprs{
				&sql.BinaryExpression{
					LHS: &sql.BinaryExpression{
						LHS: &sql.BinaryExpression{
							LHS: sql.NewIdent("meta.__." + key + ".c"),
							Op:  sql.MUL,
							RHS: sql.NewIdent("meta.__." + key + ".m"),
						},
						Op: sql.SUB,
						RHS: &sql.BinaryExpression{
							LHS: sql.NewIdent("meta.__." + key + ".t"),
							Op:  sql.MUL,
							RHS: sql.NewIdent("meta.__." + key + ".t"),
						},
					},
					Op: sql.DIV,
					RHS: &sql.BinaryExpression{
						LHS: sql.NewIdent("meta.__." + key + ".c"),
						Op:  sql.MUL,
						RHS: &sql.BinaryExpression{
							LHS: sql.NewIdent("meta.__." + key + ".c"),
							Op:  sql.SUB,
							RHS: 1,
						},
					},
				},
			},
		},
	})

	set.Data = append(set.Data, &sql.ItemExpression{
		LHS: sql.NewIdent(key),
		Op:  sql.EQ,
		RHS: &sql.IfelExpression{
			Cond: sql.Exprs{
				&sql.BinaryExpression{
					LHS: sql.NewIdent(key),
					Op:  sql.EQ,
					RHS: &sql.Empty{},
				},
			},
			Then: sql.Exprs{0},
			Else: sql.NewIdent(key),
		},
	})

}

func tableVariance(set *sql.DataExpression, key string, val interface{}, when modify) {

	var op sql.Token

	switch when {
	case _REMOVE:
		op = sql.DEC
	case _CHANGE:
		op = sql.INC
	}

	set.Data = append(set.Data, &sql.ItemExpression{
		LHS: sql.NewIdent("meta.__." + key + ".c"),
		Op:  op,
		RHS: 1,
	})

	set.Data = append(set.Data, &sql.ItemExpression{
		LHS: sql.NewIdent("meta.__." + key + ".t"),
		Op:  op,
		RHS: val,
	})

	set.Data = append(set.Data, &sql.ItemExpression{
		LHS: sql.NewIdent("meta.__." + key + ".m"),
		Op:  op,
		RHS: &sql.BinaryExpression{
			LHS: val,
			Op:  sql.MUL,
			RHS: val,
		},
	})

	// FIXME Need to ensure removed values update correctly

	switch when {
	case _REMOVE:
		op = sql.SUB // FIXME This is incorrect
	case _CHANGE:
		op = sql.ADD
	}

	set.Data = append(set.Data, &sql.ItemExpression{
		LHS: sql.NewIdent(key),
		Op:  sql.EQ,
		RHS: &sql.BinaryExpression{
			LHS: &sql.BinaryExpression{
				LHS: &sql.BinaryExpression{
					LHS: &sql.BinaryExpression{
						LHS: sql.NewIdent("meta.__." + key + ".c"),
						Op:  sql.MUL,
						RHS: sql.NewIdent("meta.__." + key + ".m"),
					},
					Op: sql.SUB,
					RHS: &sql.BinaryExpression{
						LHS: sql.NewIdent("meta.__." + key + ".t"),
						Op:  sql.MUL,
						RHS: sql.NewIdent("meta.__." + key + ".t"),
					},
				},
				Op: sql.DIV,
				RHS: &sql.BinaryExpression{
					LHS: sql.NewIdent("meta.__." + key + ".c"),
					Op:  sql.SUB,
					RHS: 1,
				},
			},
			Op:  sql.DIV,
			RHS: sql.NewIdent("meta.__." + key + ".c"),
		},
	})

	set.Data = append(set.Data, &sql.ItemExpression{
		LHS: sql.NewIdent(key),
		Op:  sql.EQ,
		RHS: &sql.IfelExpression{
			Cond: sql.Exprs{
				&sql.BinaryExpression{
					LHS: sql.NewIdent(key),
					Op:  sql.EQ,
					RHS: &sql.Empty{},
				},
			},
			Then: sql.Exprs{0},
			Else: sql.NewIdent(key),
		},
	})

}
