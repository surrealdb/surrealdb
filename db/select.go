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
	"github.com/abcum/surreal/util/data"
	"github.com/abcum/surreal/util/keys"
)

func (e *executor) executeSelect(ctx context.Context, stm *sql.SelectStatement) ([]interface{}, error) {

	ctx = context.WithValue(ctx, ctxKeyVersion, stm.Version)

	var what sql.Exprs

	for _, val := range stm.What {
		w, err := e.fetch(ctx, val, nil)
		if err != nil {
			return nil, err
		}
		what = append(what, w)
	}

	i := newIterator(e, ctx, stm, false)

	for _, w := range what {

		switch what := w.(type) {

		default:
			key := &keys.Thing{KV: stm.KV, NS: stm.NS, DB: stm.DB}
			i.processQuery(ctx, key, []interface{}{what})

		case *sql.Table:
			key := &keys.Table{KV: stm.KV, NS: stm.NS, DB: stm.DB, TB: what.TB}
			i.processTable(ctx, key)

		case *sql.Ident:
			key := &keys.Table{KV: stm.KV, NS: stm.NS, DB: stm.DB, TB: what.ID}
			i.processTable(ctx, key)

		case *sql.Thing:
			key := &keys.Thing{KV: stm.KV, NS: stm.NS, DB: stm.DB, TB: what.TB, ID: what.ID}
			i.processThing(ctx, key)

		case *sql.Model:
			key := &keys.Thing{KV: stm.KV, NS: stm.NS, DB: stm.DB, TB: what.TB, ID: nil}
			i.processModel(ctx, key, what)

		case *sql.Batch:
			key := &keys.Thing{KV: stm.KV, NS: stm.NS, DB: stm.DB, TB: what.TB, ID: nil}
			i.processBatch(ctx, key, what)

		// Result of subquery
		case []interface{}:
			key := &keys.Thing{KV: stm.KV, NS: stm.NS, DB: stm.DB}
			i.processQuery(ctx, key, what)

		// Result of subquery with LIMIT 1
		case map[string]interface{}:
			key := &keys.Thing{KV: stm.KV, NS: stm.NS, DB: stm.DB}
			i.processQuery(ctx, key, []interface{}{what})

		}

	}

	return i.Yield(ctx)

}

func (e *executor) fetchSelect(ctx context.Context, stm *sql.SelectStatement, doc *data.Doc) (interface{}, error) {

	ctx = dive(ctx)

	if doc != nil {
		vars := data.New()
		vars.Set(doc.Data(), varKeyParent)
		ctx = context.WithValue(ctx, ctxKeySubs, vars)
	}

	out, err := e.executeSelect(ctx, stm)
	if err != nil {
		return nil, err
	}

	cnt, err := e.fetchOutputs(ctx, stm)
	if err != nil {
		return nil, err
	}

	switch cnt {
	case 1:
		switch len(stm.Expr) {
		case 1:
			f := stm.Expr[0]
			switch f.Expr.(type) {
			default:
				return data.Consume(out).Get(docKeyOne, f.Field).Data(), nil
			case *sql.All:
				return data.Consume(out).Get(docKeyOne).Data(), nil
			}
		default:
			return data.Consume(out).Get(docKeyOne).Data(), nil
		}
	default:
		switch len(stm.Expr) {
		case 1:
			f := stm.Expr[0]
			switch f.Expr.(type) {
			default:
				return data.Consume(out).Get(docKeyAll, f.Field).Data(), nil
			case *sql.All:
				return data.Consume(out).Get(docKeyAll).Data(), nil
			}
		default:
			return data.Consume(out).Get(docKeyAll).Data(), nil
		}
	}

	return out, err

}

func (d *document) runSelect(ctx context.Context, stm *sql.SelectStatement) (interface{}, error) {

	var ok bool
	var err error
	var met = _SELECT

	if err = d.init(ctx); err != nil {
		return nil, err
	}

	if err = d.rlock(ctx); err != nil {
		return nil, err
	}

	if err = d.setup(ctx); err != nil {
		return nil, err
	}

	if d.doc == nil && !d.val.Exi() {
		return nil, nil
	}

	if ok, err = d.allow(ctx, met); err != nil {
		return nil, err
	} else if ok == false {
		return nil, nil
	}

	if ok, err = d.check(ctx, stm.Cond); err != nil {
		return nil, err
	} else if ok == false {
		return nil, nil
	}

	return d.yield(ctx, stm, sql.ILLEGAL)

}
