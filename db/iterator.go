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
	"math"
	"sort"
	"sync"

	"context"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/comp"
	"github.com/abcum/surreal/util/data"
	"github.com/abcum/surreal/util/fncs"
	"github.com/abcum/surreal/util/guid"
	"github.com/abcum/surreal/util/ints"
	"github.com/abcum/surreal/util/keys"
	"github.com/abcum/surreal/util/nums"
	"github.com/abcum/surreal/util/rand"
)

type iterator struct {
	e *executor

	id int

	err error
	vir bool
	stm sql.Statement
	res []interface{}

	wait sync.WaitGroup
	fail chan error
	stop chan struct{}
	jobs chan *workable
	vals chan *doneable

	expr  sql.Fields
	what  sql.Exprs
	cond  sql.Expr
	group sql.Groups
	order sql.Orders
	limit int
	start int
	versn int64
	tasks int
}

type workable struct {
	key *keys.Thing
	val kvs.KV
	doc *data.Doc
}

type doneable struct {
	res interface{}
	err error
}

type groupable struct {
	doc *data.Doc
	ats []interface{}
}

type orderable struct {
	doc *data.Doc
	ats []interface{}
}

func newIterator(e *executor, ctx context.Context, stm sql.Statement, vir bool) (i *iterator) {

	i = iteratorPool.Get().(*iterator)

	i.e = e

	i.id = rand.Int()

	i.err = nil
	i.stm = stm
	i.vir = vir
	i.res = make([]interface{}, 0)

	i.wait = sync.WaitGroup{}
	i.fail = make(chan error, 1)
	i.stop = make(chan struct{})
	i.jobs = make(chan *workable, 1000)
	i.vals = make(chan *doneable, 1000)

	// Comment here

	i.setupState(ctx)

	// Comment here ...

	i.setupWorkers(ctx)

	// Comment here ...

	i.watchVals(ctx)

	return

}

func (i *iterator) Close() {

	i.e = nil
	i.err = nil
	i.stm = nil
	i.res = nil

	i.fail = nil
	i.stop = nil
	i.jobs = nil
	i.vals = nil

	i.expr = nil
	i.what = nil
	i.cond = nil
	i.group = nil
	i.order = nil
	i.limit = -1
	i.start = -1
	i.versn = 0
	i.tasks = 0

	iteratorPool.Put(i)

}

func (i *iterator) setupState(ctx context.Context) {

	i.expr = nil
	i.what = nil
	i.cond = nil
	i.group = nil
	i.order = nil
	i.limit = -1
	i.start = -1
	i.versn = math.MaxInt64

	switch stm := i.stm.(type) {
	case *sql.SelectStatement:
		i.expr = stm.Expr
		i.what = stm.What
		i.cond = stm.Cond
		i.group = stm.Group
		i.order = stm.Order
		i.tasks = stm.Parallel
	case *sql.CreateStatement:
		i.what = stm.What
		i.tasks = stm.Parallel
	case *sql.UpdateStatement:
		i.what = stm.What
		i.cond = stm.Cond
		i.tasks = stm.Parallel
	case *sql.DeleteStatement:
		i.what = stm.What
		i.cond = stm.Cond
		i.tasks = stm.Parallel
	case *sql.RelateStatement:
		i.tasks = stm.Parallel
	case *sql.InsertStatement:
		i.what = sql.Exprs{stm.Data}
		i.tasks = stm.Parallel
	case *sql.UpsertStatement:
		i.what = sql.Exprs{stm.Data}
		i.tasks = stm.Parallel
	}

	if stm, ok := i.stm.(*sql.SelectStatement); ok {

		// Fetch and check the LIMIT BY expression
		// to see if any parameter specified is valid.

		i.limit, i.err = i.e.fetchLimit(ctx, stm.Limit)
		if i.err != nil {
			close(i.stop)
			return
		}

		// Fetch and check the START AT expression
		// to see if any parameter specified is valid.

		i.start, i.err = i.e.fetchStart(ctx, stm.Start)
		if i.err != nil {
			close(i.stop)
			return
		}

		// Fetch and check the VERSION expression to
		// see if any parameter specified is valid.

		i.versn, i.err = i.e.fetchVersion(ctx, stm.Version)
		if i.err != nil {
			close(i.stop)
			return
		}

	}

}

func (i *iterator) checkState(ctx context.Context) bool {

	select {
	case <-ctx.Done():
		return false
	case <-i.stop:
		return false
	default:
		return true
	}

}

func (i *iterator) setupWorkers(ctx context.Context) {

	count := ints.Between(1, maxWorkers, workerCount)

	if i.checkState(ctx) {
		switch {
		case i.tasks == 0:
			for w := 1; w <= count; w++ {
				go i.setupWorker(ctx, i.jobs, i.vals)
			}
		default:
			for w := 1; w <= ints.Between(1, count, i.tasks); w++ {
				go i.setupWorker(ctx, i.jobs, i.vals)
			}
		}
	}

}

func (i *iterator) setupWorker(ctx context.Context, jobs chan *workable, vals chan *doneable) {

	for j := range jobs {

		res, err := newDocument(i, j.key, j.val, j.doc).query(ctx, i.stm)

		vals <- &doneable{res: res, err: err}

	}

}

func (i *iterator) submitTask(key *keys.Thing, val kvs.KV, doc *data.Doc) {

	i.wait.Add(1)

	i.jobs <- &workable{key: key, val: val, doc: doc}

}

func (i *iterator) watchVals(ctx context.Context) {

	go func(vals <-chan *doneable) {
		for val := range vals {
			i.receive(val)
		}
	}(i.vals)

}

func (i *iterator) receive(val *doneable) {

	defer i.wait.Done()

	// If an error was received from the
	// worker, then set the error if no
	// previous iterator error has occured.

	if val.err != nil {
		select {
		case <-i.stop:
			return
		default:
			i.fail <- val.err
			close(i.stop)
			return
		}
	}

	// Otherwise add the received result
	// to the iterator result slice so
	// that it is ready for processing.

	if val.res != nil {
		i.res = append(i.res, val.res)
	}

	// The statement does not have a limit
	// expression specified, so therefore
	// we need to load all data before
	// stopping the iterator.

	if i.limit < 0 {
		return
	}

	// If the statement specified a GROUP
	// BY expression, then we need to load
	// all data from all sources before
	// stopping the iterator.

	if len(i.group) > 0 {
		return
	}

	// If the statement specified an ORDER
	// BY expression, then we need to load
	// all data from all sources before
	// stopping the iterator.

	if len(i.order) > 0 {
		return
	}

	// Otherwise we can stop the iterator
	// early, if we have the necessary
	// number of records specified in the
	// query statement.

	select {
	case <-i.stop:
		return
	default:
		if i.start >= 0 {
			if len(i.res) == i.limit+i.start {
				close(i.stop)
			}
		} else {
			if len(i.res) == i.limit {
				close(i.stop)
			}
		}
	}

}

func (i *iterator) processPerms(ctx context.Context, nsv, dbv, tbv string) {

	var tb *sql.DefineTableStatement

	// If we are authenticated using DB, NS,
	// or KV permissions level, then we can
	// ignore all permissions checks, but we
	// must ensure the TB, DB, and NS exist.

	if ctx.Value(ctxKeyKind).(cnf.Kind) < cnf.AuthSC {

		// If we do not have a specified table
		// value, because we are processing a
		// subquery, then there is no need to
		// check if the table exists or not.

		if len(tbv) == 0 {
			return
		}

		// If this is a select statement then
		// there is no need to fetch the table
		// to check whether it is a view table.

		switch i.stm.(type) {
		case *sql.SelectStatement:
			return
		}

		// If it is not a select statement, then
		// we need to fetch the table to ensure
		// that the table is not a view table.

		tb, i.err = i.e.dbo.AddTB(ctx, nsv, dbv, tbv)
		if i.err != nil {
			close(i.stop)
			return
		}

		// If the table is locked (because it
		// has been specified as a view), then
		// check to see what query type it is
		// and return an error if it attempts
		// to alter the table in any way.

		if tb.Lock && i.vir == false {
			switch i.stm.(type) {
			case *sql.CreateStatement:
				i.err = &TableError{table: tb.Name.ID}
			case *sql.UpdateStatement:
				i.err = &TableError{table: tb.Name.ID}
			case *sql.DeleteStatement:
				i.err = &TableError{table: tb.Name.ID}
			case *sql.RelateStatement:
				i.err = &TableError{table: tb.Name.ID}
			case *sql.InsertStatement:
				i.err = &TableError{table: tb.Name.ID}
			case *sql.UpsertStatement:
				i.err = &TableError{table: tb.Name.ID}
			}
		}

		if i.err != nil {
			close(i.stop)
		}

		return

	}

	// First check that the NS exists, as
	// otherwise, the scoped authentication
	// request can not do anything.

	_, i.err = i.e.dbo.GetNS(ctx, nsv)
	if i.err != nil {
		close(i.stop)
		return
	}

	// Next check that the DB exists, as
	// otherwise, the scoped authentication
	// request can not do anything.

	_, i.err = i.e.dbo.GetDB(ctx, nsv, dbv)
	if i.err != nil {
		close(i.stop)
		return
	}

	// If we do not have a specified table
	// value, because we are processing a
	// subquery, then there is no need to
	// check if the table exists or not.

	if len(tbv) == 0 {
		return
	}

	// Then check that the TB exists, as
	// otherwise, the scoped authentication
	// request can not do anything.

	tb, i.err = i.e.dbo.GetTB(ctx, nsv, dbv, tbv)
	if i.err != nil {
		close(i.stop)
		return
	}

	// If the table is locked (because it
	// has been specified as a view), then
	// check to see what query type it is
	// and return an error, if it attempts
	// to alter the table in any way.

	if tb.Lock && i.vir == false {
		switch i.stm.(type) {
		case *sql.CreateStatement:
			i.err = &TableError{table: tb.Name.ID}
		case *sql.UpdateStatement:
			i.err = &TableError{table: tb.Name.ID}
		case *sql.DeleteStatement:
			i.err = &TableError{table: tb.Name.ID}
		case *sql.RelateStatement:
			i.err = &TableError{table: tb.Name.ID}
		case *sql.InsertStatement:
			i.err = &TableError{table: tb.Name.ID}
		case *sql.UpsertStatement:
			i.err = &TableError{table: tb.Name.ID}
		}
	}

	if i.err != nil {
		close(i.stop)
		return
	}

	// If the table does exist we reset the
	// context to DB level so that no other
	// embedded permissions are checked on
	// records within these permissions.

	ctx = context.WithValue(ctx, ctxKeyKind, cnf.AuthDB)

	// If the table does exist we then try
	// to process the relevant permissions
	// expression, but only if they don't
	// reference any document fields.

	switch p := tb.Perms.(type) {
	default:
		i.err = &PermsError{table: tb.Name.ID}
	case *sql.PermExpression:
		switch i.stm.(type) {
		case *sql.SelectStatement:
			i.err = i.e.fetchPerms(ctx, p.Select, tb.Name)
		case *sql.CreateStatement:
			i.err = i.e.fetchPerms(ctx, p.Create, tb.Name)
		case *sql.UpdateStatement:
			i.err = i.e.fetchPerms(ctx, p.Update, tb.Name)
		case *sql.DeleteStatement:
			i.err = i.e.fetchPerms(ctx, p.Delete, tb.Name)
		case *sql.RelateStatement:
			i.err = i.e.fetchPerms(ctx, p.Create, tb.Name)
		case *sql.InsertStatement:
			i.err = i.e.fetchPerms(ctx, p.Create, tb.Name)
		case *sql.UpsertStatement:
			i.err = i.e.fetchPerms(ctx, p.Update, tb.Name)
		}
	}

	if i.err != nil {
		close(i.stop)
		return
	}

	return

}

func (i *iterator) processThing(ctx context.Context, key *keys.Thing) {

	i.processPerms(ctx, key.NS, key.DB, key.TB)

	if i.checkState(ctx) {
		i.submitTask(key, nil, nil)
	}

}

func (i *iterator) processTable(ctx context.Context, key *keys.Table) {

	i.processPerms(ctx, key.NS, key.DB, key.TB)

	// TODO use indexes to speed up queries
	// We need to make use of indexes here
	// so that the query speed is improved.
	// If an index exists with the correct
	// ORDER BY fields then iterate over
	// the IDs from the index.

	beg := &keys.Thing{KV: key.KV, NS: key.NS, DB: key.DB, TB: key.TB, ID: keys.Ignore}
	end := &keys.Thing{KV: key.KV, NS: key.NS, DB: key.DB, TB: key.TB, ID: keys.Suffix}

	min, max := beg.Encode(), end.Encode()

	for x := 0; ; x = 1 {

		var vals []kvs.KV

		if !i.checkState(ctx) {
			return
		}

		vals, i.err = i.e.dbo.GetR(ctx, i.versn, min, max, 10000)
		if i.err != nil {
			close(i.stop)
			return
		}

		// If there are no further records
		// fetched from the data layer, then
		// return out of this loop iteration.

		if x >= len(vals) {
			return
		}

		// If there were at least 1 or 2
		// keys-values, then loop over all
		// the items and process the records.

		for _, val := range vals {
			if i.checkState(ctx) {
				i.submitTask(nil, val, nil)
				continue
			}
		}

		// When we loop around, we will use
		// the key of the last retrieved key
		// to perform the next range request.

		beg.Decode(vals[len(vals)-1].Key())

		min = append(beg.Encode(), byte(0))

	}

}

func (i *iterator) processBatch(ctx context.Context, key *keys.Thing, qry *sql.Batch) {

	i.processPerms(ctx, key.NS, key.DB, key.TB)

	for _, val := range qry.BA {

		// Loop over the items in the batch
		// and specify the TB and ID for
		// each record.

		if i.checkState(ctx) {
			key := key.Copy()
			key.TB, key.ID = val.TB, val.ID
			i.submitTask(key, nil, nil)
			continue
		}

		break

	}

}

func (i *iterator) processModel(ctx context.Context, key *keys.Thing, qry *sql.Model) {

	i.processPerms(ctx, key.NS, key.DB, key.TB)

	switch {

	case qry.INC == 0:

		// If there was no incrementing pattern
		// specified for the model, then let's
		// generate unique ids for each record.

		for j := 1; j <= int(qry.MAX); j++ {

			if i.checkState(ctx) {
				key := key.Copy()
				key.ID = guid.New().String()
				i.submitTask(key, nil, nil)
				continue
			}

			break

		}

	case qry.MIN < qry.MAX:

		// If an incrementing pattern has been
		// specified, then ascend through the
		// steps sequentially.

		dec := nums.CountPlaces(qry.INC)

		for num := qry.MIN; num <= qry.MAX; num = nums.FormatPlaces(num+qry.INC, dec) {

			if i.checkState(ctx) {
				key := key.Copy()
				key.ID = num
				i.submitTask(key, nil, nil)
				continue
			}

			break

		}

	case qry.MIN > qry.MAX:

		// If an decrementing pattern has been
		// specified, then descend through the
		// steps sequentially.

		dec := nums.CountPlaces(qry.INC)

		for num := qry.MIN; num >= qry.MAX; num = nums.FormatPlaces(num-qry.INC, dec) {

			if i.checkState(ctx) {
				key := key.Copy()
				key.ID = num
				i.submitTask(key, nil, nil)
				continue
			}

			break

		}

	}

}

func (i *iterator) processQuery(ctx context.Context, key *keys.Thing, val []interface{}) {

	i.processPerms(ctx, key.NS, key.DB, key.TB)

	for _, val := range val {

		switch val := val.(type) {

		case *sql.Thing:

			// If the item is a *sql.Thing then
			// this was a subquery which projected
			// the ID only, and we can query the
			// record further after loading it.

			if i.checkState(ctx) {
				key := key.Copy()
				key.TB, key.ID = val.TB, val.ID
				i.submitTask(key, nil, nil)
				continue
			}

		default:

			// Otherwise let's just load up all
			// of the data so we can process it.

			if i.checkState(ctx) {
				i.submitTask(nil, nil, data.Consume(val))
				continue
			}

		}

		break

	}

}

func (i *iterator) processArray(ctx context.Context, key *keys.Thing, val []interface{}) {

	i.processPerms(ctx, key.NS, key.DB, key.TB)

	for _, val := range val {

		switch val := val.(type) {

		case *sql.Thing:

			// If the item is a *sql.Thing then
			// this was a subquery, so use the ID.

			if i.checkState(ctx) {
				key := key.Copy()
				key.ID = val.ID
				i.submitTask(key, nil, nil)
				continue
			}

		case map[string]interface{}:

			// If the data item has an ID field,
			// then use this as the new record ID.

			if fld, ok := val["id"]; ok {

				if thg, ok := val["id"].(*sql.Thing); ok {

					// If the ID is a *sql.Thing then this
					// was a subquery, so use the ID.

					if i.checkState(ctx) {
						key := key.Copy()
						key.ID = thg.ID
						i.submitTask(key, nil, data.Consume(val))
						continue
					}

				} else {

					// If not, then take the whole ID and
					// use that as the ID of the new record.

					if i.checkState(ctx) {
						key := key.Copy()
						key.ID = fld
						i.submitTask(key, nil, data.Consume(val))
						continue
					}

				}

			} else {

				// If there is no ID field, then create
				// a unique id for the new record.

				if i.checkState(ctx) {
					key := key.Copy()
					key.ID = guid.New().String()
					i.submitTask(key, nil, data.Consume(val))
					continue
				}

			}

		}

		break

	}

}

func (i *iterator) Yield(ctx context.Context) (out []interface{}, err error) {

	defer i.Close()

	i.wait.Wait()

	close(i.jobs)
	close(i.vals)

	if i.err != nil {
		return nil, i.err
	}

	if i.err == nil {
		select {
		default:
		case i.err = <-i.fail:
			return nil, i.err
		}
	}

	if len(i.group) > 0 {
		i.res = i.Group(ctx, i.res)
	}

	if len(i.order) > 0 {
		i.res = i.Order(ctx, i.res)
	}

	if i.start >= 0 {
		num := ints.Min(i.start, len(i.res))
		i.res = i.res[num:]
	}

	if i.limit >= 0 {
		num := ints.Min(i.limit, len(i.res))
		i.res = i.res[:num]
	}

	return i.res, i.err

}

func (i *iterator) Group(ctx context.Context, arr []interface{}) (out []interface{}) {

	var grp []*groupable
	var col = make(map[string][]interface{})

	// Loop through all of the items
	// and create a *groupable to
	// store the record, and all of
	// the attributes in the GROUP BY.

	for _, a := range arr {

		g := &groupable{
			doc: data.Consume(a),
			ats: make([]interface{}, len(i.group)),
		}

		for k, e := range i.group {
			g.ats[k], _ = i.e.fetch(ctx, e.Expr, g.doc)
		}

		grp = append(grp, g)

	}

	// Group all of the items together
	// according to the GROUP by clause.
	// We use a string representation of
	// the group fields to group records.

	for _, s := range grp {
		k := fmt.Sprintf("%v", s.ats)
		col[k] = append(col[k], s.doc.Data())
	}

	for _, obj := range col {

		doc, all := data.New(), data.Consume(obj)

		for _, e := range i.expr {

			// If the clause has a GROUP BY expression
			// then let's check if this is an aggregate
			// function, and if it is then calculate
			// the output with the aggregated data.

			if f, ok := e.Expr.(*sql.FuncExpression); ok && f.Aggr {
				args := make([]interface{}, len(f.Args))
				for x := 0; x < len(f.Args); x++ {
					if x == 0 {
						args[x] = all.Get("*", f.String()).Data()
					} else {
						args[x], _ = i.e.fetch(ctx, f.Args[x], nil)
					}
				}
				val, _ := fncs.Run(ctx, f.Name, args...)
				doc.Set(val, e.Field)
				continue
			}

			// Otherwise if not, then it is a field
			// which is also specified in the GROUP BY
			// clause, so let's include the first
			// value in the aggregated results.

			val := all.Get("0", e.Field).Data()
			doc.Set(val, e.Field)

		}

		out = append(out, doc.Data())

	}

	return

}

func (i *iterator) Order(ctx context.Context, arr []interface{}) (out []interface{}) {

	var ord []*orderable

	// Loop through all of the items
	// and create an *orderable to
	// store the record, and all of
	// the attributes in the ORDER BY.

	for _, a := range arr {
		ord = append(ord, &orderable{
			doc: data.Consume(a),
			ats: make([]interface{}, 0),
		})
	}

	// Sort the *sortable items whilst
	// fetching any values which were
	// previously not loaded. Cache
	// the values on the *orderable.

	sort.Slice(ord, func(k, j int) bool {
		for x, e := range i.order {
			if len(ord[k].ats) <= x {
				a, _ := i.e.fetch(ctx, e.Expr, ord[k].doc)
				ord[k].ats = append(ord[k].ats, a)
			}
			if len(ord[j].ats) <= x {
				a, _ := i.e.fetch(ctx, e.Expr, ord[j].doc)
				ord[j].ats = append(ord[j].ats, a)
			}
			if c := comp.Comp(ord[k].ats[x], ord[j].ats[x], e); c != 0 {
				return (c < 0 && e.Dir) || (c > 0 && !e.Dir)
			}
		}
		return false
	})

	// Loop over the sorted items and
	// add the document data for each
	// item to the output array.

	for _, s := range ord {
		out = append(out, s.doc.Data())
	}

	return

}
