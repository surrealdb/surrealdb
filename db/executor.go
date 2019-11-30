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
	"sync"
	"time"

	"context"

	"runtime/debug"

	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/log"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/txn"
)

type executor struct {
	id   string
	ns   string
	db   string
	tx   *txn.TX
	err  error
	buf  []*Response
	time time.Time
	lock *mutex
	opts *options
	data sync.Map
	send chan *Response
}

func newExecutor(id, ns, db string) (e *executor) {

	e = executorPool.Get().(*executor)

	e.id = id
	e.ns = ns
	e.db = db

	e.err = nil
	e.buf = nil

	e.data = sync.Map{}

	e.opts = newOptions()

	e.send = make(chan *Response)

	return

}

func (e *executor) execute(ctx context.Context, ast *sql.Query) {

	// Ensure that the executor is added back into
	// the executor pool when the executor has
	// finished processing the request.

	defer executorPool.Put(e)

	// Ensure that the query responses channel is
	// closed when the full query has been processed
	// and dealt with.

	defer close(e.send)

	// If we are making use of a global transaction
	// which is not committed at the end of the
	// query set, then cancel the transaction.

	defer func() {
		if e.tx != nil {
			e.tx.Cancel()
			clear(e.id)
		}
	}()

	// If we have panicked during query execution
	// then ensure that we recover from the error
	// and print the error to the log.

	defer func() {
		if err := recover(); err != nil {
			if log.IsError() {
				log.WithPrefix(logKeyDB).WithFields(map[string]interface{}{
					logKeyId: e.id, logKeyStack: string(debug.Stack()),
				}).Errorln(err)
			}
		}
	}()

	// Loop over the defined query statements and
	// process them, while listening for the quit
	// channel to see if the client has gone away.

	for _, stm := range ast.Statements {
		select {
		case <-ctx.Done():
			return
		default:
			e.conduct(ctx, stm)
		}
	}

}

func (e *executor) conduct(ctx context.Context, stm sql.Statement) {

	var rsp *Response
	var res []interface{}

	// If we are not inside a global transaction
	// then reset the error to nil so that the
	// next statement is not ignored.

	if e.tx == nil {
		e.err = nil
	}

	// Check to see if the current statement is
	// a TRANSACTION statement, and if it is
	// then deal with it and move on to the next.

	switch stm.(type) {
	case *sql.BeginStatement:
		e.err = e.begin(ctx, true)
		if e.err != nil {
			clear(e.id)
		}
		return
	case *sql.CancelStatement:
		e.err = e.cancel(e.send)
		if e.err != nil {
			clear(e.id)
		} else {
			clear(e.id)
		}
		return
	case *sql.CommitStatement:
		e.err = e.commit(e.send)
		if e.err != nil {
			clear(e.id)
		} else {
			flush(e.id)
		}
		return
	}

	// If an error has occured and we are inside
	// a global transaction, then ignore all
	// subsequent statements in the transaction.

	if e.err == nil {
		res, e.err = e.operate(ctx, stm)
	} else {
		res, e.err = []interface{}{}, errQueryNotExecuted
	}

	// Generate the response

	rsp = &Response{
		Time:   time.Since(e.time).String(),
		Status: status(e.err),
		Detail: detail(e.err),
		Result: append([]interface{}{}, res...),
	}

	// Log the sql statement along with the
	// query duration time, and mark it as
	// an error if the query failed.

	switch e.err.(type) {
	default:
		if log.IsDebug() {
			log.WithPrefix(logKeySql).WithFields(map[string]interface{}{
				logKeyId:   e.id,
				logKeyNS:   e.ns,
				logKeyDB:   e.db,
				logKeyKind: ctx.Value(ctxKeyKind),
				logKeyVars: ctx.Value(ctxKeyVars),
				logKeyTime: time.Since(e.time).String(),
			}).Debugln(stm)
		}
	case error:
		if log.IsError() {
			log.WithPrefix(logKeySql).WithFields(map[string]interface{}{
				logKeyId:    e.id,
				logKeyNS:    e.ns,
				logKeyDB:    e.db,
				logKeyKind:  ctx.Value(ctxKeyKind),
				logKeyVars:  ctx.Value(ctxKeyVars),
				logKeyTime:  time.Since(e.time).String(),
				logKeyError: detail(e.err),
				logKeyStack: string(debug.Stack()),
			}).Errorln(stm)
		}
	}

	// If we are not inside a global transaction
	// then we can output the statement response
	// immediately to the channel.

	if e.tx == nil {
		e.send <- rsp
	}

	// If we are inside a global transaction we
	// must buffer the responses for output at
	// the end of the transaction.

	if e.tx != nil {
		switch stm.(type) {
		case *sql.ReturnStatement:
			for i := len(e.buf) - 1; i >= 0; i-- {
				e.buf[len(e.buf)-1] = nil
				e.buf = e.buf[:len(e.buf)-1]
			}
			e.buf = append(e.buf, rsp)
		default:
			e.buf = append(e.buf, rsp)
		}
	}

}

func (e *executor) operate(ctx context.Context, stm sql.Statement) (res []interface{}, err error) {

	var loc bool
	var trw bool
	var canc context.CancelFunc

	// If we are not inside a global transaction
	// then grab a new transaction, ensuring that
	// it is closed at the end.

	if e.tx == nil {

		switch stm := stm.(type) {
		case sql.WriteableStatement:
			loc, trw = true, stm.Writeable()
		default:
			loc, trw = true, false
		}

		err = e.begin(ctx, trw)
		if err != nil {
			return
		}

		defer func() {
			e.tx.Cancel()
			e.tx = nil
		}()

	}

	// Mark the beginning of this statement so we
	// can monitor the running time, and ensure
	// it runs no longer than specified.

	if stm, ok := stm.(sql.KillableStatement); ok {
		if stm.Duration() > 0 {
			ctx, canc = context.WithTimeout(ctx, stm.Duration())
			defer func() {
				if tim := ctx.Err(); err == nil && tim != nil {
					res, err = nil, &TimerError{timer: stm.Duration()}
				}
				canc()
			}()
		}
	}

	// Execute the defined statement, receiving the
	// result set, and any errors which occured
	// while processing the query.

	switch stm := stm.(type) {

	case *sql.OptStatement:
		res, err = e.executeOpt(ctx, stm)

	case *sql.UseStatement:
		res, err = e.executeUse(ctx, stm)

	case *sql.RunStatement:
		res, err = e.executeRun(ctx, stm)

	case *sql.InfoStatement:
		res, err = e.executeInfo(ctx, stm)

	case *sql.LetStatement:
		res, err = e.executeLet(ctx, stm)
	case *sql.ReturnStatement:
		res, err = e.executeReturn(ctx, stm)

	case *sql.LiveStatement:
		res, err = e.executeLive(ctx, stm)
	case *sql.KillStatement:
		res, err = e.executeKill(ctx, stm)

	case *sql.IfelseStatement:
		res, err = e.executeIfelse(ctx, stm)
	case *sql.SelectStatement:
		res, err = e.executeSelect(ctx, stm)
	case *sql.CreateStatement:
		res, err = e.executeCreate(ctx, stm)
	case *sql.UpdateStatement:
		res, err = e.executeUpdate(ctx, stm)
	case *sql.DeleteStatement:
		res, err = e.executeDelete(ctx, stm)
	case *sql.RelateStatement:
		res, err = e.executeRelate(ctx, stm)

	case *sql.InsertStatement:
		res, err = e.executeInsert(ctx, stm)
	case *sql.UpsertStatement:
		res, err = e.executeUpsert(ctx, stm)

	case *sql.DefineNamespaceStatement:
		res, err = e.executeDefineNamespace(ctx, stm)
	case *sql.RemoveNamespaceStatement:
		res, err = e.executeRemoveNamespace(ctx, stm)

	case *sql.DefineDatabaseStatement:
		res, err = e.executeDefineDatabase(ctx, stm)
	case *sql.RemoveDatabaseStatement:
		res, err = e.executeRemoveDatabase(ctx, stm)

	case *sql.DefineLoginStatement:
		res, err = e.executeDefineLogin(ctx, stm)
	case *sql.RemoveLoginStatement:
		res, err = e.executeRemoveLogin(ctx, stm)

	case *sql.DefineTokenStatement:
		res, err = e.executeDefineToken(ctx, stm)
	case *sql.RemoveTokenStatement:
		res, err = e.executeRemoveToken(ctx, stm)

	case *sql.DefineScopeStatement:
		res, err = e.executeDefineScope(ctx, stm)
	case *sql.RemoveScopeStatement:
		res, err = e.executeRemoveScope(ctx, stm)

	case *sql.DefineTableStatement:
		res, err = e.executeDefineTable(ctx, stm)
	case *sql.RemoveTableStatement:
		res, err = e.executeRemoveTable(ctx, stm)

	case *sql.DefineEventStatement:
		res, err = e.executeDefineEvent(ctx, stm)
	case *sql.RemoveEventStatement:
		res, err = e.executeRemoveEvent(ctx, stm)

	case *sql.DefineFieldStatement:
		res, err = e.executeDefineField(ctx, stm)
	case *sql.RemoveFieldStatement:
		res, err = e.executeRemoveField(ctx, stm)

	case *sql.DefineIndexStatement:
		res, err = e.executeDefineIndex(ctx, stm)
	case *sql.RemoveIndexStatement:
		res, err = e.executeRemoveIndex(ctx, stm)

	}

	// If the context is already closed or failed,
	// then ignore this result, clear all queued
	// changes, and reset the transaction.

	select {

	case <-ctx.Done():

		e.tx.Cancel()
		clear(e.id)

	default:

		// If this is a local transaction for only the
		// current statement, then commit or cancel
		// depending on the result error.

		if loc && e.tx.Closed() == false {

			// If there was an error with the query
			// then clear the queued changes and
			// return immediately.

			if err != nil {
				e.tx.Cancel()
				clear(e.id)
				return
			}

			// Otherwise check if this is a read or
			// a write transaction, and attempt to
			// Cancel or Commit, returning any errors.

			if !trw {
				if err = e.tx.Cancel(); err != nil {
					clear(e.id)
				} else {
					clear(e.id)
				}
			} else {
				if err = e.tx.Commit(); err != nil {
					clear(e.id)
				} else {
					flush(e.id)
				}
			}

		}

	}

	return

}

func (e *executor) begin(ctx context.Context, rw bool) (err error) {
	e.tx, err = txn.New(ctx, rw)
	e.time = time.Now()
	e.lock = new(mutex)
	return
}

func (e *executor) cancel(chn chan<- *Response) (err error) {

	defer func() {
		e.tx.Cancel()
		e.tx = nil
		e.buf = nil
		e.err = nil
	}()

	for _, v := range e.buf {
		v.Time = time.Since(e.time).String()
		v.Status = "ERR"
		v.Result = []interface{}{}
		v.Detail = "Transaction cancelled"
		chn <- v
	}

	return

}

func (e *executor) commit(chn chan<- *Response) (err error) {

	defer func() {
		e.tx.Cancel()
		e.tx = nil
		e.buf = nil
		e.err = nil
	}()

	if e.err != nil {
		err = e.tx.Cancel()
	} else {
		err = e.tx.Commit()
	}

	for _, v := range e.buf {
		if err != nil {
			v.Time = time.Since(e.time).String()
			v.Status = "ERR"
			v.Result = []interface{}{}
			v.Detail = "Transaction failed: " + err.Error()
		}
		chn <- v
	}

	return

}

func status(e error) (s string) {
	switch e.(type) {
	default:
		return "OK"
	case *kvs.DBError:
		return "ERR_DB"
	case *PermsError:
		return "ERR_PE"
	case *ExistError:
		return "ERR_EX"
	case *FieldError:
		return "ERR_FD"
	case *IndexError:
		return "ERR_IX"
	case *TimerError:
		return "ERR_TO"
	case error:
		return "ERR"
	}
}

func detail(e error) (s string) {
	switch err := e.(type) {
	default:
		return
	case error:
		return err.Error()
	}
}
