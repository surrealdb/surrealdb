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
	"io"
	"time"

	"net/http"

	"runtime/debug"

	"github.com/abcum/fibre"
	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/log"
	"github.com/abcum/surreal/mem"
	"github.com/abcum/surreal/sql"

	_ "github.com/abcum/surreal/kvs/rixxdb"
	// _ "github.com/abcum/surreal/kvs/dendro"
)

var QueryNotExecuted = fmt.Errorf("Query not executed")

type Response struct {
	Time   string        `codec:"time,omitempty"`
	Status string        `codec:"status,omitempty"`
	Detail string        `codec:"detail,omitempty"`
	Result []interface{} `codec:"result,omitempty"`
}

var db *kvs.DS

// Setup sets up the connection with the data layer
func Setup(opts *cnf.Options) (err error) {

	log.WithPrefix("db").Infof("Starting database")

	db, err = kvs.New(opts)

	return

}

// Exit shuts down the connection with the data layer
func Exit() {

	log.WithPrefix("db").Infof("Gracefully shutting down database")

	db.Close()

}

// Import loads database operations from a reader.
// This can be used to playback a database snapshot
// into an already running database.
func Import(r io.Reader) (err error) {
	return db.Import(r)
}

// Export saves all database operations to a writer.
// This can be used to save a database snapshot
// to a secondary file or stream.
func Export(w io.Writer) (err error) {
	return db.Export(w)
}

// Begin begins a new read / write transaction
// with the underlying database, and returns
// the transaction, or any error which occured.
func Begin(rw bool) (txn kvs.TX, err error) {
	return db.Begin(rw)
}

// Execute parses a single sql query, or multiple
// sql queries, and executes them serially against
// the underlying data layer.
func Execute(ctx *fibre.Context, txt interface{}, vars map[string]interface{}) (out []*Response, err error) {

	// If no preset variables have been defined
	// then ensure that the variables is
	// instantiated for future use.

	if vars == nil {
		vars = make(map[string]interface{})
	}

	// Parse the received SQL batch query strings
	// into SQL ASTs, using any immutable preset
	// variables if set.

	ast, err := sql.Parse(ctx, txt, vars)
	if err != nil {
		return
	}

	// Ensure that the current authentication data
	// is made available as a runtime variable to
	// the query layer.

	vars["auth"] = ctx.Get("auth").(*cnf.Auth).Data

	return Process(ctx, ast, vars)

}

// Process takes a parsed set of sql queries and
// executes them serially against the underlying
// data layer.
func Process(ctx *fibre.Context, ast *sql.Query, vars map[string]interface{}) (out []*Response, err error) {

	// Create 2 channels, one for force quitting
	// the query processor, and the other for
	// receiving and buffering any query results.

	quit := make(chan bool, 1)
	recv := make(chan *Response)

	// Ensure that the force quit channel is auto
	// closed when the end of the request has been
	// reached, and we are not an http connection.

	defer close(quit)

	// If the current connection is a normal http
	// connection then force quit any running
	// queries if a socket close event occurs.

	if _, ok := ctx.Response().ResponseWriter.(http.CloseNotifier); ok {

		exit := ctx.Response().CloseNotify()
		done := make(chan bool)
		defer close(done)

		go func() {
			select {
			case <-done:
			case <-exit:
				quit <- true
			}
		}()

	}

	// Create a new query executor with the query
	// details, and the current runtime variables
	// and execute the queries within.

	exec := pool.Get().(*executor)

	defer pool.Put(exec)

	exec.Reset(ast, ctx, vars)

	go exec.execute(quit, recv)

	// Wait for all of the processed queries to
	// return results, buffer the output, and
	// return the output when finished.

	for res := range recv {
		out = append(out, res)
	}

	return

}

func (e *executor) execute(quit <-chan bool, send chan<- *Response) {

	var err error
	var now time.Time
	var rsp *Response
	var buf []*Response
	var res []interface{}

	// Ensure that the query responses channel is
	// closed when the full query has been processed
	// and dealt with.

	defer close(send)

	// If we are making use of a global transaction
	// which is not committed at the end of the
	// query set, then cancel the transaction.

	defer func() {
		if e.txn != nil {
			e.txn.Cancel()
		}
	}()

	// If we have panicked during query execution
	// then ensure that we recover from the error
	// and print the error to the log.

	defer func() {
		if r := recover(); r != nil {
			switch err := r.(type) {
			case string:
				log.WithPrefix("db").Errorln(err)
				if log.Instance().Level >= log.DebugLevel {
					log.WithPrefix("db").Debugf("%s", debug.Stack())
				}
			case error:
				log.WithPrefix("db").Errorln(err)
				if log.Instance().Level >= log.DebugLevel {
					log.WithPrefix("db").WithError(err).Debugf("%s", debug.Stack())
				}
			}
		}
	}()

	stms := make(chan sql.Statement)

	// Loop over the defined query statements and
	// pass them to the statement processing
	// channel for execution.

	go func() {
		for _, stm := range e.ast.Statements {
			stms <- stm
		}
		close(stms)
	}()

	// Listen for any new statements to process and
	// at the same time listen for the quit signal
	// notifying us whether the client has gone away.

	for {

		select {

		case <-quit:
			return

		case stm, open := <-stms:

			// If we have reached the end of the statement
			// processing channel then return out of this
			// for loop and exit.

			if !open {
				return
			}

			// If we are not inside a global transaction
			// then reset the error to nil so that the
			// next statement is not ignored.

			if e.txn == nil {
				err, now = nil, time.Now()
			}

			// When in debugging mode, log every sql
			// query, along with the query execution
			// speed, so we can analyse slow queries.

			log.WithPrefix("sql").Debugln(stm)

			// Check to see if the current statement is
			// a TRANSACTION statement, and if it is
			// then deal with it and move on to the next.

			switch stm.(type) {
			case *sql.BeginStatement:
				err = e.begin(true)
				continue
			case *sql.CancelStatement:
				err, buf = e.cancel(buf, err, send)
				continue
			case *sql.CommitStatement:
				err, buf = e.commit(buf, err, send)
				continue
			}

			// If an error has occured and we are inside
			// a global transaction, then ignore all
			// subsequent statements in the transaction.

			if err == nil {
				res, err = e.operate(stm)
			} else {
				res, err = []interface{}{}, QueryNotExecuted
			}

			rsp = &Response{
				Time:   time.Since(now).String(),
				Status: status(err),
				Detail: detail(err),
				Result: append([]interface{}{}, res...),
			}

			// If we are not inside a global transaction
			// then we can output the statement response
			// immediately to the channel.

			if e.txn == nil {
				send <- rsp
				continue
			}

			// If we are inside a global transaction we
			// must buffer the responses for output at
			// the end of the transaction.

			if e.txn != nil {
				switch stm.(type) {
				case *sql.ReturnStatement:
					buf = clear(buf, rsp)
				default:
					buf = append(buf, rsp)
				}
				continue
			}

		}

	}

}

func (e *executor) operate(ast sql.Statement) (res []interface{}, err error) {

	var loc bool
	var trw bool

	// If we are not inside a global transaction
	// then grab a new transaction, ensuring that
	// it is closed at the end.

	if e.txn == nil {

		loc = true

		switch ast.(type) {
		case *sql.InfoStatement:
			trw = false
			err = e.begin(trw)
		default:
			trw = true
			err = e.begin(trw)
		}

		if err != nil {
			return
		}

		defer e.txn.Cancel()

	}

	// Execute the defined statement, receiving the
	// result set, and any errors which occured
	// while processing the query.

	switch stm := ast.(type) {

	case *sql.InfoStatement:
		res, err = e.executeInfoStatement(stm)

	case *sql.LetStatement:
		res, err = e.executeLetStatement(stm)
	case *sql.ReturnStatement:
		res, err = e.executeReturnStatement(stm)

	case *sql.SelectStatement:
		res, err = e.executeSelectStatement(stm)
	case *sql.CreateStatement:
		res, err = e.executeCreateStatement(stm)
	case *sql.UpdateStatement:
		res, err = e.executeUpdateStatement(stm)
	case *sql.DeleteStatement:
		res, err = e.executeDeleteStatement(stm)
	case *sql.RelateStatement:
		res, err = e.executeRelateStatement(stm)

	case *sql.DefineNamespaceStatement:
		res, err = e.executeDefineNamespaceStatement(stm)
	case *sql.RemoveNamespaceStatement:
		res, err = e.executeRemoveNamespaceStatement(stm)

	case *sql.DefineDatabaseStatement:
		res, err = e.executeDefineDatabaseStatement(stm)
	case *sql.RemoveDatabaseStatement:
		res, err = e.executeRemoveDatabaseStatement(stm)

	case *sql.DefineLoginStatement:
		res, err = e.executeDefineLoginStatement(stm)
	case *sql.RemoveLoginStatement:
		res, err = e.executeRemoveLoginStatement(stm)

	case *sql.DefineTokenStatement:
		res, err = e.executeDefineTokenStatement(stm)
	case *sql.RemoveTokenStatement:
		res, err = e.executeRemoveTokenStatement(stm)

	case *sql.DefineScopeStatement:
		res, err = e.executeDefineScopeStatement(stm)
	case *sql.RemoveScopeStatement:
		res, err = e.executeRemoveScopeStatement(stm)

	case *sql.DefineTableStatement:
		res, err = e.executeDefineTableStatement(stm)
	case *sql.RemoveTableStatement:
		res, err = e.executeRemoveTableStatement(stm)

	case *sql.DefineFieldStatement:
		res, err = e.executeDefineFieldStatement(stm)
	case *sql.RemoveFieldStatement:
		res, err = e.executeRemoveFieldStatement(stm)

	case *sql.DefineIndexStatement:
		res, err = e.executeDefineIndexStatement(stm)
	case *sql.RemoveIndexStatement:
		res, err = e.executeRemoveIndexStatement(stm)

	}

	// If this is a local transaction for only the
	// current statement, then commit or cancel
	// depending on the result error.

	if loc && !e.txn.Closed() {
		if !trw || err != nil {
			e.txn.Cancel()
			e.txn = nil
		} else {
			e.txn.Commit()
			e.txn = nil
		}
	}

	return

}

func status(e error) (s string) {
	switch e.(type) {
	default:
		return "OK"
	case *kvs.DBError:
		return "ERR_DB"
	case *kvs.KVError:
		return "ERR_KV"
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

func clear(buf []*Response, rsp *Response) []*Response {
	for i := len(buf) - 1; i >= 0; i-- {
		buf[len(buf)-1] = nil
		buf = buf[:len(buf)-1]
	}
	return append(buf, rsp)
}

func (e *executor) begin(rw bool) (err error) {
	if e.txn == nil {
		e.txn, err = db.Begin(rw)
		e.mem = mem.New(e.txn)
	}
	return
}

func (e *executor) cancel(buf []*Response, err error, chn chan<- *Response) (error, []*Response) {

	defer func() {
		e.txn = nil
		e.mem = nil
	}()

	if e.txn == nil {
		return nil, buf
	}

	e.txn.Cancel()

	for _, v := range buf {
		v.Status = "ERR"
		chn <- v
	}

	for i := len(buf) - 1; i >= 0; i-- {
		buf[len(buf)-1] = nil
		buf = buf[:len(buf)-1]
	}

	return nil, buf

}

func (e *executor) commit(buf []*Response, err error, chn chan<- *Response) (error, []*Response) {

	defer func() {
		e.txn = nil
		e.mem = nil
	}()

	if e.txn == nil {
		return nil, buf
	}

	if err != nil {
		e.txn.Cancel()
	} else {
		e.txn.Commit()
	}

	for _, v := range buf {
		if err != nil {
			v.Status = "ERR"
		}
		chn <- v
	}

	for i := len(buf) - 1; i >= 0; i-- {
		buf[len(buf)-1] = nil
		buf = buf[:len(buf)-1]
	}

	return nil, buf

}
