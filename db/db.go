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
	"time"

	"net/http"

	"github.com/abcum/fibre"
	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/log"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"

	_ "github.com/abcum/surreal/kvs/boltdb"
	_ "github.com/abcum/surreal/kvs/mysql"
	_ "github.com/abcum/surreal/kvs/pgsql"
)

type executor struct {
	txn kvs.TX
	ctx *data.Doc
	ast *sql.Query
}

func newExecutor(ast *sql.Query, vars map[string]interface{}) *executor {
	return &executor{ast: ast, ctx: data.Consume(vars)}
}

func (e *executor) Txn() kvs.TX {
	return e.txn
}

func (e *executor) Set(key string, val interface{}) {
	e.ctx.Set(val, key)
}

func (e *executor) Get(key string) (val interface{}) {
	return e.ctx.Get(key).Data()
}

type Response struct {
	Time   string        `codec:"time,omitempty"`
	Status string        `codec:"status,omitempty"`
	Detail string        `codec:"detail,omitempty"`
	Result []interface{} `codec:"result,omitempty"`
}

var db *kvs.DB

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

// Execute parses the query and executes it against the data layer
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

	return Process(ctx, ast, vars)

}

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

	go newExecutor(ast, vars).execute(quit, recv)

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
	var txn kvs.TX
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
		if txn != nil {
			txn.Cancel()
		}
	}()

	// If we have paniced during query execution
	// then ensure that we recover from the error
	// and print the error to the log.

	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				fmt.Println(err)
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

			if txn == nil {
				err = nil
			}

			// Check to see if the current statement is
			// a TRANSACTION statement, and if it is
			// then deal with it and move on to the next.

			switch stm.(type) {
			case *sql.BeginStatement:
				txn, err = begin(txn)
				continue
			case *sql.CancelStatement:
				txn, err, buf = cancel(txn, buf, err, send)
				continue
			case *sql.CommitStatement:
				txn, err, buf = commit(txn, buf, err, send)
				continue
			}

			// This is not a TRANSACTION statement and
			// therefore we must time the execution speed
			// and process the statement response.

			now := time.Now()

			// If an error has occured and we are inside
			// a global transaction, then ignore all
			// subsequent statements in the transaction.

			if err == nil {
				res, err = e.operate(txn, stm)
			} else {
				res, err = []interface{}{}, fmt.Errorf("Query not executed")
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

			if txn == nil {
				send <- rsp
				continue
			}

			// If we are inside a global transaction we
			// must buffer the responses for output at
			// the end of the transaction.

			if txn != nil {
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

func (e *executor) operate(txn kvs.TX, ast sql.Statement) (res []interface{}, err error) {

	var loc bool

	// If we are not inside a global transaction
	// then grab a new transaction, ensuring that
	// it is closed at the end.

	if txn == nil {

		loc = true

		switch ast.(type) {
		case *sql.InfoStatement:
			txn, err = readable()
		default:
			txn, err = writable()
		}

		if err != nil {
			return
		}

		defer txn.Close()

	}

	// Execute the defined statement, receiving the
	// result set, and any errors which occured
	// while processing the query.

	switch stm := ast.(type) {

	case *sql.InfoStatement:
		res, err = e.executeInfoStatement(txn, stm)

	case *sql.LetStatement:
		res, err = e.executeLetStatement(txn, stm)
	case *sql.ReturnStatement:
		res, err = e.executeReturnStatement(txn, stm)

	case *sql.SelectStatement:
		res, err = e.executeSelectStatement(txn, stm)
	case *sql.CreateStatement:
		res, err = e.executeCreateStatement(txn, stm)
	case *sql.UpdateStatement:
		res, err = e.executeUpdateStatement(txn, stm)
	case *sql.DeleteStatement:
		res, err = e.executeDeleteStatement(txn, stm)
	case *sql.RelateStatement:
		res, err = e.executeRelateStatement(txn, stm)

	case *sql.DefineScopeStatement:
		res, err = e.executeDefineScopeStatement(txn, stm)
	case *sql.RemoveScopeStatement:
		res, err = e.executeRemoveScopeStatement(txn, stm)

	case *sql.DefineTableStatement:
		res, err = e.executeDefineTableStatement(txn, stm)
	case *sql.RemoveTableStatement:
		res, err = e.executeRemoveTableStatement(txn, stm)

	case *sql.DefineRulesStatement:
		res, err = e.executeDefineRulesStatement(txn, stm)
	case *sql.RemoveRulesStatement:
		res, err = e.executeRemoveRulesStatement(txn, stm)

	case *sql.DefineFieldStatement:
		res, err = e.executeDefineFieldStatement(txn, stm)
	case *sql.RemoveFieldStatement:
		res, err = e.executeRemoveFieldStatement(txn, stm)

	case *sql.DefineIndexStatement:
		res, err = e.executeDefineIndexStatement(txn, stm)
	case *sql.RemoveIndexStatement:
		res, err = e.executeRemoveIndexStatement(txn, stm)

	}

	// If this is a local transaction for only the
	// current statement, then commit or cancel
	// depending on the result error.

	if loc {
		if err != nil {
			txn.Cancel()
		} else {
			txn.Commit()
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
	case *kvs.TXError:
		return "ERR_TX"
	case *kvs.CKError:
		return "ERR_CK"
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

func begin(txn kvs.TX) (tmp kvs.TX, err error) {
	if txn == nil {
		txn, err = writable()
	}
	return txn, err
}

func cancel(txn kvs.TX, buf []*Response, err error, chn chan<- *Response) (kvs.TX, error, []*Response) {

	if txn == nil {
		return nil, nil, buf
	}

	txn.Cancel()

	for _, v := range buf {
		v.Status = "ERR"
		chn <- v
	}

	for i := len(buf) - 1; i >= 0; i-- {
		buf[len(buf)-1] = nil
		buf = buf[:len(buf)-1]
	}

	return nil, nil, buf

}

func commit(txn kvs.TX, buf []*Response, err error, chn chan<- *Response) (kvs.TX, error, []*Response) {

	if txn == nil {
		return nil, nil, buf
	}

	if err != nil {
		txn.Cancel()
	} else {
		txn.Commit()
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

	return nil, nil, buf

}

func writable() (txn kvs.TX, err error) {
	return db.Txn(true)
}

func readable() (txn kvs.TX, err error) {
	return db.Txn(false)
}
