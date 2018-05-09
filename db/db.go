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
	"io"
	"os"

	"context"

	"net/http"

	"github.com/abcum/fibre"
	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/log"
	"github.com/abcum/surreal/sql"

	"github.com/abcum/surreal/util/data"
	"github.com/abcum/surreal/util/uuid"

	_ "github.com/abcum/surreal/kvs/mysql"
	_ "github.com/abcum/surreal/kvs/rixxdb"
)

var db *kvs.DS

// Response is a response from the database
type Response struct {
	Time   string        `codec:"time,omitempty"`
	Status string        `codec:"status,omitempty"`
	Detail string        `codec:"detail,omitempty"`
	Result []interface{} `codec:"result,omitempty"`
}

// Dispatch is a dispatch from the database
type Dispatch struct {
	Query  string      `codec:"query,omitempty"`
	Action string      `codec:"action,omitempty"`
	Result interface{} `codec:"result,omitempty"`
}

// Setup sets up the connection with the data layer
func Setup(opts *cnf.Options) (err error) {

	log.WithPrefix("db").Infof("Starting database")

	db, err = kvs.New(opts)

	return
}

// Exit shuts down the connection with the data layer
func Exit() (err error) {

	log.WithPrefix("db").Infof("Gracefully shutting down database")

	sockets.Range(func(key, val interface{}) bool {
		id, so := key.(string), val.(*socket)
		deregister(so.fibre, id)()
		return true
	})

	return db.Close()

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
	return db.Begin(context.Background(), rw)
}

// Socket registers a websocket for live queries
// returning two callback functions. The first
// function should be called when the websocket
// connects, and the second function should be
// called when the websocket disconnects.
func Socket(fib *fibre.Context, id string) (beg, end func()) {
	return register(fib, id), deregister(fib, id)
}

// Execute parses a single sql query, or multiple
// sql queries, and executes them serially against
// the underlying data layer.
func Execute(fib *fibre.Context, txt interface{}, vars map[string]interface{}) (out []*Response, err error) {

	// Parse the received SQL batch query strings
	// into SQL ASTs, using any immutable preset
	// variables if set.

	ast, err := sql.Parse(fib, txt)
	if err != nil {
		return
	}

	// Process the parsed SQL batch query using
	// the predefined query variables.

	return Process(fib, ast, vars)

}

// Process takes a parsed set of sql queries and
// executes them serially against the underlying
// data layer.
func Process(fib *fibre.Context, ast *sql.Query, vars map[string]interface{}) (out []*Response, err error) {

	// If no preset variables have been defined
	// then ensure that the variables is
	// instantiated for future use.

	if vars == nil {
		vars = make(map[string]interface{})
	}

	// Ensure that we have a unique id assigned
	// to this fibre connection, as we need it
	// to detect unique websocket notifications.

	if fib.Get(ctxKeyId) == nil {
		fib.Set(ctxKeyId, uuid.New().String())
	}

	// Ensure that the IP address of the
	// user signing in is available so that
	// it can be used within signin queries.

	vars[varKeyIp] = fib.IP().String()

	// Ensure that the website origin of the
	// user signing in is available so that
	// it can be used within signin queries.

	vars[varKeyOrigin] = fib.Origin()

	// Ensure that the specified environment
	// variable 'ENV' is available to the
	// request, to detect the environment.

	vars[varKeyEnv] = os.Getenv(varKeyEnv)

	// Ensure that the current authentication
	// data is made available as a runtime
	// variable to the query layer.

	vars[varKeyAuth] = fib.Get(varKeyAuth).(*cnf.Auth).Data

	// Ensure that the current authentication
	// scope is made available as a runtime
	// variable to the query layer.

	vars[varKeyScope] = fib.Get(varKeyAuth).(*cnf.Auth).Scope

	// Create a new context so that we can quit
	// all goroutine workers if the http client
	// itself is closed before finishing.

	ctx, quit := context.WithCancel(fib.Context())

	// When this function has finished ensure
	// that we cancel this context so that any
	// associated resources are released.

	defer quit()

	// Get the unique id for this connection
	// so that we can assign it to the context
	// and detect any websocket notifications.

	id := fib.Get(ctxKeyId).(string)

	// Assign the fibre request context id to
	// the context so that we can log the id
	// together with the request.

	ctx = context.WithValue(ctx, ctxKeyId, id)

	// Assign the authentication data to the
	// context so that we can log the auth kind
	// and the auth variable data to the request.

	auth := fib.Get(varKeyAuth).(*cnf.Auth)
	ctx = context.WithValue(ctx, ctxKeyAuth, auth.Data)
	ctx = context.WithValue(ctx, ctxKeyKind, auth.Kind)

	// Add the request variables to the context
	// so that we can access them at a later
	// stage within the nested callbacks.

	ctx = context.WithValue(ctx, ctxKeyVars, data.Consume(vars))

	// If the current connection is a normal http
	// connection then force quit any running
	// queries if the http socket closes.

	if _, ok := fib.Response().Writer().(http.CloseNotifier); ok {

		exit := fib.Response().CloseNotify()
		done := make(chan struct{})
		defer close(done)

		go func() {
			select {
			case <-done:
			case <-exit:
				quit()
			}
		}()

	}

	// Create a new query executor with the query
	// details, and the current runtime variables
	// and execute the queries within.

	executor := newExecutor()

	go executor.execute(ctx, ast)

	// Wait for all of the processed queries to
	// return results, buffer the output, and
	// return the output when finished.

	for {
		select {
		case <-ctx.Done():
			return nil, fibre.NewHTTPError(504)
		case res, open := <-executor.send:
			if !open {
				return
			}
			out = append(out, res)
		}
	}

	return

}
