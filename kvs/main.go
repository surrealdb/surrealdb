// Copyright © 2016 SurrealDB Ltd.
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

package kvs

import (
	"context"
	"io"

	"github.com/surrealdb/surrealdb/cnf"
	"github.com/surrealdb/surrealdb/log"
)

var ds *DS

// Stores the different backend implementations
var stores = make(map[string]func(*cnf.Options) (DB, error))

// Setup sets up the connection with the data layer
func Setup(opts *cnf.Options) (err error) {
	log.WithPrefix("kvs").Infof("Starting kvs storage at %s", opts.DB.Path)
	ds, err = New(opts)
	log.WithPrefix("kvs").Infof("Started kvs storage at %s", opts.DB.Path)
	return
}

// Exit shuts down the connection with the data layer
func Exit(opts *cnf.Options) (err error) {
	log.WithPrefix("kvs").Infof("Shutting down kvs storage at %s", opts.DB.Path)
	return ds.Close()
}

// Begin begins a new read / write transaction
// with the underlying database, and returns
// the transaction, or any error which occured.
func Begin(ctx context.Context, writable bool) (txn TX, err error) {
	return ds.db.Begin(ctx, writable)
}

// Import loads database operations from a reader.
// This can be used to playback a database snapshot
// into an already running database.
func Import(r io.Reader) (err error) {
	return ds.db.Import(r)
}

// Export saves all database operations to a writer.
// This can be used to save a database snapshot
// to a secondary file or stream.
func Export(w io.Writer) (err error) {
	return ds.db.Export(w)
}

// Close closes the underlying rixxdb / dendrodb
// database connection, enabling the underlying
// database to clean up remainging transactions.
func Close() (err error) {
	return ds.db.Close()
}

// Register registers a new database type with
// the kvs package, enabling it's use as a
// backing datastore within SurrealDB.
func Register(name string, constructor func(*cnf.Options) (DB, error)) {
	stores[name] = constructor
}
