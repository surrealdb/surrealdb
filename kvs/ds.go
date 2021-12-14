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
	"strings"

	"github.com/surrealdb/surrealdb/cnf"
)

// DB represents a backing datastore.
type DS struct {
	db DB
}

// New sets up the underlying key-value store
func New(opts *cnf.Options) (ds *DS, err error) {

	var db DB

	switch {
	case opts.DB.Path == "memory":
		db, err = stores["rixxdb"](opts)
	case strings.HasPrefix(opts.DB.Path, "file://"):
		db, err = stores["rixxdb"](opts)
	case strings.HasPrefix(opts.DB.Path, "dendrodb://"):
		db, err = stores["dendro"](opts)
	}

	if err != nil {
		return
	}

	ds = &DS{db: db}

	return

}

// Begin begins a new read / write transaction
// with the underlying database, and returns
// the transaction, or any error which occured.
func (ds *DS) Begin(ctx context.Context, writable bool) (txn TX, err error) {
	return ds.db.Begin(ctx, writable)
}

// Import loads database operations from a reader.
// This can be used to playback a database snapshot
// into an already running database.
func (ds *DS) Import(r io.Reader) (err error) {
	return ds.db.Import(r)
}

// Export saves all database operations to a writer.
// This can be used to save a database snapshot
// to a secondary file or stream.
func (ds *DS) Export(w io.Writer) (err error) {
	return ds.db.Export(w)
}

// Close closes the underlying rixxdb / dendrodb
// database connection, enabling the underlying
// database to clean up remainging transactions.
func (ds *DS) Close() (err error) {
	return ds.db.Close()
}
