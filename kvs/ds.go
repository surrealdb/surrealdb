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

package kvs

import (
	"strings"

	"github.com/abcum/surreal/cnf"
)

var stores = make(map[string]func(*cnf.Options) (DB, error))

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
	case strings.HasPrefix(opts.DB.Path, "s3://"):
		db, err = stores["rixxdb"](opts)
	case strings.HasPrefix(opts.DB.Path, "gcs://"):
		db, err = stores["rixxdb"](opts)
	case strings.HasPrefix(opts.DB.Path, "logr://"):
		db, err = stores["rixxdb"](opts)
	case strings.HasPrefix(opts.DB.Path, "file://"):
		db, err = stores["rixxdb"](opts)
	case strings.HasPrefix(opts.DB.Path, "rixxdb://"):
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
func (ds *DS) Begin(writable bool) (txn TX, err error) {
	return ds.db.Begin(writable)
}

// Close closes the underlying rixxdb / dendrodb
// database connection, enabling the underlying
// database to clean up remainging transactions.
func (ds *DS) Close() (err error) {
	return ds.db.Close()
}

// Register registers a new database type with
// the kvs package, enabling it's use as a
// backing datastore within SurrealDB.
func Register(name string, constructor func(*cnf.Options) (DB, error)) {
	stores[name] = constructor
}
