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
	"os"

	"github.com/boltdb/bolt"
)

// DB is a database handle to a single Surreal cluster.
type DB struct {
	db *bolt.DB
}

func New() (db *DB, err error) {

	var bo *bolt.DB

	os.Remove("dev/bolt.db") // TODO remove this code!!!

	bo, err = bolt.Open("dev/bolt.db", 0666, nil)

	bo.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists(bucket)
		return nil
	})

	return &DB{db: bo}, err

}

func (db *DB) All() (kvs []*KV, err error) {

	tx, err := db.Txn(false)
	if err != nil {
		return
	}

	defer tx.Close()

	return tx.All()

}

// Get retrieves a single key:value item.
func (db *DB) Get(key []byte) (kv *KV, err error) {

	tx, err := db.Txn(false)
	if err != nil {
		return
	}

	defer tx.Close()

	return tx.Get(key)

}

// MGet retrieves multiple key:value items.
func (db *DB) MGet(keys ...[]byte) (kvs []*KV, err error) {

	tx, err := db.Txn(false)
	if err != nil {
		return
	}

	defer tx.Close()

	return tx.MGet(keys...)

}

// PGet retrieves the range of rows which are prefixed with `pre`.
func (db *DB) PGet(pre []byte) (kvs []*KV, err error) {

	tx, err := db.Txn(false)
	if err != nil {
		return
	}

	defer tx.Close()

	return tx.PGet(pre)

}

// RGet retrieves the range of `max` rows between `beg` (inclusive) and
// `end` (exclusive). To return the range in descending order, ensure
// that `end` sorts lower than `beg` in the key value store.
func (db *DB) RGet(beg, end []byte, max uint64) (kvs []*KV, err error) {

	tx, err := db.Txn(false)
	if err != nil {
		return
	}

	defer tx.Close()

	return tx.RGet(beg, end, max)

}

// Put sets the value for a key.
func (db *DB) Put(key, val []byte) (err error) {

	tx, err := db.Txn(true)
	if err != nil {
		return
	}

	defer tx.Commit()

	return tx.Put(key, val)

}

// CPut conditionally sets the value for a key if the existing value is equal
// to the expected value. To conditionally set a value only if there is no
// existing entry pass nil for the expected value.
func (db *DB) CPut(key, val, exp []byte) (err error) {

	tx, err := db.Txn(true)
	if err != nil {
		return
	}

	defer tx.Commit()

	return tx.CPut(key, val, exp)

}

// Del deletes a single key:value item.
func (db *DB) Del(key []byte) (err error) {

	tx, err := db.Txn(true)
	if err != nil {
		return
	}

	defer tx.Commit()

	return tx.Del(key)

}

// CDel conditionally deletes a key if the existing value is equal to the
// expected value.
func (db *DB) CDel(key, exp []byte) (err error) {

	tx, err := db.Txn(true)
	if err != nil {
		return
	}

	defer tx.Commit()

	return tx.CDel(key, exp)

}

// MDel deletes multiple key:value items.
func (db *DB) MDel(keys ...[]byte) (err error) {

	tx, err := db.Txn(true)
	if err != nil {
		return
	}

	defer tx.Commit()

	return tx.MDel(keys...)

}

// PDel deletes the range of rows which are prefixed with `pre`.
func (db *DB) PDel(pre []byte) (err error) {

	tx, err := db.Txn(true)
	if err != nil {
		return
	}

	defer tx.Commit()

	return tx.PDel(pre)

}

// RDel deletes the range of `max` rows between `beg` (inclusive) and
// `end` (exclusive). To delete the range in descending order, ensure
// that `end` sorts lower than `beg` in the key value store.
func (db *DB) RDel(beg, end []byte, max uint64) (err error) {

	tx, err := db.Txn(true)
	if err != nil {
		return
	}

	defer tx.Commit()

	return tx.RDel(beg, end, max)

}

// Txn executes retryable in the context of a distributed transaction.
// The transaction is automatically aborted if retryable returns any
// error aside from recoverable internal errors, and is automatically
// committed otherwise. The retryable function should have no side
// effects which could cause problems in the event it must be run more
// than once.
func (db *DB) Txn(writable bool) (txn *TX, err error) {

	tx, err := db.db.Begin(writable)
	if err != nil {
		tx.Rollback()
		return
	}

	txn = &TX{db: db, tx: tx, bu: tx.Bucket(bucket)}

	return

}

func (db *DB) Save(path string) (err error) {

	tx, err := db.Txn(false)
	if err != nil {
		return
	}

	defer tx.Close()

	return tx.tx.CopyFile(path, 0666)

}

// Close ...
func (db *DB) Close() (err error) {

	// TODO Check if there are transactions open...

	return db.db.Close()

}
