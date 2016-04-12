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

package kv

// DB is a database handle to a single Surreal cluster.
type DB struct{}

// Get retrieves a single key:value item.
func (d *DB) Get(key interface{}) (*KV, error) {
	return nil, nil
}

// MGet retrieves multiple key:value items.
func (d *DB) MGet(keys ...interface{}) ([]*KV, error) {
	return nil, nil
}

// RGet retrieves the range of rows between `beg` (inclusive) and `end`
// (exclusive). To return the range in descending order, ensure that `end`
// sorts lower than `beg` in the key value store.
func (d *DB) RGet(beg, end interface{}, max int64) ([]*KV, error) {
	return nil, nil
}

// Put sets the value for a key.
func (d *DB) Put(key, val interface{}) (*KV, error) {
	return nil, nil
}

// CPut conditionally sets the value for a key if the existing value is equal
// to the expected value. To conditionally set a value only if there is no
// existing entry pass nil for the expected value.
func (d *DB) CPut(key, val, exp interface{}) (*KV, error) {
	return nil, nil
}

// Del deletes a single key:value item.
func (d *DB) Del(key interface{}) (*KV, error) {
	return nil, nil
}

// CDel conditionally deletes a key if the existing value is equal to the
// expected value.
func (d *DB) CDel(key, exp interface{}) (*KV, error) {
	return nil, nil
}

// MDel deletes multiple key:value items.
func (d *DB) MDel(keys ...interface{}) ([]*KV, error) {
	return nil, nil
}

// RDel deletes the rows between begin (inclusive) and end (exclusive).
func (d *DB) RDel(beg, end interface{}, max int64) ([]*KV, error) {
	return nil, nil
}

// Txn executes retryable in the context of a distributed transaction.
// The transaction is automatically aborted if retryable returns any
// error aside from recoverable internal errors, and is automatically
// committed otherwise. The retryable function should have no side
// effects which could cause problems in the event it must be run more
// than once.
func (d *DB) Txn() error {
	return nil
}
