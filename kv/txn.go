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

// Txn is a distributed database transaction.
type Txn struct{}

// Get retrieves a single key:value item.
func (t *Txn) Get(key interface{}) (*KV, error) {
	return nil, nil
}

// MGet retrieves multiple key:value items.
func (t *Txn) MGet(keys ...interface{}) ([]*KV, error) {
	return nil, nil
}

// RGet retrieves the range of rows between `beg` (inclusive) and `end`
// (exclusive). To return the range in descending order, ensure that `end`
// sorts lower than `beg` in the key value store.
func (t *Txn) RGet(beg, end interface{}, max int64) ([]*KV, error) {
	return nil, nil
}

// Put sets the value for a key.
func (t *Txn) Put(key, val interface{}) (*KV, error) {
	return nil, nil
}

// CPut conditionally sets the value for a key if the existing value is equal
// to the expected value. To conditionally set a value only if there is no
// existing entry pass nil for the expected value.
func (t *Txn) CPut(key, val, exp interface{}) (*KV, error) {
	return nil, nil
}

// Del deletes a single key:value item.
func (t *Txn) Del(key interface{}) (*KV, error) {
	return nil, nil
}

// CDel conditionally deletes a key if the existing value is equal to the
// expected value.
func (t *Txn) CDel(key, exp interface{}) (*KV, error) {
	return nil, nil
}

// MDel deletes multiple key:value items.
func (t *Txn) MDel(keys ...interface{}) ([]*KV, error) {
	return nil, nil
}

// RDel deletes the rows between begin (inclusive) and end (exclusive).
func (t *Txn) RDel(beg, end interface{}, max int64) ([]*KV, error) {
	return nil, nil
}

func (t *Txn) Commit(key interface{}) (*KV, error) {
	return nil, nil
}

func (t *Txn) Rollback(key interface{}) (*KV, error) {
	return nil, nil
}
