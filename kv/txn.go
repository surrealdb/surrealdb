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

import (
	"bytes"
	"math"

	"github.com/boltdb/bolt"
)

// Txn is a distributed database transaction.
type Txn struct {
	db *DB
	tx *bolt.Tx
	bu *bolt.Bucket
}

// All retrieves all key:value items in the db.
func (tx *Txn) All() (kvs []*KV, err error) {

	err = tx.bu.ForEach(func(key, val []byte) error {
		kvs = mul(kvs, key, val)
		return nil
	})

	return

}

// Get retrieves a single key:value item.
func (tx *Txn) Get(key []byte) (kv *KV, err error) {

	val := tx.bu.Get(key)

	kv = one(key, val)

	return

}

// MGet retrieves multiple key:value items.
func (tx *Txn) MGet(keys ...[]byte) (kvs []*KV, err error) {

	for _, key := range keys {
		val := tx.bu.Get(key)
		kvs = mul(kvs, key, val)
	}

	return

}

// PGet retrieves the range of rows which are prefixed with `pre`.
func (tx *Txn) PGet(pre []byte) (kvs []*KV, err error) {

	cu := tx.bu.Cursor()

	for key, val := cu.Seek(pre); bytes.HasPrefix(key, pre); key, val = cu.Next() {
		kvs = mul(kvs, key, val)
	}

	return

}

// RGet retrieves the range of `max` rows between `beg` (inclusive) and
// `end` (exclusive). To return the range in descending order, ensure
// that `end` sorts lower than `beg` in the key value store.
func (tx *Txn) RGet(beg, end []byte, max uint64) (kvs []*KV, err error) {

	if max == 0 {
		max = math.MaxUint64
	}

	cu := tx.bu.Cursor()

	if bytes.Compare(beg, end) < 1 {
		for key, val := cu.Seek(beg); key != nil && max > 0 && bytes.Compare(key, end) < 0; key, val = cu.Next() {
			kvs = mul(kvs, key, val)
			max--
		}
	}

	if bytes.Compare(beg, end) > 1 {
		for key, val := cu.Seek(end); key != nil && max > 0 && bytes.Compare(beg, key) < 0; key, val = cu.Prev() {
			kvs = mul(kvs, key, val)
			max--
		}
	}

	return

}

// Put sets the value for a key.
func (tx *Txn) Put(key, val []byte) (err error) {

	if !tx.tx.Writable() {
		err = &TXError{err}
		return
	}

	if err = tx.bu.Put(key, val); err != nil {
		err = &DBError{err}
		return
	}

	return

}

// CPut conditionally sets the value for a key if the existing value is equal
// to the expected value. To conditionally set a value only if there is no
// existing entry pass nil for the expected value.
func (tx *Txn) CPut(key, val, exp []byte) (err error) {

	if !tx.tx.Writable() {
		err = &TXError{err}
		return
	}

	now := tx.bu.Get(key)

	if !bytes.Equal(now, exp) {
		err = &KVError{err, key, now, exp}
		return
	}

	if err = tx.bu.Put(key, val); err != nil {
		err = &DBError{err}
		return
	}

	return

}

// Del deletes a single key:value item.
func (tx *Txn) Del(key []byte) (err error) {

	if !tx.tx.Writable() {
		err = &TXError{err}
		return
	}

	if err = tx.bu.Delete(key); err != nil {
		err = &DBError{err}
		return
	}

	return

}

// CDel conditionally deletes a key if the existing value is equal to the
// expected value.
func (tx *Txn) CDel(key, exp []byte) (err error) {

	if !tx.tx.Writable() {
		err = &TXError{err}
		return
	}

	now := tx.bu.Get(key)

	if !bytes.Equal(now, exp) {
		err = &KVError{err, key, now, exp}
		return
	}

	if err = tx.bu.Delete(key); err != nil {
		err = &DBError{err}
		return
	}

	return

}

// MDel deletes multiple key:value items.
func (tx *Txn) MDel(keys ...[]byte) (err error) {

	if !tx.tx.Writable() {
		err = &TXError{err}
		return
	}

	for _, key := range keys {

		if err = tx.bu.Delete(key); err != nil {
			err = &DBError{err}
			return
		}

	}

	return

}

// PDel deletes the range of rows which are prefixed with `pre`.
func (tx *Txn) PDel(pre []byte) (err error) {

	cu := tx.bu.Cursor()

	for key, _ := cu.Seek(pre); bytes.HasPrefix(key, pre); key, _ = cu.Next() {
		if err = tx.bu.Delete(key); err != nil {
			err = &DBError{err}
			return
		}
	}

	return

}

// RDel deletes the range of `max` rows between `beg` (inclusive) and
// `end` (exclusive). To delete the range in descending order, ensure
// that `end` sorts lower than `beg` in the key value store.
func (tx *Txn) RDel(beg, end []byte, max uint64) (err error) {

	if max == 0 {
		max = math.MaxUint64
	}

	if !tx.tx.Writable() {
		err = &TXError{err}
		return
	}

	cu := tx.bu.Cursor()

	if bytes.Compare(beg, end) < 1 {
		for key, _ := cu.Seek(beg); key != nil && max > 0 && bytes.Compare(key, end) < 0; key, _ = cu.Next() {
			if err = tx.bu.Delete(key); err != nil {
				err = &DBError{err}
				return
			}
			max--
		}
	}

	if bytes.Compare(beg, end) > 1 {
		for key, _ := cu.Seek(end); key != nil && max > 0 && bytes.Compare(beg, key) < 0; key, _ = cu.Prev() {
			if err = tx.bu.Delete(key); err != nil {
				err = &DBError{err}
				return
			}
			max--
		}
	}

	return

}

func (tx *Txn) Close() (err error) {
	return tx.Rollback()
}

func (tx *Txn) Commit() (err error) {
	return tx.tx.Commit()
}

func (tx *Txn) Rollback() (err error) {
	return tx.tx.Rollback()
}

func one(key, val []byte) (kv *KV) {

	kv = &KV{
		Exi: (val != nil),
		Key: key,
		Val: make([]byte, len(val)),
	}

	copy(kv.Val, val)

	return

}

func mul(mul []*KV, key, val []byte) (kvs []*KV) {

	kvs = append(mul, one(key, val))

	return

}
