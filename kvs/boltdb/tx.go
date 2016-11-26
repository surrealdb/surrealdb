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

package boltdb

import (
	"bytes"
	"math"

	"github.com/boltdb/bolt"

	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/util/cryp"
	"github.com/abcum/surreal/util/snap"
)

// TX is a distributed database transaction.
type TX struct {
	ds *DS
	do bool
	ck []byte
	tx *bolt.Tx
	bu *bolt.Bucket
}

// All retrieves all key:value items in the db.
func (tx *TX) All() (kvs []kvs.KV, err error) {

	err = tx.bu.ForEach(func(key, val []byte) (err error) {

		kv, err := get(tx, key, val)
		if err != nil {
			return
		}

		kvs = append(kvs, kv)

		return nil

	})

	return

}

// Get retrieves a single key:value item.
func (tx *TX) Get(key []byte) (kv kvs.KV, err error) {

	val := tx.bu.Get(key)

	return get(tx, key, val)

}

// MGet retrieves multiple key:value items.
func (tx *TX) MGet(keys ...[]byte) (kvs []kvs.KV, err error) {

	for _, key := range keys {

		val := tx.bu.Get(key)

		kv, err := get(tx, key, val)
		if err != nil {
			return nil, err
		}

		kvs = append(kvs, kv)

	}

	return

}

// PGet retrieves the range of rows which are prefixed with `pre`.
func (tx *TX) PGet(pre []byte) (kvs []kvs.KV, err error) {

	cu := tx.bu.Cursor()

	for key, val := cu.Seek(pre); bytes.HasPrefix(key, pre); key, val = cu.Next() {

		kv, err := get(tx, key, val)
		if err != nil {
			return nil, err
		}

		kvs = append(kvs, kv)

	}

	return

}

// RGet retrieves the range of `max` rows between `beg` (inclusive) and
// `end` (exclusive). To return the range in descending order, ensure
// that `end` sorts lower than `beg` in the key value store.
func (tx *TX) RGet(beg, end []byte, max uint64) (kvs []kvs.KV, err error) {

	if max == 0 {
		max = math.MaxUint64
	}

	cu := tx.bu.Cursor()

	if bytes.Compare(beg, end) < 1 {
		for key, val := cu.Seek(beg); key != nil && max > 0 && bytes.Compare(key, end) < 0; key, val = cu.Next() {

			kv, err := get(tx, key, val)
			if err != nil {
				return nil, err
			}

			kvs = append(kvs, kv)

			max--
		}
	}

	if bytes.Compare(beg, end) > 1 {
		for key, val := cu.Seek(end); key != nil && max > 0 && bytes.Compare(beg, key) < 0; key, val = cu.Prev() {

			kv, err := get(tx, key, val)
			if err != nil {
				return nil, err
			}

			kvs = append(kvs, kv)

			max--
		}
	}

	return

}

// Put sets the value for a key.
func (tx *TX) Put(key, val []byte) (err error) {

	if !tx.tx.Writable() {
		err = &kvs.TXError{err}
		return
	}

	if val, err = snap.Encode(val); err != nil {
		err = &kvs.DBError{err}
		return
	}

	if val, err = cryp.Encrypt(tx.ds.ck, val); err != nil {
		err = &kvs.CKError{err}
		return
	}

	if err = tx.bu.Put(key, val); err != nil {
		err = &kvs.DBError{err}
		return
	}

	return

}

// CPut conditionally sets the value for a key if the existing value is equal
// to the expected value. To conditionally set a value only if there is no
// existing entry pass nil for the expected value.
func (tx *TX) CPut(key, val, exp []byte) (err error) {

	if !tx.tx.Writable() {
		err = &kvs.TXError{err}
		return
	}

	now, _ := tx.Get(key)
	act := now.(*KV).val

	if !bytes.Equal(act, exp) {
		err = &kvs.KVError{err, key, act, exp}
		return
	}

	if val, err = snap.Encode(val); err != nil {
		err = &kvs.DBError{err}
		return
	}

	if val, err = cryp.Encrypt(tx.ds.ck, val); err != nil {
		err = &kvs.CKError{err}
		return
	}

	if err = tx.bu.Put(key, val); err != nil {
		err = &kvs.DBError{err}
		return
	}

	return

}

// Del deletes a single key:value item.
func (tx *TX) Del(key []byte) (err error) {

	if !tx.tx.Writable() {
		err = &kvs.TXError{err}
		return
	}

	if err = tx.bu.Delete(key); err != nil {
		err = &kvs.DBError{err}
		return
	}

	return

}

// CDel conditionally deletes a key if the existing value is equal to the
// expected value.
func (tx *TX) CDel(key, exp []byte) (err error) {

	if !tx.tx.Writable() {
		err = &kvs.TXError{err}
		return
	}

	now, _ := tx.Get(key)
	act := now.(*KV).val

	if !bytes.Equal(act, exp) {
		err = &kvs.KVError{err, key, act, exp}
		return
	}

	if err = tx.bu.Delete(key); err != nil {
		err = &kvs.DBError{err}
		return
	}

	return

}

// MDel deletes multiple key:value items.
func (tx *TX) MDel(keys ...[]byte) (err error) {

	if !tx.tx.Writable() {
		err = &kvs.TXError{err}
		return
	}

	for _, key := range keys {

		if err = tx.bu.Delete(key); err != nil {
			err = &kvs.DBError{err}
			return
		}

	}

	return

}

// PDel deletes the range of rows which are prefixed with `pre`.
func (tx *TX) PDel(pre []byte) (err error) {

	cu := tx.bu.Cursor()

	for key, _ := cu.Seek(pre); bytes.HasPrefix(key, pre); key, _ = cu.Seek(pre) {
		if err = tx.bu.Delete(key); err != nil {
			err = &kvs.DBError{err}
			return
		}
	}

	return

}

// RDel deletes the range of `max` rows between `beg` (inclusive) and
// `end` (exclusive). To delete the range in descending order, ensure
// that `end` sorts lower than `beg` in the key value store.
func (tx *TX) RDel(beg, end []byte, max uint64) (err error) {

	if max == 0 {
		max = math.MaxUint64
	}

	if !tx.tx.Writable() {
		err = &kvs.TXError{err}
		return
	}

	cu := tx.bu.Cursor()

	if bytes.Compare(beg, end) < 1 {
		for key, _ := cu.Seek(beg); key != nil && max > 0 && bytes.Compare(key, end) < 0; key, _ = cu.Seek(beg) {
			if err = tx.bu.Delete(key); err != nil {
				err = &kvs.DBError{err}
				return
			}
			max--
		}
	}

	if bytes.Compare(beg, end) > 1 {
		for key, _ := cu.Seek(end); key != nil && max > 0 && bytes.Compare(beg, key) < 0; key, _ = cu.Seek(end) {
			if err = tx.bu.Delete(key); err != nil {
				err = &kvs.DBError{err}
				return
			}
			max--
		}
	}

	return

}

func (tx *TX) Done() (val bool) {
	return tx.do
}

func (tx *TX) Close() (err error) {
	return tx.Rollback()
}

func (tx *TX) Cancel() (err error) {
	return tx.Rollback()
}

func (tx *TX) Commit() (err error) {
	tx.do = true
	if tx.tx.Writable() {
		return tx.tx.Commit()
	}
	return tx.tx.Rollback()
}

func (tx *TX) Rollback() (err error) {
	tx.do = true
	return tx.tx.Rollback()
}

func get(tx *TX, key, val []byte) (kv *KV, err error) {

	kv = &KV{
		exi: (val != nil),
		key: key,
		val: val,
	}

	kv.val, err = cryp.Decrypt(tx.ds.ck, kv.val)
	if err != nil {
		err = &kvs.CKError{err}
		return
	}

	kv.val, err = snap.Decode(kv.val)
	if err != nil {
		err = &kvs.DBError{err}
		return
	}

	return

}
