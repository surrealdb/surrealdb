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

package pgsql

import (
	"bytes"
	"math"

	"database/sql"

	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/util/cryp"
	"github.com/abcum/surreal/util/snap"
)

// TX is a distributed database transaction.
type TX struct {
	ds *DS
	ck []byte
	tx *sql.Tx
}

// All retrieves all key:value items in the db.
func (tx *TX) All() (kvs []kvs.KV, err error) {

	res, err := tx.tx.Query("SELECT `key`, `val` FROM kv ORDER BY `key` ASC")
	if err != nil {
		return
	}

	defer res.Close()

	for res.Next() {

		var key, val []byte

		err := res.Scan(&key, &val)
		if err != nil {
			return nil, err
		}

		kv, err := get(tx, key, val)
		if err != nil {
			return nil, err
		}

		kvs = append(kvs, kv)

	}

	err = res.Err()
	if err != nil {
		return nil, err
	}

	return

}

// Get retrieves a single key:value item.
func (tx *TX) Get(key []byte) (kv kvs.KV, err error) {

	row := tx.tx.QueryRow("SELECT `val` FROM kv WHERE `key` = $1", key)

	var val []byte

	row.Scan(&val)

	return get(tx, key, val)

}

// MGet retrieves multiple key:value items.
func (tx *TX) MGet(keys ...[]byte) (kvs []kvs.KV, err error) {

	/*
		res, err := tx.tx.Query("SELECT `key`, `val` FROM kv WHERE `key` IN ($1)", keys)
		if err != nil {
			return
		}

		defer res.Close()

		for res.Next() {

			var key, val []byte

			err := res.Scan(&key, &val)
			if err != nil {
				return nil, err
			}

			kv, err := get(tx, key, val)
			if err != nil {
				return nil, err
			}

			kvs = append(kvs, kv)

		}

		err = res.Err()
		if err != nil {
			return nil, err
		}
	*/

	for _, key := range keys {
		kv, _ := tx.Get(key)
		kvs = append(kvs, kv)
	}

	return

}

// PGet retrieves the range of rows which are prefixed with `pre`.
func (tx *TX) PGet(pre []byte) (kvs []kvs.KV, err error) {

	end := append(pre, 0xff)

	return tx.RGet(pre, end, 0)

}

// RGet retrieves the range of `max` rows between `beg` (inclusive) and
// `end` (exclusive). To return the range in descending order, ensure
// that `end` sorts lower than `beg` in the key value store.
func (tx *TX) RGet(beg, end []byte, max uint64) (kvs []kvs.KV, err error) {

	if max == 0 {
		max = math.MaxUint64
	}

	res, err := tx.tx.Query("SELECT `key`, `val` FROM kv WHERE `key` BETWEEN $1 AND $2 ORDER BY `key` ASC LIMIT $3", beg, end, int(max))
	if err != nil {
		return nil, err
	}

	defer res.Close()

	for res.Next() {

		var key, val []byte

		err := res.Scan(&key, &val)
		if err != nil {
			return nil, err
		}

		kv, err := get(tx, key, val)
		if err != nil {
			return nil, err
		}

		kvs = append(kvs, kv)

	}

	err = res.Err()
	if err != nil {
		return nil, err
	}

	return

}

// Put sets the value for a key.
func (tx *TX) Put(key, val []byte) (err error) {

	if val, err = snap.Encode(val); err != nil {
		err = &kvs.DBError{err}
		return
	}

	if val, err = cryp.Encrypt(tx.ck, val); err != nil {
		err = &kvs.CKError{err}
		return
	}

	if _, err = tx.tx.Exec("INSERT INTO kv (`key`, `val`) VALUES ($1, $2) ON DUPLICATE KEY UPDATE `val` = $3", key, val, val); err != nil {
		err = &kvs.DBError{err}
		return
	}

	return

}

// CPut conditionally sets the value for a key if the existing value is equal
// to the expected value. To conditionally set a value only if there is no
// existing entry pass nil for the expected value.
func (tx *TX) CPut(key, val, exp []byte) (err error) {

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

	if val, err = cryp.Encrypt(tx.ck, val); err != nil {
		err = &kvs.CKError{err}
		return
	}

	if _, err = tx.tx.Exec("INSERT INTO kv (`key`, `val`) VALUES ($1, $2) ON DUPLICATE KEY UPDATE `val` = $3", key, val, val); err != nil {
		err = &kvs.DBError{err}
		return
	}

	return

}

// Del deletes a single key:value item.
func (tx *TX) Del(key []byte) (err error) {

	if _, err = tx.tx.Exec("DELETE FROM kv WHERE `key` = $1", key); err != nil {
		err = &kvs.DBError{err}
		return
	}

	return

}

// CDel conditionally deletes a key if the existing value is equal to the
// expected value.
func (tx *TX) CDel(key, exp []byte) (err error) {

	now, _ := tx.Get(key)
	act := now.(*KV).val

	if !bytes.Equal(act, exp) {
		err = &kvs.KVError{err, key, act, exp}
		return
	}

	if _, err = tx.tx.Exec("DELETE FROM kv WHERE `key` = $1", key); err != nil {
		err = &kvs.DBError{err}
		return
	}

	return

}

// MDel deletes multiple key:value items.
func (tx *TX) MDel(keys ...[]byte) (err error) {

	/*
		if _, err = tx.tx.Exec("DELETE FROM kv WHERE `key` IN ($1)", keys); err != nil {
			err = &kvs.DBError{err}
			return
		}
	*/

	for _, key := range keys {
		err = tx.Del(key)
	}

	return

}

// PDel deletes the range of rows which are prefixed with `pre`.
func (tx *TX) PDel(pre []byte) (err error) {

	end := append(pre, 0xff)

	return tx.RDel(pre, end, 0)

}

// RDel deletes the range of `max` rows between `beg` (inclusive) and
// `end` (exclusive). To delete the range in descending order, ensure
// that `end` sorts lower than `beg` in the key value store.
func (tx *TX) RDel(beg, end []byte, max uint64) (err error) {

	if max == 0 {
		max = math.MaxUint64
	}

	if _, err = tx.tx.Exec("DELETE FROM kv WHERE `key` BETWEEN $1 AND $2 ORDER BY `key` ASC LIMIT $3", beg, end, int(max)); err != nil {
		err = &kvs.DBError{err}
		return
	}

	return

}

func (tx *TX) Close() (err error) {
	return tx.Rollback()
}

func (tx *TX) Commit() (err error) {
	return tx.tx.Commit()
}

func (tx *TX) Rollback() (err error) {
	return tx.tx.Rollback()
}

func get(tx *TX, key, val []byte) (kv *KV, err error) {

	kv = &KV{
		exi: (val != nil),
		key: key,
		val: val,
	}

	kv.val, err = cryp.Decrypt(tx.ck, kv.val)
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
