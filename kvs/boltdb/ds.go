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
	"github.com/boltdb/bolt"

	"github.com/abcum/surreal/kvs"
)

type DS struct {
	db *bolt.DB
}

func (ds *DS) Txn(writable bool) (txn kvs.TX, err error) {

	tx, err := ds.db.Begin(writable)
	if err != nil {
		err = &kvs.DSError{err}
		if tx != nil {
			tx.Rollback()
		}
		return
	}

	return &TX{ds: ds, tx: tx, bu: tx.Bucket(bucket)}, err

}

func (ds *DS) Close() (err error) {

	return ds.db.Close()

}
