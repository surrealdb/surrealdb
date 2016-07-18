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
	"strings"

	"github.com/boltdb/bolt"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/kvs"
)

var bucket = []byte("default")

func init() {
	kvs.Register("boltdb", New)
}

func New(opts *cnf.Options) (ds kvs.DS, err error) {

	var db *bolt.DB

	path := strings.TrimLeft(opts.DB.Path, "boltdb://")

	db, err = bolt.Open(path, 0666, nil)

	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists(bucket)
		return nil
	})

	return &DS{db: db}, err

}
