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

package memory

import (
	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/stores"
	"github.com/steveyen/gkvlite"
)

func init() {
	stores.Register("memory", New)
}

type Store struct {
	ctx cnf.Context
	db  gkvlite.Collection
}

func New(ctx cnf.Context) (stores.Store, error) {

	db, err := gkvlite.NewStore(nil)

	if err != nil {
		return nil, err
	}

	store := Store{ctx: ctx, db: *db.SetCollection(ctx.Base, nil)}

	return &store, nil

}

func (store *Store) Get(key interface{}) stores.KeyValue {
	val, _ := store.db.Get([]byte(key.(string)))
	return stores.KeyValue{
		Key:   key.(string),
		Value: string(val),
	}
}

func (store *Store) Put(key, val interface{}) error {
	return store.db.Set([]byte(key.(string)), []byte(val.(string)))
}

func (store *Store) Del(key interface{}) error {
	return nil
}

func (store *Store) Scan(beg, end interface{}, max int64) []stores.KeyValue {
	return []stores.KeyValue{}
	// store.db.VisitItemsAscend([]byte(key.(string)), true, func(i *gkvlite.Item) bool {
	// 	// This visitor callback will be invoked with every item
	// 	// with key "ford" and onwards, in key-sorted order.
	// 	// So: "mercedes", "tesla" are visited, in that ascending order,
	// 	// but not "bmw".
	// 	// If we want to stop visiting, return false;
	// 	// otherwise return true to keep visiting.
	// 	return true
	// })
}
