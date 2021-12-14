// Copyright © 2016 SurrealDB Ltd.
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

package txn

import (
	"sync"

	"context"

	"github.com/surrealdb/surrealdb/kvs"
)

type symbol int8

const (
	_ns symbol = iota
	_db
	_tb
	_fd
	_ix
	_ev
	_ft
	_lv
	_kv
)

// --------------------------------------------------

type TX struct {
	kvs.TX
	lock sync.RWMutex
	data map[symbol]map[string]interface{}
}

func New(ctx context.Context, rw bool) (*TX, error) {
	txn, err := kvs.Begin(ctx, rw)
	if err != nil {
		return nil, err
	}
	return &TX{TX: txn}, nil
}

// --------------------------------------------------

func (t *TX) mem(s symbol) {
	t.lock.Lock()
	if t.data == nil {
		t.data = make(map[symbol]map[string]interface{}, 8)
	}
	if t.data[s] == nil {
		t.data[s] = make(map[string]interface{}, 5)
	}
	t.lock.Unlock()
}

func (t *TX) del(s symbol, key string) {
	t.mem(s)
	t.lock.Lock()
	delete(t.data[s], key)
	t.lock.Unlock()
}

func (t *TX) set(s symbol, key string, val interface{}) {
	t.mem(s)
	t.lock.Lock()
	t.data[s][key] = val
	t.lock.Unlock()
}

func (t *TX) get(s symbol, key string) (val interface{}, ok bool) {
	t.mem(s)
	t.lock.RLock()
	val, ok = t.data[s][key]
	t.lock.RUnlock()
	return
}
