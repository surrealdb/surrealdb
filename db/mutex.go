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

package db

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

type mutex struct {
	m sync.Map
}

type value struct {
	v uint32
	q chan struct{}
	l chan struct{}
}

func (m *mutex) Lock(ctx context.Context, key fmt.Stringer) {

	_, v := m.item(ctx, key)

	select {
	case <-ctx.Done():
		return
	case <-v.q:
		return
	case v.l <- struct{}{}:
		atomic.StoreUint32(&v.v, vers(ctx))
		return
	default:
		if atomic.LoadUint32(&v.v) < vers(ctx) {
			close(v.q)
			panic(errRaceCondition)
		}
	}

	select {
	case <-ctx.Done():
		return
	case <-v.q:
		return
	case v.l <- struct{}{}:
		atomic.StoreUint32(&v.v, vers(ctx))
		return
	}

}

func (m *mutex) Unlock(ctx context.Context, key fmt.Stringer) {

	_, v := m.item(ctx, key)

	select {
	case <-ctx.Done():
		return
	case <-v.q:
		return
	case <-v.l:
		return
	}

}

func (m *mutex) item(ctx context.Context, key fmt.Stringer) (string, *value) {
	k := key.String()
	v, _ := m.m.LoadOrStore(k, &value{
		v: vers(ctx),
		q: make(chan struct{}),
		l: make(chan struct{}, 1),
	})
	return k, v.(*value)
}
