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

package db

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

type mutex struct {
	m sync.Map
	l sync.Mutex
}

type value struct {
	v int
	r int64
	w int64
	l sync.RWMutex
}

func (m *mutex) Lock(ctx context.Context, key fmt.Stringer) {
	m.l.Lock()
	_, v := m.item(ctx, key)
	if v.v < vers(ctx) {
		m.l.Unlock()
		panic(errRaceCondition)
	}
	atomic.AddInt64(&v.w, 1)
	m.l.Unlock()
	v.l.Lock()
}

func (m *mutex) RLock(ctx context.Context, key fmt.Stringer) {
	m.l.Lock()
	_, v := m.item(ctx, key)
	atomic.AddInt64(&v.r, 1)
	m.l.Unlock()
	v.l.RLock()
}

func (m *mutex) Unlock(ctx context.Context, key fmt.Stringer) {
	m.l.Lock()
	defer m.l.Unlock()
	k, v := m.item(ctx, key)
	if w := atomic.LoadInt64(&v.w); w > 0 {
		if w := atomic.AddInt64(&v.w, -1); w <= 0 {
			m.m.Delete(k)
		}
		v.l.Unlock()
	}
}

func (m *mutex) RUnlock(ctx context.Context, key fmt.Stringer) {
	m.l.Lock()
	defer m.l.Unlock()
	k, v := m.item(ctx, key)
	if r := atomic.LoadInt64(&v.r); r > 0 {
		if r := atomic.AddInt64(&v.r, -1); r <= 0 {
			m.m.Delete(k)
		}
		v.l.RUnlock()
	}
}

func (m *mutex) item(ctx context.Context, key fmt.Stringer) (string, *value) {
	k := key.String()
	v, _ := m.m.LoadOrStore(k, &value{v: vers(ctx)})
	return k, v.(*value)
}
