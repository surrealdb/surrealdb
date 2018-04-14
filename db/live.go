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
	"sync"

	"context"

	"github.com/abcum/fibre"
	"github.com/abcum/surreal/sql"
)

var lock sync.Mutex

var sockets map[string]*socket

func init() {
	sockets = make(map[string]*socket)
}

func register(fib *fibre.Context, id string) func() {
	return func() {

		lock.Lock()
		defer lock.Unlock()

		sockets[id] = &socket{
			fibre: fib,
			items: make(map[string][]interface{}),
			lives: make(map[string]*sql.LiveStatement),
		}

	}
}

func deregister(fib *fibre.Context, id string) func() {
	return func() {

		lock.Lock()
		defer lock.Unlock()

		if sck, ok := sockets[id]; ok {
			sck.deregister(id)
		}

	}
}

func (e *executor) executeLive(ctx context.Context, stm *sql.LiveStatement) (out []interface{}, err error) {

	stm.FB = ctx.Value(ctxKeyId).(string)

	if sck, ok := sockets[stm.FB]; ok {
		return sck.executeLive(e, ctx, stm)
	}

	return nil, &QueryError{}

}

func (e *executor) executeKill(ctx context.Context, stm *sql.KillStatement) (out []interface{}, err error) {

	stm.FB = ctx.Value(ctxKeyId).(string)

	if sck, ok := sockets[stm.FB]; ok {
		return sck.executeKill(e, ctx, stm)
	}

	return nil, &QueryError{}

}
