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
	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/sql"
)

var sockets sync.Map

func register(fib *fibre.Context, id string) func() {
	return func() {

		sockets.Store(id, &socket{
			fibre: fib,
			items: make(map[string][]interface{}),
			lives: make(map[string]*sql.LiveStatement),
		})

	}
}

func deregister(fib *fibre.Context, id string) func() {
	return func() {

		if sck, ok := sockets.Load(id); ok {
			sck.(*socket).deregister(id)
		}

	}
}

func (e *executor) executeLive(ctx context.Context, stm *sql.LiveStatement) (out []interface{}, err error) {

	if err := e.access(ctx, cnf.AuthNO); err != nil {
		return nil, err
	}

	if sck, ok := sockets.Load(e.id); ok {
		return sck.(*socket).executeLive(e, ctx, stm)
	}

	return nil, &LiveError{}

}

func (e *executor) executeKill(ctx context.Context, stm *sql.KillStatement) (out []interface{}, err error) {

	if err := e.access(ctx, cnf.AuthNO); err != nil {
		return nil, err
	}

	if sck, ok := sockets.Load(e.id); ok {
		return sck.(*socket).executeKill(e, ctx, stm)
	}

	return nil, &LiveError{}

}
