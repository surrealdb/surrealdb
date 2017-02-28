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

	"github.com/abcum/fibre"
	"github.com/abcum/surreal/kvs"
	"github.com/abcum/surreal/mem"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
)

var executors = sync.Pool{
	New: func() interface{} {
		return &executor{}
	},
}

type executor struct {
	txn    kvs.TX
	ctx    *data.Doc
	ast    *sql.Query
	mem    *mem.Store
	web    *fibre.Context
	closed chan struct{}
}

func newExec(ast *sql.Query, web *fibre.Context, vars map[string]interface{}) (e *executor) {
	e = executors.Get().(*executor)
	e.ast = ast
	e.web = web
	e.ctx = data.Consume(vars)
	e.closed = make(chan struct{}, 1)
	return
}

func (e *executor) done() {
	executors.Put(e)
}

func (e *executor) set(key string, val interface{}) {
	e.ctx.Set(val, key)
}

func (e *executor) get(key string) (val interface{}) {
	return e.ctx.Get(key).Data()
}
