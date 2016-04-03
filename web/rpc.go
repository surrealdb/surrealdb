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

package web

import (
	"github.com/abcum/fibre"
	"github.com/abcum/surreal/db"
)

type rpc struct{}

func (r *rpc) Sql(c *fibre.Context, sql string) (interface{}, error) {
	return db.Execute(c, sql)
}

func (r *rpc) List(c *fibre.Context, class string) (interface{}, error) {
	sql := db.Prepare("SELECT * FROM %v", class)
	return db.Execute(c, sql)
}

func (r *rpc) Select(c *fibre.Context, class, thing string) (interface{}, error) {
	sql := db.Prepare("SELECT * FROM @%v:%v", class, thing)
	return db.Execute(c, sql)
}

func (r *rpc) Create(c *fibre.Context, class, thing, data string) (interface{}, error) {
	sql := db.Prepare("CREATE @%v:%v CONTENT %v RETURN AFTER", class, thing, data)
	return db.Execute(c, sql)
}

func (r *rpc) Update(c *fibre.Context, class, thing, data string) (interface{}, error) {
	sql := db.Prepare("UPDATE @%v:%v CONTENT %v RETURN AFTER", class, thing, data)
	return db.Execute(c, sql)
}

func (r *rpc) Modify(c *fibre.Context, class, thing, data string) (interface{}, error) {
	sql := db.Prepare("MODIFY @%v:%v DIFF %v RETURN AFTER", class, thing, data)
	return db.Execute(c, sql)
}

func (r *rpc) Delete(c *fibre.Context, class, thing string) (interface{}, error) {
	sql := db.Prepare("DELETE @%v:%v", class, thing)
	return db.Execute(c, sql)
}
