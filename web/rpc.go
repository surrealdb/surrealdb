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
	"github.com/abcum/surreal/sql"
)

type rpc struct{}

func (r *rpc) Sql(c *fibre.Context, sql string, vars map[string]interface{}) (interface{}, error) {
	return db.Execute(c, sql, vars)
}

func (r *rpc) Select(c *fibre.Context, class string, thing interface{}) (interface{}, error) {
	switch thing.(type) {
	case *fibre.RPCNull:
		return db.Execute(c, "SELECT * FROM $class", map[string]interface{}{
			"class": sql.NewTable(class),
		})
	default:
		return db.Execute(c, "SELECT * FROM $thing", map[string]interface{}{
			"thing": sql.NewThing(class, thing),
		})
	}
}

func (r *rpc) Create(c *fibre.Context, class string, thing interface{}, data map[string]interface{}) (interface{}, error) {
	switch thing.(type) {
	case *fibre.RPCNull:
		return db.Execute(c, "CREATE $class CONTENT $data RETURN AFTER", map[string]interface{}{
			"class": sql.NewTable(class),
			"data":  data,
		})
	default:
		return db.Execute(c, "CREATE $thing CONTENT $data RETURN AFTER", map[string]interface{}{
			"thing": sql.NewThing(class, thing),
			"data":  data,
		})
	}
}

func (r *rpc) Update(c *fibre.Context, class string, thing interface{}, data map[string]interface{}) (interface{}, error) {
	switch thing.(type) {
	case *fibre.RPCNull:
		return db.Execute(c, "UPDATE $class CONTENT $data RETURN AFTER", map[string]interface{}{
			"class": sql.NewTable(class),
			"data":  data,
		})
	default:
		return db.Execute(c, "UPDATE $thing CONTENT $data RETURN AFTER", map[string]interface{}{
			"thing": sql.NewThing(class, thing),
			"data":  data,
		})
	}
}

func (r *rpc) Modify(c *fibre.Context, class string, thing interface{}, data map[string]interface{}) (interface{}, error) {
	switch thing.(type) {
	case *fibre.RPCNull:
		return db.Execute(c, "UPDATE $class DIFF $data RETURN AFTER", map[string]interface{}{
			"class": sql.NewTable(class),
			"data":  data,
		})
	default:
		return db.Execute(c, "UPDATE $thing DIFF $data RETURN AFTER", map[string]interface{}{
			"thing": sql.NewThing(class, thing),
			"data":  data,
		})
	}
}

func (r *rpc) Delete(c *fibre.Context, class string, thing interface{}) (interface{}, error) {
	switch thing.(type) {
	case *fibre.RPCNull:
		return db.Execute(c, "DELETE $class", map[string]interface{}{
			"class": sql.NewTable(class),
		})
	default:
		return db.Execute(c, "DELETE $thing", map[string]interface{}{
			"thing": sql.NewThing(class, thing),
		})
	}
}
