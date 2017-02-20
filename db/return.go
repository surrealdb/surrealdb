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
	"github.com/abcum/surreal/log"
	"github.com/abcum/surreal/sql"
)

func (e *executor) executeReturnStatement(ast *sql.ReturnStatement) (out []interface{}, err error) {

	log.WithPrefix("sql").WithFields(map[string]interface{}{
		"ns": ast.NS,
		"db": ast.DB,
	}).Debugln(ast)

	switch what := ast.What.(type) {
	default:
		out = append(out, what)
	case *sql.Param:
		out = append(out, e.ctx.Get(what.ID).Data())
	}

	return

}
