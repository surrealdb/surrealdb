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
	"io"

	"github.com/abcum/surreal/sql"
)

// Execute parses the query and executes it against the data layer
func Execute(c *echo.Context, input interface{}) (res []interface{}, err error) {

	var ast *sql.Query
	var stm interface{}

	switch input.(type) {
	case string:
		ast, err = sql.ParseString(input.(string))
	case io.Reader:
		ast, err = sql.ParseBuffer(input.(io.Reader))
	}

	if err != nil {
		return nil, err
	}

	for _, s := range ast.Statements {

		switch s.(type) {

		case *sql.SelectStatement:
			stm = executeSelectStatement(s)
		case *sql.CreateStatement:
			stm = executeCreateStatement(s)
		case *sql.UpdateStatement:
			stm = executeUpdateStatement(s)
		case *sql.ModifyStatement:
			stm = executeModifyStatement(s)
		case *sql.DeleteStatement:
			stm = executeDeleteStatement(s)
		case *sql.RelateStatement:
			stm = executeRelateStatement(s)
		case *sql.RecordStatement:
			stm = executeRecordStatement(s)

		case *sql.DefineViewStatement:
			stm = executeDefineStatement(s)
		case *sql.ResyncViewStatement:
			stm = executeResyncStatement(s)
		case *sql.RemoveViewStatement:
			stm = executeRemoveStatement(s)

		case *sql.DefineIndexStatement:
			stm = executeDefineStatement(s)
		case *sql.ResyncIndexStatement:
			stm = executeResyncStatement(s)
		case *sql.RemoveIndexStatement:
			stm = executeRemoveStatement(s)

		}

		res = append(res, stm)

	}

	return res, err

}
