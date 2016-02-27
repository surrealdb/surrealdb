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

func Execute(query *sql.Query, err error) (interface{}, error) {

	if err != nil {
		return nil, err
	}

	for _, s := range ast.Statements {

		switch s.(type) {

		case *sql.SelectStatement:
			return executeSelectStatement(s), err
		case *sql.CreateStatement:
			return executeCreateStatement(s), err
		case *sql.UpdateStatement:
			return executeUpdateStatement(s), err
		case *sql.ModifyStatement:
			return executeModifyStatement(s), err
		case *sql.DeleteStatement:
			return executeDeleteStatement(s), err
		case *sql.RelateStatement:
			return executeRelateStatement(s), err
		case *sql.RecordStatement:
			return executeRecordStatement(s), err

		case *sql.DefineViewStatement:
			return executeDefineStatement(s), err
		case *sql.ResyncViewStatement:
			return executeResyncStatement(s), err
		case *sql.RemoveViewStatement:
			return executeRemoveStatement(s), err

		case *sql.DefineIndexStatement:
			return executeDefineStatement(s), err
		case *sql.ResyncIndexStatement:
			return executeResyncStatement(s), err
		case *sql.RemoveIndexStatement:
			return executeRemoveStatement(s), err

		}

	}

	return query, err

}

func ExecuteString(input string) (interface{}, error) {
	return Execute(sql.Parse(input))
}

func ExecuteBuffer(input io.Reader) (interface{}, error) {
	return Execute(sql.NewParser(input).Parse())
}
