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

	"github.com/abcum/surreal/sql"
)

func (e *executor) executeReturnStatement(ctx context.Context, ast *sql.ReturnStatement) (out []interface{}, err error) {

	switch what := ast.What.(type) {
	default:
		out = append(out, what)
	case *sql.Void:
		// Ignore
	case *sql.Empty:
		// Ignore
	case *sql.Null: // Specifically asked for null
		out = append(out, nil)
	case *sql.Ident: // Return does not have columns.
		out = append(out, nil)
	case *sql.Value: // Specifically asked for a string.
		out = append(out, what.ID)
	case *sql.Param: // Let's get the value of the param.
		out = append(out, e.get(what.ID))
	}

	return

}
