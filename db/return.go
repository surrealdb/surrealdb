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

func (e *executor) executeReturn(ctx context.Context, stm *sql.ReturnStatement) (out []interface{}, err error) {

	for _, w := range stm.What {

		switch what := w.(type) {
		case *sql.Void:
			// Ignore
		case *sql.Empty:
			// Ignore
		default:
			val, err := e.fetch(ctx, what, nil)
			if err != nil {
				return nil, err
			}
			out = append(out, val)
		}

	}

	return

}
