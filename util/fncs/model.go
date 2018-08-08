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

package fncs

import (
	"context"

	"github.com/abcum/surreal/sql"
)

func model(ctx context.Context, args ...interface{}) (interface{}, error) {
	tb, _ := ensureString(args[0])
	switch len(args) {
	case 2:
		if max, ok := ensureFloat(args[1]); ok {
			return sql.NewModel(tb, 0, 0, max), nil
		}
	case 3:
		if min, ok := ensureFloat(args[1]); ok {
			if max, ok := ensureFloat(args[2]); ok {
				return sql.NewModel(tb, min, 1, max), nil
			}
		}
	case 4:
		if min, ok := ensureFloat(args[1]); ok {
			if inc, ok := ensureFloat(args[2]); ok {
				if max, ok := ensureFloat(args[3]); ok {
					return sql.NewModel(tb, min, inc, max), nil
				}
			}
		}
	}
	return nil, nil
}
