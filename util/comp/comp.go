// Copyright © 2016 SurrealDB Ltd.
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

package comp

import (
	"sort"
	"strings"
	"time"

	"golang.org/x/text/collate"

	"github.com/surrealdb/surrealdb/sql"
)

func Comp(a, b interface{}, expr *sql.Order) int {

	switch x := a.(type) {

	case nil:

		switch b.(type) {
		case nil:
			return 0
		}

	case bool:

		switch y := b.(type) {
		case nil:
			return 1
		case bool:
			if x == y {
				return 0
			} else if !x && y {
				return -1
			} else if x && !y {
				return +1
			}
		}

	case int64:

		switch y := b.(type) {
		case nil, bool:
			return 1
		case int64:
			if x == y {
				return 0
			} else if x < y {
				return -1
			} else if x > y {
				return +1
			}
		case float64:
			f := float64(x)
			if f == y {
				return 0
			} else if f < y {
				return -1
			} else if f > y {
				return +1
			}
		}

	case float64:

		switch y := b.(type) {
		case nil, bool:
			return 1
		case int64:
			f := float64(y)
			if x == f {
				return 0
			} else if x < f {
				return -1
			} else if x > f {
				return +1
			}
		case float64:
			if x == y {
				return 0
			} else if x < y {
				return -1
			} else if x > y {
				return +1
			}
		}

	case time.Time:

		switch y := b.(type) {
		case nil, bool, int64, float64:
			return 1
		case time.Time:
			t1 := x.UTC().UnixNano()
			t2 := y.UTC().UnixNano()
			if t1 == t2 {
				return 0
			} else if t1 < t2 {
				return -1
			} else if t1 > t2 {
				return +1
			}
		}

	case string:

		switch y := b.(type) {
		case nil, bool, int64, float64, time.Time:
			return 1
		case string:
			if expr.Tag.IsRoot() {
				return strings.Compare(x, y)
			} else {
				c := collate.New(
					expr.Tag,
					collate.Loose,
					collate.Force,
					collate.OptionsFromTag(expr.Tag),
				)
				return c.CompareString(x, y)
			}
		}

	case *sql.Thing:

		switch y := b.(type) {
		case nil, bool, int64, float64, time.Time, string:
			return 1
		case *sql.Thing:
			if c := strings.Compare(x.TB, y.TB); c == 0 {
				return Comp(x.ID, y.ID, expr)
			} else {
				return c
			}
		}

	case []interface{}:

		switch y := b.(type) {
		case nil, bool, int64, float64, time.Time, string, *sql.Thing:
			return 1
		case []interface{}:

			for i := 0; i < len(x) && i < len(y); i++ {
				if c := Comp(x[i], y[i], expr); c != 0 {
					return c
				}
			}

			return len(x) - len(y)

		}

	case map[string]interface{}:

		switch y := b.(type) {
		case nil, bool, int64, float64, time.Time, string, *sql.Thing, []interface{}:
			return 1
		case map[string]interface{}:

			var ke = make([]string, 0)
			var me = make(map[string]bool)

			for k := range x {
				if !me[k] {
					me[k] = true
					ke = append(ke, k)
				}
			}
			for k := range y {
				if !me[k] {
					me[k] = true
					ke = append(ke, k)
				}
			}

			sort.Strings(ke)

			for i := 0; i < len(x) && i < len(y); i++ {
				k := ke[i]
				if x[k] != nil && y[k] == nil {
					return -1
				}
				if x[k] == nil && y[k] != nil {
					return 1
				}
				if c := Comp(x[k], y[k], expr); c != 0 {
					return c
				}
			}

			return len(x) - len(y)

		}

	}

	return -1

}
