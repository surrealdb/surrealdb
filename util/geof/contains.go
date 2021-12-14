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

package geof

import (
	"github.com/surrealdb/surrealdb/sql"
)

func Contains(a *sql.Polygon, b *sql.Point) bool {
	beg := len(a.PS) - 1
	end := 0
	contains := raycast(b, a.PS[beg], a.PS[end])
	for i := 1; i < len(a.PS); i++ {
		if raycast(b, a.PS[i-1], a.PS[i]) {
			contains = !contains
		}
	}
	return contains
}
