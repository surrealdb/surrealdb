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

package indx

import (
	"testing"

	"github.com/surrealdb/surrealdb/sql"
	"github.com/surrealdb/surrealdb/util/data"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDiff(t *testing.T) {

	Convey("Perform diff of same arrays", t, func() {
		old := [][]interface{}{
			{"one", "two"},
			{"two", "tre"},
		}
		now := [][]interface{}{
			{"one", "two"},
			{"two", "tre"},
		}
		So(old, ShouldHaveLength, 2)
		So(now, ShouldHaveLength, 2)
		del, add := Diff(old, now)
		So(del, ShouldHaveLength, 0)
		So(del, ShouldResemble, [][]interface{}(nil))
		So(add, ShouldHaveLength, 0)
		So(add, ShouldResemble, [][]interface{}(nil))
	})

	Convey("Perform diff of reversed arrays", t, func() {
		old := [][]interface{}{
			{"one", "two"},
			{"two", "tre"},
		}
		now := [][]interface{}{
			{"two", "tre"},
			{"one", "two"},
		}
		So(old, ShouldHaveLength, 2)
		So(now, ShouldHaveLength, 2)
		del, add := Diff(old, now)
		So(del, ShouldHaveLength, 0)
		So(del, ShouldResemble, [][]interface{}(nil))
		So(add, ShouldHaveLength, 0)
		So(add, ShouldResemble, [][]interface{}(nil))
	})

	Convey("Perform diff of same and different arrays", t, func() {
		old := [][]interface{}{
			{"one", "two"},
			{"two", "tre"},
		}
		now := [][]interface{}{
			{"two", "tre"},
			{"one", "tre"},
		}
		So(old, ShouldHaveLength, 2)
		So(now, ShouldHaveLength, 2)
		del, add := Diff(old, now)
		So(del, ShouldHaveLength, 1)
		So(del, ShouldResemble, [][]interface{}{{"one", "two"}})
		So(add, ShouldHaveLength, 1)
		So(add, ShouldResemble, [][]interface{}{{"one", "tre"}})
	})

	Convey("Perform diff of same and different length arrays", t, func() {
		old := [][]interface{}{
			{"one", "two"},
			{"two", "tre"},
			{"one", "tre"},
		}
		now := [][]interface{}{
			{"two", "tre"},
		}
		So(old, ShouldHaveLength, 3)
		So(now, ShouldHaveLength, 1)
		del, add := Diff(old, now)
		So(del, ShouldHaveLength, 2)
		So(del, ShouldResemble, [][]interface{}{{"one", "two"}, {"one", "tre"}})
		So(add, ShouldHaveLength, 0)
		So(add, ShouldResemble, [][]interface{}(nil))
	})

}

func TestBuild(t *testing.T) {

	Convey("Perform build with missing fields", t, func() {

		col := sql.Idents{
			sql.NewIdent("one"),
			sql.NewIdent("two"),
		}

		doc := data.Consume(map[string]interface{}{
			"tre": "TRE",
		})

		out := Build(col, doc)

		So(out, ShouldResemble, [][]interface{}{
			{nil, nil},
		})

	})

	Convey("Perform build with existing fields", t, func() {

		col := sql.Idents{
			sql.NewIdent("one"),
			sql.NewIdent("two"),
		}

		doc := data.Consume(map[string]interface{}{
			"one": "ONE",
			"two": "TWO",
		})

		out := Build(col, doc)

		So(out, ShouldResemble, [][]interface{}{
			{"ONE", "TWO"},
		})

	})

	Convey("Perform build with subfield array fields", t, func() {

		col := sql.Idents{
			sql.NewIdent("one"),
			sql.NewIdent("two"),
			sql.NewIdent("arr"),
		}

		doc := data.Consume(map[string]interface{}{
			"one": "ONE",
			"two": "TWO",
			"arr": []interface{}{1, 2, 3},
		})

		out := Build(col, doc)

		So(out, ShouldResemble, [][]interface{}{
			{"ONE", "TWO", 1},
			{"ONE", "TWO", 2},
			{"ONE", "TWO", 3},
		})

	})

	Convey("Perform build with empty array field", t, func() {

		col := sql.Idents{
			sql.NewIdent("test.*"),
		}

		doc := data.Consume(map[string]interface{}{
			"test": []interface{}{},
		})

		out := Build(col, doc)

		So(out, ShouldResemble, [][]interface{}(nil))

	})

	Convey("Perform build with non-empty array field", t, func() {

		col := sql.Idents{
			sql.NewIdent("test.*"),
		}

		doc := data.Consume(map[string]interface{}{
			"test": []interface{}{"one", "two", "tre"},
		})

		out := Build(col, doc)

		So(out, ShouldResemble, [][]interface{}{
			{"one"},
			{"two"},
			{"tre"},
		})

	})

}
