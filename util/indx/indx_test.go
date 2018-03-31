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

package indx

import (
	"testing"

	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"

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
		old, now = Diff(old, now)
		So(old, ShouldHaveLength, 0)
		So(now, ShouldHaveLength, 0)
	})

	Convey("Perform diff of different arrays", t, func() {
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
		old, now = Diff(old, now)
		So(old, ShouldHaveLength, 2)
		So(now, ShouldHaveLength, 2)
	})

	Convey("Perform diff of same and different arrays", t, func() {
		old := [][]interface{}{
			{"one", "two"},
			{"two", "tre"},
		}
		now := [][]interface{}{
			{"two", "tre"},
			{"two", "tre"},
		}
		So(old, ShouldHaveLength, 2)
		So(now, ShouldHaveLength, 2)
		old, now = Diff(old, now)
		So(old, ShouldHaveLength, 1)
		So(now, ShouldHaveLength, 1)
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

}
