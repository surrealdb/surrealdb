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

package db

import (
	"testing"

	"github.com/surrealdb/surrealdb/sql"
	"github.com/surrealdb/surrealdb/util/data"
	. "github.com/smartystreets/goconvey/convey"
)

func TestScope(t *testing.T) {

	Convey("Select records from an array of strings", t, func() {

		setupDB(1)

		func() {

			txt := `
			USE NS test DB test;
			DEFINE NAMESPACE test;
			DEFINE DATABASE test;
			`

			res, err := Execute(permsKV(), txt, nil)
			So(err, ShouldBeNil)
			So(res, ShouldHaveLength, 3)

		}()

		func() {

			txt := `
			USE NS test DB test;
			SELECT * FROM [
				"one",
				"two",
				"tre",
			];
			`

			res, err := Execute(permsSC(), txt, nil)
			So(err, ShouldBeNil)
			So(res, ShouldHaveLength, 2)
			So(res[1].Result, ShouldHaveLength, 3)
			So(data.Consume(res[1].Result[0]).Data(), ShouldResemble, "one")
			So(data.Consume(res[1].Result[1]).Data(), ShouldResemble, "two")
			So(data.Consume(res[1].Result[2]).Data(), ShouldResemble, "tre")

		}()

	})

	Convey("Select records from an array of objects with an id key", t, func() {

		setupDB(1)

		func() {

			txt := `
			USE NS test DB test;
			DEFINE NAMESPACE test;
			DEFINE DATABASE test;
			`

			res, err := Execute(permsKV(), txt, nil)
			So(err, ShouldBeNil)
			So(res, ShouldHaveLength, 3)

		}()

		func() {

			txt := `
			USE NS test DB test;
			SELECT * FROM [
				{ id: "one" },
				{ id: "two" },
				{ id: "tre" },
			];
			`

			res, err := Execute(permsSC(), txt, nil)
			So(err, ShouldBeNil)
			So(res, ShouldHaveLength, 2)
			So(res[1].Result, ShouldHaveLength, 3)
			So(data.Consume(res[1].Result[0]).Data(), ShouldResemble, map[string]interface{}{"id": "one"})
			So(data.Consume(res[1].Result[1]).Data(), ShouldResemble, map[string]interface{}{"id": "two"})
			So(data.Consume(res[1].Result[2]).Data(), ShouldResemble, map[string]interface{}{"id": "tre"})

		}()

	})

	Convey("Select records from an array of objects with no id key", t, func() {

		setupDB(1)

		func() {

			txt := `
			USE NS test DB test;
			DEFINE NAMESPACE test;
			DEFINE DATABASE test;
			`

			res, err := Execute(permsKV(), txt, nil)
			So(err, ShouldBeNil)
			So(res, ShouldHaveLength, 3)

		}()

		func() {

			txt := `
			USE NS test DB test;
			SELECT * FROM [
				{ test: "one" },
				{ test: "two" },
				{ test: "tre" },
			];
			`

			res, err := Execute(permsSC(), txt, nil)
			So(err, ShouldBeNil)
			So(res, ShouldHaveLength, 2)
			So(res[1].Result, ShouldHaveLength, 3)
			So(data.Consume(res[1].Result[0]).Data(), ShouldResemble, map[string]interface{}{"test": "one"})
			So(data.Consume(res[1].Result[1]).Data(), ShouldResemble, map[string]interface{}{"test": "two"})
			So(data.Consume(res[1].Result[2]).Data(), ShouldResemble, map[string]interface{}{"test": "tre"})

		}()

	})

	Convey("Select records from an array of virtual record things with no permissions", t, func() {

		setupDB(1)

		func() {

			txt := `
			USE NS test DB test;
			DEFINE NAMESPACE test;
			DEFINE DATABASE test;
			CREATE test:one, test:two, test:tre;
			`

			res, err := Execute(permsKV(), txt, nil)
			So(err, ShouldBeNil)
			So(res, ShouldHaveLength, 4)

		}()

		func() {

			txt := `
			USE NS test DB test;
			SELECT * FROM array(
				thing("test", "one"),
				thing("test", "two"),
				thing("test", "tre")
			);
			`

			res, err := Execute(permsSC(), txt, nil)
			So(err, ShouldBeNil)
			So(res, ShouldHaveLength, 2)
			So(res[1].Result, ShouldHaveLength, 0)

		}()

	})

	Convey("Select records from an array of virtual record things with full permissions", t, func() {

		setupDB(1)

		func() {

			txt := `
			USE NS test DB test;
			DEFINE NAMESPACE test;
			DEFINE DATABASE test;
			DEFINE TABLE test PERMISSIONS FULL;
			CREATE test:one, test:two, test:tre;
			`

			res, err := Execute(permsKV(), txt, nil)
			So(err, ShouldBeNil)
			So(res, ShouldHaveLength, 5)

		}()

		func() {

			txt := `
			USE NS test DB test;
			SELECT * FROM array(
				thing("test", "one"),
				thing("test", "two"),
				thing("test", "tre")
			);
			`

			res, err := Execute(permsSC(), txt, nil)
			So(err, ShouldBeNil)
			So(res, ShouldHaveLength, 2)
			So(res[1].Result, ShouldHaveLength, 3)
			So(data.Consume(res[1].Result[0]).Data(), ShouldResemble, map[string]interface{}{
				"id": sql.NewThing("test", "one"),
				"meta": map[string]interface{}{
					"tb": "test",
					"id": "one",
				},
			})
			So(data.Consume(res[1].Result[1]).Data(), ShouldResemble, map[string]interface{}{
				"id": sql.NewThing("test", "two"),
				"meta": map[string]interface{}{
					"tb": "test",
					"id": "two",
				},
			})
			So(data.Consume(res[1].Result[2]).Data(), ShouldResemble, map[string]interface{}{
				"id": sql.NewThing("test", "tre"),
				"meta": map[string]interface{}{
					"tb": "test",
					"id": "tre",
				},
			})

		}()

	})

}
