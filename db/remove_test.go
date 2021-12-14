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

	"github.com/surrealdb/surrealdb/util/data"
	. "github.com/smartystreets/goconvey/convey"
)

func TestRemove(t *testing.T) {

	Convey("Remove a namespace", t, func() {

		setupDB(workerCount)

		txt := `
		USE NS test DB test;
		CREATE |person:10|;
		REMOVE NAMESPACE test;
		SELECT * FROM person;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[2].Status, ShouldEqual, "OK")
		So(res[3].Status, ShouldEqual, "OK")
		So(res[3].Result, ShouldHaveLength, 0)

	})

	Convey("Remove a database", t, func() {

		setupDB(workerCount)

		txt := `
		USE NS test DB test;
		CREATE |person:10|;
		REMOVE DATABASE test;
		INFO FOR NAMESPACE;
		SELECT * FROM person;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[2].Status, ShouldEqual, "OK")
		So(res[3].Status, ShouldEqual, "OK")
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("database").Data(), ShouldHaveLength, 0)
		So(res[4].Status, ShouldEqual, "OK")
		So(res[4].Result, ShouldHaveLength, 0)

	})

	Convey("Remove a table", t, func() {

		setupDB(workerCount)

		txt := `
		USE NS test DB test;
		CREATE |person:10|;
		REMOVE TABLE person;
		INFO FOR DATABASE;
		SELECT * FROM person;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[2].Status, ShouldEqual, "OK")
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("table").Data(), ShouldHaveLength, 0)
		So(res[4].Status, ShouldEqual, "OK")
		So(res[4].Result, ShouldHaveLength, 0)

	})

}
