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
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDelete(t *testing.T) {

	Convey("Delete with invalid value", t, func() {

		setupDB(1)

		txt := `
		USE NS test DB test;
		DELETE 1;
		DELETE "one";
		DELETE ["many"];
		DELETE [{value:"one"}];
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[1].Status, ShouldEqual, "ERR")
		So(res[1].Detail, ShouldEqual, "Can not execute DELETE query using value '1'")
		So(res[2].Status, ShouldEqual, "ERR")
		So(res[2].Detail, ShouldEqual, "Can not execute DELETE query using value 'one'")
		So(res[3].Status, ShouldEqual, "ERR")
		So(res[3].Detail, ShouldEqual, "Can not execute DELETE query using value '[many]'")
		So(res[4].Status, ShouldEqual, "ERR")
		So(res[4].Detail, ShouldEqual, "Can not execute DELETE query using value '[map[value:one]]'")

	})

	Convey("Delete records using `table`", t, func() {

		setupDB(1)

		txt := `
		USE NS test DB test;
		CREATE |person:10|;
		SELECT * FROM person;
		DELETE person;
		SELECT * FROM person;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[1].Result, ShouldHaveLength, 10)
		So(res[2].Result, ShouldHaveLength, 10)
		So(res[4].Result, ShouldHaveLength, 0)

	})

	Convey("Delete specific record using `thing`", t, func() {

		setupDB(1)

		txt := `
		USE NS test DB test;
		CREATE person:test;
		SELECT * FROM person;
		DELETE person:test;
		SELECT * FROM person;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[4].Result, ShouldHaveLength, 0)

	})

	Convey("Delete unique records using `batch`", t, func() {

		setupDB(1)

		txt := `
		USE NS test DB test;
		CREATE |person:1..10|;
		SELECT * FROM person;
		DELETE batch("person", ["1", "2", "person:3"]);
		SELECT * FROM person;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[1].Result, ShouldHaveLength, 10)
		So(res[2].Result, ShouldHaveLength, 10)
		So(res[3].Result, ShouldHaveLength, 0)
		So(res[4].Result, ShouldHaveLength, 7)

	})

	Convey("Delete unique records using `model`", t, func() {

		setupDB(1)

		txt := `
		USE NS test DB test;
		CREATE |person:1..10|;
		SELECT * FROM person;
		DELETE |person:1..5|;
		SELECT * FROM person;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[1].Result, ShouldHaveLength, 10)
		So(res[2].Result, ShouldHaveLength, 10)
		So(res[3].Result, ShouldHaveLength, 0)
		So(res[4].Result, ShouldHaveLength, 5)

	})

	Convey("Deleting with a timeout of 1ns returns an error", t, func() {

		setupDB(1)

		txt := `
		USE NS test DB test;
		DELETE |person:1..1000| TIMEOUT 1ns;
		SELECT * FROM person;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[1].Result, ShouldHaveLength, 0)
		So(res[2].Result, ShouldHaveLength, 0)
		So(res[1].Status, ShouldEqual, "ERR_TO")
		So(res[1].Detail, ShouldEqual, "Query timeout of 1ns exceeded")

	})

}
