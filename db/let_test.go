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

	. "github.com/smartystreets/goconvey/convey"
)

func TestLet(t *testing.T) {

	Convey("Let to create a new variable", t, func() {

		setupDB(1)

		txt := `
		USE NS test DB test;
		LET temp = "test";
		RETURN $temp;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[2].Status, ShouldEqual, "OK")
		So(res[2].Result, ShouldResemble, []interface{}{"test"})

	})

	Convey("Let to create and VOID a variable", t, func() {

		setupDB(1)

		txt := `
		USE NS test DB test;
		LET temp = "test";
		LET temp = VOID;
		RETURN $temp;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[2].Status, ShouldEqual, "OK")
		So(res[3].Status, ShouldEqual, "OK")
		So(res[3].Result, ShouldResemble, []interface{}{nil})

	})

	Convey("Let to create and EMPTY a variable", t, func() {

		setupDB(1)

		txt := `
		USE NS test DB test;
		LET temp = "test";
		LET temp = EMPTY;
		RETURN $temp;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[2].Status, ShouldEqual, "OK")
		So(res[3].Status, ShouldEqual, "OK")
		So(res[3].Result, ShouldResemble, []interface{}{nil})

	})

}
