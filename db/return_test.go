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

func TestReturn(t *testing.T) {

	Convey("Return a string", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		RETURN "test";
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[1].Result, ShouldResemble, []interface{}{"test"})

	})

	Convey("Return a number", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		RETURN 33693;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[1].Result, ShouldResemble, []interface{}{33693.0})

	})

	Convey("Return a VOID expression", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		RETURN VOID;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[1].Result, ShouldResemble, []interface{}{})

	})

	Convey("Return an EMPTY expression", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		RETURN EMPTY;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[1].Result, ShouldResemble, []interface{}{})

	})

}
