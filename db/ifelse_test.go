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

func TestIf(t *testing.T) {

	Convey("If statement which runs if clause", t, func() {

		setupDB(1)

		txt := `
		USE NS test DB test;
		CREATE |person:1..3|;
		LET temp = 13.753;
		IF $temp THEN
			(SELECT * FROM person:1)
		ELSE
			(SELECT * FROM person:3)
		END;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[2].Status, ShouldEqual, "OK")
		So(res[3].Status, ShouldEqual, "OK")
		So(data.Consume(res[3].Result[0]).Get("meta.id").Data(), ShouldEqual, 1)

	})

	Convey("If statement which runs if clause", t, func() {

		setupDB(1)

		txt := `
		USE NS test DB test;
		CREATE |person:1..3|;
		LET temp = 13.753;
		IF $temp > 10 THEN
			(SELECT * FROM person:1)
		ELSE IF $temp > 5 THEN
			(SELECT * FROM person:2)
		ELSE
			(SELECT * FROM person:3)
		END;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[2].Status, ShouldEqual, "OK")
		So(res[3].Status, ShouldEqual, "OK")
		So(data.Consume(res[3].Result[0]).Get("meta.id").Data(), ShouldEqual, 1)

	})

	Convey("If statement which runs elif clause", t, func() {

		setupDB(1)

		txt := `
		USE NS test DB test;
		CREATE |person:1..3|;
		LET temp = 7.1374;
		IF $temp > 10 THEN
			(SELECT * FROM person:1)
		ELSE IF $temp > 5 THEN
			(SELECT * FROM person:2)
		ELSE
			(SELECT * FROM person:3)
		END;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[2].Status, ShouldEqual, "OK")
		So(res[3].Status, ShouldEqual, "OK")
		So(data.Consume(res[3].Result[0]).Get("meta.id").Data(), ShouldEqual, 2)

	})

	Convey("If statement which runs else clause", t, func() {

		setupDB(1)

		txt := `
		USE NS test DB test;
		CREATE |person:1..3|;
		LET temp = true;
		IF $temp > 10 THEN
			(SELECT * FROM person:1)
		ELSE IF $temp > 5 THEN
			(SELECT * FROM person:2)
		ELSE
			(SELECT * FROM person:3)
		END;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[2].Status, ShouldEqual, "OK")
		So(res[3].Status, ShouldEqual, "OK")
		So(data.Consume(res[3].Result[0]).Get("meta.id").Data(), ShouldEqual, 3)

	})

}
