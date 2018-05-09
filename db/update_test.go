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
	"time"

	"testing"

	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
	. "github.com/smartystreets/goconvey/convey"
)

func TestUpdate(t *testing.T) {

	Convey("Update with invalid value", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		UPDATE 1;
		UPDATE "one";
		UPDATE ["many"];
		UPDATE [{value:"one"}];
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[1].Status, ShouldEqual, "ERR")
		So(res[1].Detail, ShouldEqual, "Can not execute UPDATE query using value '1'")
		So(res[2].Status, ShouldEqual, "ERR")
		So(res[2].Detail, ShouldEqual, "Can not execute UPDATE query using value 'one'")
		So(res[3].Status, ShouldEqual, "ERR")
		So(res[3].Detail, ShouldEqual, "Can not execute UPDATE query using value '[many]'")
		So(res[4].Status, ShouldEqual, "ERR")
		So(res[4].Detail, ShouldEqual, "Can not execute UPDATE query using value '[map[value:one]]'")

	})

	Convey("Update record when it exists", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		UPDATE person:test;
		UPDATE person:test;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)

	})

	Convey("Update unique record using `table`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		UPDATE person, table("person");
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Result, ShouldHaveLength, 0)

	})

	Convey("Update specific record using `thing`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		UPDATE person:test, thing("person", "test");
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Result, ShouldHaveLength, 2)
		So(data.Consume(res[1].Result[0]).Get("meta.id").Data(), ShouldHaveLength, 4)
		So(data.Consume(res[1].Result[0]).Get("meta.tb").Data(), ShouldEqual, "person")
		So(data.Consume(res[1].Result[1]).Get("meta.id").Data(), ShouldHaveLength, 4)
		So(data.Consume(res[1].Result[1]).Get("meta.tb").Data(), ShouldEqual, "person")

	})

	Convey("Update unique records using `batch`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		UPDATE batch("person", ["1", "2", "person:3"]);
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Result, ShouldHaveLength, 3)
		So(data.Consume(res[1].Result[0]).Get("meta.id").Data(), ShouldEqual, 1)
		So(data.Consume(res[1].Result[2]).Get("meta.id").Data(), ShouldEqual, 3)

	})

	Convey("Update unique records using `model`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		UPDATE |person:100|;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Result, ShouldHaveLength, 100)
		So(data.Consume(res[1].Result[0]).Get("meta.id").Data(), ShouldHaveLength, 20)
		So(data.Consume(res[1].Result[99]).Get("meta.id").Data(), ShouldHaveLength, 20)

	})

	Convey("Update sequential ascending records using `model`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		UPDATE |person:1..100|;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Result, ShouldHaveLength, 100)
		So(data.Consume(res[1].Result[0]).Get("meta.id").Data(), ShouldEqual, 1)
		So(data.Consume(res[1].Result[99]).Get("meta.id").Data(), ShouldEqual, 100)

	})

	Convey("Update sequential descending records using `model`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		UPDATE |person:100..1|;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Result, ShouldHaveLength, 100)
		So(data.Consume(res[1].Result[0]).Get("meta.id").Data(), ShouldEqual, 100)
		So(data.Consume(res[1].Result[99]).Get("meta.id").Data(), ShouldEqual, 1)

	})

	Convey("Update sequential ascending negative-to-positive records using `model`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		UPDATE |person:-50..50|;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Result, ShouldHaveLength, 101)
		So(data.Consume(res[1].Result[0]).Get("meta.id").Data(), ShouldEqual, -50)
		So(data.Consume(res[1].Result[1]).Get("meta.id").Data(), ShouldEqual, -49)
		So(data.Consume(res[1].Result[100]).Get("meta.id").Data(), ShouldEqual, 50)

	})

	Convey("Update sequential ascending decimal records using `model`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		UPDATE |person:1,0.5..50|;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Result, ShouldHaveLength, 99)
		So(data.Consume(res[1].Result[0]).Get("meta.id").Data(), ShouldEqual, 1)
		So(data.Consume(res[1].Result[1]).Get("meta.id").Data(), ShouldEqual, 1.5)
		So(data.Consume(res[1].Result[98]).Get("meta.id").Data(), ShouldEqual, 50)

	})

	Convey("Update sequential descending decimal records using `model`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		UPDATE |person:50,0.5..1|;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Result, ShouldHaveLength, 99)
		So(data.Consume(res[1].Result[0]).Get("meta.id").Data(), ShouldEqual, 50)
		So(data.Consume(res[1].Result[1]).Get("meta.id").Data(), ShouldEqual, 49.5)
		So(data.Consume(res[1].Result[98]).Get("meta.id").Data(), ShouldEqual, 1)

	})

	Convey("Update sequential ascending decimal negative-to-positive records using `model`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		UPDATE |person:-50,0.5..50|;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Result, ShouldHaveLength, 201)
		So(data.Consume(res[1].Result[0]).Get("meta.id").Data(), ShouldEqual, -50)
		So(data.Consume(res[1].Result[1]).Get("meta.id").Data(), ShouldEqual, -49.5)
		So(data.Consume(res[1].Result[200]).Get("meta.id").Data(), ShouldEqual, 50)

	})

	Convey("Parsing same ID using ints, floats, and strings", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		UPDATE person:1;
		UPDATE person:1.0;
		UPDATE person:⟨1⟩;
		UPDATE person:⟨1.0⟩;
		UPDATE person:⟨1.0000⟩;
		SELECT name FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 7)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(res[4].Result, ShouldHaveLength, 1)
		So(res[5].Result, ShouldHaveLength, 1)
		So(res[6].Result, ShouldHaveLength, 1)

	})

	Convey("Updating with a timeout of 1ms returns an error", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		UPDATE |person:1..1000| TIMEOUT 1ms;
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[1].Result, ShouldHaveLength, 0)
		So(res[2].Result, ShouldHaveLength, 0)
		So(res[1].Status, ShouldEqual, "ERR_TO")
		So(res[1].Detail, ShouldEqual, "Query timeout of 1ms exceeded")

	})

	Convey("Update a record using CONTENT", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:test SET test="text";
		UPDATE person:test CONTENT {"other":true};
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Result, ShouldHaveLength, 1)
		So(data.Consume(res[1].Result[0]).Get("test").Data(), ShouldEqual, "text")
		So(data.Consume(res[1].Result[0]).Get("other").Data(), ShouldEqual, nil)
		So(res[2].Result, ShouldHaveLength, 1)
		So(data.Consume(res[2].Result[0]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[2].Result[0]).Get("other").Data(), ShouldEqual, true)
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[3].Result[0]).Get("other").Data(), ShouldEqual, true)

	})

	Convey("Update records using CONTENT", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1 SET test="one";
		CREATE person:2 SET test="two";
		CREATE person:3 SET test="tre";
		UPDATE person CONTENT {"other":true};
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 6)
		So(res[1].Result, ShouldHaveLength, 1)
		So(data.Consume(res[1].Result[0]).Get("test").Data(), ShouldEqual, "one")
		So(data.Consume(res[1].Result[0]).Get("other").Data(), ShouldEqual, nil)
		So(res[2].Result, ShouldHaveLength, 1)
		So(data.Consume(res[2].Result[0]).Get("test").Data(), ShouldEqual, "two")
		So(data.Consume(res[2].Result[0]).Get("other").Data(), ShouldEqual, nil)
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldEqual, "tre")
		So(data.Consume(res[3].Result[0]).Get("other").Data(), ShouldEqual, nil)
		So(res[4].Result, ShouldHaveLength, 3)
		So(data.Consume(res[4].Result[0]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[4].Result[0]).Get("other").Data(), ShouldEqual, true)
		So(data.Consume(res[4].Result[1]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[4].Result[1]).Get("other").Data(), ShouldEqual, true)
		So(data.Consume(res[4].Result[2]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[4].Result[2]).Get("other").Data(), ShouldEqual, true)
		So(res[5].Result, ShouldHaveLength, 3)
		So(data.Consume(res[5].Result[0]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[5].Result[0]).Get("other").Data(), ShouldEqual, true)
		So(data.Consume(res[5].Result[1]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[5].Result[1]).Get("other").Data(), ShouldEqual, true)
		So(data.Consume(res[5].Result[2]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[5].Result[2]).Get("other").Data(), ShouldEqual, true)

	})

	Convey("Update a record using CONTENT stored in a $param", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		Let data = {"other":true};
		CREATE person:test SET test="text";
		UPDATE person:test CONTENT $data;
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[2].Result, ShouldHaveLength, 1)
		So(data.Consume(res[2].Result[0]).Get("test").Data(), ShouldEqual, "text")
		So(data.Consume(res[2].Result[0]).Get("other").Data(), ShouldEqual, nil)
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[3].Result[0]).Get("other").Data(), ShouldEqual, true)
		So(res[4].Result, ShouldHaveLength, 1)
		So(data.Consume(res[4].Result[0]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[4].Result[0]).Get("other").Data(), ShouldEqual, true)

	})

	Convey("Update a record using MERGE", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:test SET test="text";
		UPDATE person:test MERGE {"other":true};
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Result, ShouldHaveLength, 1)
		So(data.Consume(res[1].Result[0]).Get("test").Data(), ShouldEqual, "text")
		So(data.Consume(res[1].Result[0]).Get("other").Data(), ShouldEqual, nil)
		So(res[2].Result, ShouldHaveLength, 1)
		So(data.Consume(res[2].Result[0]).Get("test").Data(), ShouldEqual, "text")
		So(data.Consume(res[2].Result[0]).Get("other").Data(), ShouldEqual, true)
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldEqual, "text")
		So(data.Consume(res[3].Result[0]).Get("other").Data(), ShouldEqual, true)

	})

	Convey("Update records using MERGE", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1 SET test="one";
		CREATE person:2 SET test="two";
		CREATE person:3 SET test="tre";
		UPDATE person MERGE {"other":true};
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 6)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(res[4].Result, ShouldHaveLength, 3)
		So(data.Consume(res[4].Result[0]).Get("test").Data(), ShouldEqual, "one")
		So(data.Consume(res[4].Result[0]).Get("other").Data(), ShouldEqual, true)
		So(data.Consume(res[4].Result[1]).Get("test").Data(), ShouldEqual, "two")
		So(data.Consume(res[4].Result[1]).Get("other").Data(), ShouldEqual, true)
		So(data.Consume(res[4].Result[2]).Get("test").Data(), ShouldEqual, "tre")
		So(data.Consume(res[4].Result[2]).Get("other").Data(), ShouldEqual, true)
		So(res[5].Result, ShouldHaveLength, 3)
		So(data.Consume(res[5].Result[0]).Get("test").Data(), ShouldEqual, "one")
		So(data.Consume(res[5].Result[0]).Get("other").Data(), ShouldEqual, true)
		So(data.Consume(res[5].Result[1]).Get("test").Data(), ShouldEqual, "two")
		So(data.Consume(res[5].Result[1]).Get("other").Data(), ShouldEqual, true)
		So(data.Consume(res[5].Result[2]).Get("test").Data(), ShouldEqual, "tre")
		So(data.Consume(res[5].Result[2]).Get("other").Data(), ShouldEqual, true)

	})

	Convey("Update a record using MERGE stored in a $param", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		LET data = {"other":true};
		CREATE person:test SET test="text";
		UPDATE person:test MERGE $data;
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[2].Result, ShouldHaveLength, 1)
		So(data.Consume(res[2].Result[0]).Get("test").Data(), ShouldEqual, "text")
		So(data.Consume(res[2].Result[0]).Get("other").Data(), ShouldEqual, nil)
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldEqual, "text")
		So(data.Consume(res[3].Result[0]).Get("other").Data(), ShouldEqual, true)
		So(res[4].Result, ShouldHaveLength, 1)
		So(data.Consume(res[4].Result[0]).Get("test").Data(), ShouldEqual, "text")
		So(data.Consume(res[4].Result[0]).Get("other").Data(), ShouldEqual, true)

	})

	Convey("Update a record using DIFF", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:test SET test="text";
		UPDATE person:test DIFF [{"op":"add","path":"/other","value":true}];
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Result, ShouldHaveLength, 1)
		So(data.Consume(res[1].Result[0]).Get("test").Data(), ShouldEqual, "text")
		So(data.Consume(res[1].Result[0]).Get("other").Data(), ShouldEqual, nil)
		So(res[2].Result, ShouldHaveLength, 1)
		So(data.Consume(res[2].Result[0]).Get("test").Data(), ShouldEqual, "text")
		So(data.Consume(res[2].Result[0]).Get("other").Data(), ShouldEqual, true)
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldEqual, "text")
		So(data.Consume(res[3].Result[0]).Get("other").Data(), ShouldEqual, true)

	})

	Convey("Update a record using DIFF stored in a $param", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		LET data = [{"op":"add","path":"/other","value":true}];
		CREATE person:test SET test="text";
		UPDATE person:test DIFF $data;
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[2].Result, ShouldHaveLength, 1)
		So(data.Consume(res[2].Result[0]).Get("test").Data(), ShouldEqual, "text")
		So(data.Consume(res[2].Result[0]).Get("other").Data(), ShouldEqual, nil)
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldEqual, "text")
		So(data.Consume(res[3].Result[0]).Get("other").Data(), ShouldEqual, true)
		So(res[4].Result, ShouldHaveLength, 1)
		So(data.Consume(res[4].Result[0]).Get("test").Data(), ShouldEqual, "text")
		So(data.Consume(res[4].Result[0]).Get("other").Data(), ShouldEqual, true)

	})

	Convey("Update records using DIFF", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1 SET test="one";
		CREATE person:2 SET test="two";
		CREATE person:3 SET test="tre";
		UPDATE person DIFF [{"op":"add","path":"/other","value":true}];
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 6)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(res[4].Result, ShouldHaveLength, 3)
		So(data.Consume(res[4].Result[0]).Get("test").Data(), ShouldEqual, "one")
		So(data.Consume(res[4].Result[0]).Get("other").Data(), ShouldEqual, true)
		So(data.Consume(res[4].Result[1]).Get("test").Data(), ShouldEqual, "two")
		So(data.Consume(res[4].Result[1]).Get("other").Data(), ShouldEqual, true)
		So(data.Consume(res[4].Result[2]).Get("test").Data(), ShouldEqual, "tre")
		So(data.Consume(res[4].Result[2]).Get("other").Data(), ShouldEqual, true)
		So(res[5].Result, ShouldHaveLength, 3)
		So(data.Consume(res[5].Result[0]).Get("test").Data(), ShouldEqual, "one")
		So(data.Consume(res[5].Result[0]).Get("other").Data(), ShouldEqual, true)
		So(data.Consume(res[5].Result[1]).Get("test").Data(), ShouldEqual, "two")
		So(data.Consume(res[5].Result[1]).Get("other").Data(), ShouldEqual, true)
		So(data.Consume(res[5].Result[2]).Get("test").Data(), ShouldEqual, "tre")
		So(data.Consume(res[5].Result[2]).Get("other").Data(), ShouldEqual, true)

	})

	Convey("Update a record using NULL to unset a field", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:test SET test = true;
		UPDATE person:test SET test = NULL;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[1].Result, ShouldHaveLength, 1)
		So(data.Consume(res[1].Result[0]).Get("test").Data(), ShouldEqual, true)
		So(res[2].Result, ShouldHaveLength, 1)
		So(data.Consume(res[2].Result[0]).Get("test").Data(), ShouldEqual, nil)
		_, ok := res[2].Result[0].(map[string]interface{})["test"]
		So(ok, ShouldEqual, true)

	})

	Convey("Update a record using VOID to remove a field", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:test SET test = true;
		UPDATE person:test SET test = VOID;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[1].Result, ShouldHaveLength, 1)
		So(data.Consume(res[1].Result[0]).Get("test").Data(), ShouldEqual, true)
		So(res[2].Result, ShouldHaveLength, 1)
		So(data.Consume(res[2].Result[0]).Get("test").Data(), ShouldEqual, nil)
		_, ok := res[2].Result[0].(map[string]interface{})["test"]
		So(ok, ShouldEqual, false)

	})

	Convey("Update a set of records, but only if they exist", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1 SET test = true;
		UPDATE |person:1..3| SET test = false WHERE id != VOID;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[1].Result, ShouldHaveLength, 1)
		So(data.Consume(res[1].Result[0]).Get("test").Data(), ShouldEqual, true)
		So(res[2].Result, ShouldHaveLength, 1)
		So(data.Consume(res[2].Result[0]).Get("test").Data(), ShouldEqual, false)

	})

	Convey("Update a set of records using CONTENT with embedded times / records", t, func() {

		clock, _ := time.Parse(time.RFC3339, "1987-06-22T08:00:00.123456789Z")

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:test CONTENT {"time":"1987-06-22T08:00:00.123456789Z","test":"person:other"};
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[1].Result, ShouldHaveLength, 1)
		So(data.Consume(res[1].Result[0]).Get("time").Data(), ShouldEqual, clock)
		So(data.Consume(res[1].Result[0]).Get("test").Data(), ShouldResemble, sql.NewThing("person", "other"))
		So(res[2].Result, ShouldHaveLength, 1)
		So(data.Consume(res[2].Result[0]).Get("time").Data(), ShouldEqual, clock)
		So(data.Consume(res[2].Result[0]).Get("test").Data(), ShouldResemble, sql.NewThing("person", "other"))

	})

}
