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

	"github.com/abcum/surreal/util/data"
	. "github.com/smartystreets/goconvey/convey"
)

func TestCreate(t *testing.T) {

	Convey("Create with invalid value", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE 1;
		CREATE "one";
		CREATE ["many"];
		CREATE [{value:"one"}];
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[1].Status, ShouldEqual, "ERR")
		So(res[1].Detail, ShouldEqual, "Can not execute CREATE query using value '1'")
		So(res[2].Status, ShouldEqual, "ERR")
		So(res[2].Detail, ShouldEqual, "Can not execute CREATE query using value 'one'")
		So(res[3].Status, ShouldEqual, "ERR")
		So(res[3].Detail, ShouldEqual, "Can not execute CREATE query using value '[many]'")
		So(res[4].Status, ShouldEqual, "ERR")
		So(res[4].Detail, ShouldEqual, "Can not execute CREATE query using value '[map[value:one]]'")

	})

	Convey("Create record when it exists", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:test;
		CREATE person:test;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 0)
		So(res[2].Status, ShouldEqual, "ERR_KV")
		So(res[2].Detail, ShouldEqual, "Database record 'person:test' already exists")

	})

	Convey("Create unique record using `table`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Result, ShouldHaveLength, 1)
		So(data.Consume(res[1].Result[0]).Get("meta.id").Data(), ShouldHaveLength, 20)
		So(data.Consume(res[1].Result[0]).Get("meta.tb").Data(), ShouldEqual, "person")

	})

	Convey("Create specific record using `thing`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:test;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Result, ShouldHaveLength, 1)
		So(data.Consume(res[1].Result[0]).Get("meta.id").Data(), ShouldHaveLength, 4)
		So(data.Consume(res[1].Result[0]).Get("meta.tb").Data(), ShouldEqual, "person")

	})

	Convey("Create unique records using `batch`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE batch("person", ["1", "2", "person:3"]);
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Result, ShouldHaveLength, 3)
		So(data.Consume(res[1].Result[0]).Get("meta.id").Data(), ShouldEqual, 1)
		So(data.Consume(res[1].Result[2]).Get("meta.id").Data(), ShouldEqual, 3)

	})

	Convey("Create unique records using `model`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:100|;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Result, ShouldHaveLength, 100)
		So(data.Consume(res[1].Result[0]).Get("meta.id").Data(), ShouldHaveLength, 20)
		So(data.Consume(res[1].Result[99]).Get("meta.id").Data(), ShouldHaveLength, 20)

	})

	Convey("Create sequential ascending records using `model`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:1..100|;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Result, ShouldHaveLength, 100)
		So(data.Consume(res[1].Result[0]).Get("meta.id").Data(), ShouldEqual, 1)
		So(data.Consume(res[1].Result[99]).Get("meta.id").Data(), ShouldEqual, 100)

	})

	Convey("Create sequential descending records using `model`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:100..1|;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Result, ShouldHaveLength, 100)
		So(data.Consume(res[1].Result[0]).Get("meta.id").Data(), ShouldEqual, 100)
		So(data.Consume(res[1].Result[99]).Get("meta.id").Data(), ShouldEqual, 1)

	})

	Convey("Create sequential ascending negative-to-positive records using `model`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:-50..50|;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Result, ShouldHaveLength, 101)
		So(data.Consume(res[1].Result[0]).Get("meta.id").Data(), ShouldEqual, -50)
		So(data.Consume(res[1].Result[1]).Get("meta.id").Data(), ShouldEqual, -49)
		So(data.Consume(res[1].Result[100]).Get("meta.id").Data(), ShouldEqual, 50)

	})

	Convey("Create sequential ascending decimal records using `model`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:1,0.5..50|;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Result, ShouldHaveLength, 99)
		So(data.Consume(res[1].Result[0]).Get("meta.id").Data(), ShouldEqual, 1)
		So(data.Consume(res[1].Result[1]).Get("meta.id").Data(), ShouldEqual, 1.5)
		So(data.Consume(res[1].Result[98]).Get("meta.id").Data(), ShouldEqual, 50)

	})

	Convey("Create sequential descending decimal records using `model`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:50,0.5..1|;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Result, ShouldHaveLength, 99)
		So(data.Consume(res[1].Result[0]).Get("meta.id").Data(), ShouldEqual, 50)
		So(data.Consume(res[1].Result[1]).Get("meta.id").Data(), ShouldEqual, 49.5)
		So(data.Consume(res[1].Result[98]).Get("meta.id").Data(), ShouldEqual, 1)

	})

	Convey("Create sequential ascending decimal negative-to-positive records using `model`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:-50,0.5..50|;
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
		CREATE person:1;
		CREATE person:1.0;
		CREATE person:1.0000;
		CREATE person:⟨1⟩;
		CREATE person:⟨1.0⟩;
		CREATE person:⟨1.0000⟩;
		SELECT id FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 8)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Status, ShouldEqual, "ERR_KV")
		So(res[2].Detail, ShouldEqual, "Database record 'person:1' already exists")
		So(res[3].Status, ShouldEqual, "ERR_KV")
		So(res[3].Detail, ShouldEqual, "Database record 'person:1' already exists")
		So(res[4].Status, ShouldEqual, "ERR_KV")
		So(res[4].Detail, ShouldEqual, "Database record 'person:1' already exists")
		So(res[5].Status, ShouldEqual, "ERR_KV")
		So(res[5].Detail, ShouldEqual, "Database record 'person:1' already exists")
		So(res[6].Status, ShouldEqual, "ERR_KV")
		So(res[6].Detail, ShouldEqual, "Database record 'person:1' already exists")
		So(res[7].Result, ShouldHaveLength, 1)

	})

	Convey("Creating with a timeout of 1ms returns an error", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:1..1000| TIMEOUT 1ms;
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[1].Result, ShouldHaveLength, 0)
		So(res[2].Result, ShouldHaveLength, 0)
		So(res[1].Status, ShouldEqual, "ERR")
		So(res[1].Detail, ShouldEqual, "Query timeout of 1ms exceeded")

	})

}
