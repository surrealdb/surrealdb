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

	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSelect(t *testing.T) {

	Convey("Select temps", t, func() {

		setupDB()

		txt := `

		USE NS test DB test;

		LET var = NULL;

		SELECT * FROM "test" WHERE NULL = "";
		SELECT * FROM "test" WHERE NULL = $var;
		SELECT * FROM "test" WHERE NULL = VOID;
		SELECT * FROM "test" WHERE NULL = NULL;
		SELECT * FROM "test" WHERE NULL = EMPTY;

		SELECT * FROM "test" WHERE "" = NULL;
		SELECT * FROM "test" WHERE $var = NULL;
		SELECT * FROM "test" WHERE VOID = NULL;
		SELECT * FROM "test" WHERE NULL = NULL;
		SELECT * FROM "test" WHERE EMPTY = NULL;

		SELECT * FROM "test" WHERE VOID = "";
		SELECT * FROM "test" WHERE VOID = $var;
		SELECT * FROM "test" WHERE VOID = NULL;
		SELECT * FROM "test" WHERE VOID = VOID;
		SELECT * FROM "test" WHERE VOID = EMPTY;

		SELECT * FROM "test" WHERE "" = VOID;
		SELECT * FROM "test" WHERE $var = VOID;
		SELECT * FROM "test" WHERE NULL = VOID;
		SELECT * FROM "test" WHERE VOID = VOID;
		SELECT * FROM "test" WHERE EMPTY = VOID;

		SELECT * FROM "test" WHERE EMPTY = "";
		SELECT * FROM "test" WHERE EMPTY = $var;
		SELECT * FROM "test" WHERE EMPTY = NULL;
		SELECT * FROM "test" WHERE EMPTY = VOID;
		SELECT * FROM "test" WHERE EMPTY = EMPTY;

		SELECT * FROM "test" WHERE "" = EMPTY;
		SELECT * FROM "test" WHERE $var = EMPTY;
		SELECT * FROM "test" WHERE NULL = EMPTY;
		SELECT * FROM "test" WHERE VOID = EMPTY;
		SELECT * FROM "test" WHERE EMPTY = EMPTY;

		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 32)

		So(res[2].Result, ShouldHaveLength, 0)
		So(res[3].Result, ShouldHaveLength, 1)
		So(res[4].Result, ShouldHaveLength, 0)
		So(res[5].Result, ShouldHaveLength, 1)
		So(res[6].Result, ShouldHaveLength, 1)

		So(res[7].Result, ShouldHaveLength, 0)
		So(res[8].Result, ShouldHaveLength, 1)
		So(res[9].Result, ShouldHaveLength, 0)
		So(res[10].Result, ShouldHaveLength, 1)
		So(res[11].Result, ShouldHaveLength, 1)

		So(res[12].Result, ShouldHaveLength, 0)
		So(res[13].Result, ShouldHaveLength, 0)
		So(res[14].Result, ShouldHaveLength, 0)
		So(res[15].Result, ShouldHaveLength, 1)
		So(res[16].Result, ShouldHaveLength, 1)

		So(res[17].Result, ShouldHaveLength, 0)
		So(res[18].Result, ShouldHaveLength, 0)
		So(res[19].Result, ShouldHaveLength, 0)
		So(res[20].Result, ShouldHaveLength, 1)
		So(res[21].Result, ShouldHaveLength, 1)

		So(res[22].Result, ShouldHaveLength, 0)
		So(res[23].Result, ShouldHaveLength, 1)
		So(res[24].Result, ShouldHaveLength, 1)
		So(res[25].Result, ShouldHaveLength, 1)
		So(res[26].Result, ShouldHaveLength, 1)

		So(res[27].Result, ShouldHaveLength, 0)
		So(res[28].Result, ShouldHaveLength, 1)
		So(res[29].Result, ShouldHaveLength, 1)
		So(res[30].Result, ShouldHaveLength, 1)
		So(res[31].Result, ShouldHaveLength, 1)

	})

	Convey("Select records from one thing", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1;
		CREATE person:test;
		SELECT * FROM person:1;
		SELECT * FROM person:test;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("meta.id").Data(), ShouldEqual, 1)
		So(res[4].Result, ShouldHaveLength, 1)
		So(data.Consume(res[4].Result[0]).Get("meta.id").Data(), ShouldEqual, "test")

	})

	Convey("Select records from one thing using quotes", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1;
		CREATE person:test;
		SELECT * FROM person:⟨1⟩;
		SELECT * FROM person:⟨test⟩;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("meta.id").Data(), ShouldEqual, 1)
		So(res[4].Result, ShouldHaveLength, 1)
		So(data.Consume(res[4].Result[0]).Get("meta.id").Data(), ShouldEqual, "test")

	})

	Convey("Select records from one table", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:test;
		CREATE |person:10|;
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 10)
		So(res[3].Result, ShouldHaveLength, 11)

	})

	Convey("Select records from multiple things", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:test;
		CREATE entity:test;
		SELECT * FROM person, entity;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 2)

	})

	Convey("Select records from multiple things and tables", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:test;
		CREATE entity:test;
		CREATE |person:10|;
		CREATE |entity:10|;
		SELECT * FROM person:test, entity:test, person, entity;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 6)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 10)
		So(res[4].Result, ShouldHaveLength, 10)
		So(res[5].Result, ShouldHaveLength, 24)

	})

	Convey("Select records using variable for a `table`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE $tb;
		SELECT * FROM $tb;
		`

		res, err := Execute(setupKV(), txt, map[string]interface{}{
			"tb": sql.NewTable("person"),
		})

		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(data.Consume(res[1].Result[0]).Get("meta.id").Data(), ShouldHaveLength, 20)
		So(data.Consume(res[1].Result[0]).Get("meta.tb").Data(), ShouldEqual, "person")

	})

	Convey("Select records using variable for a `thing`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE $id;
		SELECT * FROM $id;
		`

		res, err := Execute(setupKV(), txt, map[string]interface{}{
			"id": sql.NewThing("person", "test"),
		})

		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(data.Consume(res[1].Result[0]).Get("meta.id").Data(), ShouldHaveLength, 4)
		So(data.Consume(res[1].Result[0]).Get("meta.tb").Data(), ShouldEqual, "person")

	})

	Convey("Select records using an * subquery", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1 SET name="Tobias";
		CREATE person:2 SET name="Silvana";
		CREATE person:3 SET name="Jonathan";
		CREATE person:4 SET name="Benjamin";
		CREATE person:5 SET name="Alexander";
		SELECT * FROM (SELECT * FROM person ORDER BY name);
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 7)
		So(res[6].Result, ShouldHaveLength, 5)
		So(data.Consume(res[6].Result[0]).Get("name").Data(), ShouldEqual, "Alexander")
		So(data.Consume(res[6].Result[4]).Get("name").Data(), ShouldEqual, "Tobias")

	})

	Convey("Select records using an * subquery, with a limit of 1", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1 SET name="Tobias";
		CREATE person:2 SET name="Silvana";
		CREATE person:3 SET name="Jonathan";
		CREATE person:4 SET name="Benjamin";
		CREATE person:5 SET name="Alexander";
		SELECT * FROM (SELECT * FROM person ORDER BY name LIMIT 1);
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 7)
		So(res[6].Result, ShouldHaveLength, 1)
		So(data.Consume(res[6].Result[0]).Get("name").Data(), ShouldEqual, "Alexander")

	})

	Convey("Select records using an * subquery, specifying a single record", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1 SET name="Tobias";
		CREATE person:2 SET name="Silvana";
		CREATE person:3 SET name="Jonathan";
		CREATE person:4 SET name="Benjamin";
		CREATE person:5 SET name="Alexander";
		SELECT * FROM (SELECT * FROM person:5);
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 7)
		So(res[6].Result, ShouldHaveLength, 1)
		So(data.Consume(res[6].Result[0]).Get("name").Data(), ShouldEqual, "Alexander")

	})

	Convey("Select records using an id subquery", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1 SET name="Tobias";
		CREATE person:2 SET name="Silvana";
		CREATE person:3 SET name="Jonathan";
		CREATE person:4 SET name="Benjamin";
		CREATE person:5 SET name="Alexander";
		SELECT * FROM (SELECT id FROM (SELECT * FROM person ORDER BY name));
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 7)
		So(res[6].Result, ShouldHaveLength, 5)
		So(data.Consume(res[6].Result[0]).Get("name").Data(), ShouldEqual, "Alexander")
		So(data.Consume(res[6].Result[4]).Get("name").Data(), ShouldEqual, "Tobias")

	})

	Convey("Select records using an id subquery, with a limit of 1", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1 SET name="Tobias";
		CREATE person:2 SET name="Silvana";
		CREATE person:3 SET name="Jonathan";
		CREATE person:4 SET name="Benjamin";
		CREATE person:5 SET name="Alexander";
		SELECT * FROM (SELECT id FROM (SELECT * FROM person ORDER BY name) LIMIT 1);
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 7)
		So(res[6].Result, ShouldHaveLength, 1)
		So(data.Consume(res[6].Result[0]).Get("name").Data(), ShouldEqual, "Alexander")

	})

	Convey("Select records using an id subquery, specifying a single record", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1 SET name="Tobias";
		CREATE person:2 SET name="Silvana";
		CREATE person:3 SET name="Jonathan";
		CREATE person:4 SET name="Benjamin";
		CREATE person:5 SET name="Alexander";
		SELECT * FROM (SELECT id FROM (SELECT * FROM person:5));
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 7)
		So(res[6].Result, ShouldHaveLength, 1)
		So(data.Consume(res[6].Result[0]).Get("name").Data(), ShouldEqual, "Alexander")

	})

	Convey("Select records using a single field subquery", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1 SET name="Tobias";
		CREATE person:2 SET name="Silvana";
		CREATE person:3 SET name="Jonathan";
		CREATE person:4 SET name="Benjamin";
		CREATE person:5 SET name="Alexander";
		SELECT * FROM (SELECT name FROM person ORDER BY name);
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 7)
		So(res[6].Result, ShouldHaveLength, 5)
		So(res[6].Result[0], ShouldEqual, "Alexander")
		So(res[6].Result[4], ShouldEqual, "Tobias")

	})

	Convey("Select records using a single field subquery, with a limit of 1", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1 SET name="Tobias";
		CREATE person:2 SET name="Silvana";
		CREATE person:3 SET name="Jonathan";
		CREATE person:4 SET name="Benjamin";
		CREATE person:5 SET name="Alexander";
		SELECT * FROM (SELECT name FROM person ORDER BY name LIMIT 1);
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 7)
		So(res[6].Result, ShouldHaveLength, 1)
		So(res[6].Result[0], ShouldEqual, "Alexander")

	})

	Convey("Select $thing from a direct `thing` record fetch", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE tester:test;
		CREATE person:test SET name="Tobias";
		SELECT *, person:test AS test FROM tester;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("meta.tb").Data(), ShouldEqual, "tester")
		So(data.Consume(res[3].Result[0]).Get("meta.id").Data(), ShouldEqual, "test")
		So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldResemble, &sql.Thing{"person", "test"})

	})

	Convey("Select 'id' parameter from a direct `thing` record fetch", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE tester:test;
		CREATE person:test SET name="Tobias";
		SELECT *, person:test.id AS test FROM tester;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("meta.tb").Data(), ShouldEqual, "tester")
		So(data.Consume(res[3].Result[0]).Get("meta.id").Data(), ShouldEqual, "test")
		So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldResemble, &sql.Thing{"person", "test"})

	})

	Convey("Select 'name' parameter from a direct `thing` record fetch", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE tester:test;
		CREATE person:test SET name="Tobias";
		SELECT *, person:test.name AS test FROM tester;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("meta.tb").Data(), ShouldEqual, "tester")
		So(data.Consume(res[3].Result[0]).Get("meta.id").Data(), ShouldEqual, "test")
		So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldEqual, "Tobias")

	})

	Convey("Select 'id.name' parameter from a direct `thing` record fetch", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE tester:test;
		CREATE person:test SET name="Tobias";
		SELECT *, person:test.id.name AS test FROM tester;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("meta.tb").Data(), ShouldEqual, "tester")
		So(data.Consume(res[3].Result[0]).Get("meta.id").Data(), ShouldEqual, "test")
		So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldEqual, "Tobias")

	})

	Convey("Select 'id.id.id.name' parameter from a direct `thing` record fetch", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE tester:test;
		CREATE person:test SET name="Tobias";
		SELECT *, person:test.id.id.id.name AS test FROM tester;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("meta.tb").Data(), ShouldEqual, "tester")
		So(data.Consume(res[3].Result[0]).Get("meta.id").Data(), ShouldEqual, "test")
		So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldEqual, "Tobias")

	})

	Convey("Select $param parameter from a direct `param` record fetch", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		LET person = person:test;
		CREATE tester:test;
		CREATE person:test SET name="Tobias";
		SELECT *, $person.id AS test FROM tester;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(res[4].Result, ShouldHaveLength, 1)
		So(data.Consume(res[4].Result[0]).Get("meta.tb").Data(), ShouldEqual, "tester")
		So(data.Consume(res[4].Result[0]).Get("meta.id").Data(), ShouldEqual, "test")
		So(data.Consume(res[4].Result[0]).Get("test").Data(), ShouldResemble, &sql.Thing{"person", "test"})

	})

	Convey("Select 'id' parameter from a direct `param` record fetch", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		LET person = person:test;
		CREATE tester:test;
		CREATE person:test SET name="Tobias";
		SELECT *, $person.id AS test FROM tester;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(res[4].Result, ShouldHaveLength, 1)
		So(data.Consume(res[4].Result[0]).Get("meta.tb").Data(), ShouldEqual, "tester")
		So(data.Consume(res[4].Result[0]).Get("meta.id").Data(), ShouldEqual, "test")
		So(data.Consume(res[4].Result[0]).Get("test").Data(), ShouldResemble, &sql.Thing{"person", "test"})

	})

	Convey("Select 'name' parameter from a direct `param` record fetch", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		LET person = person:test;
		CREATE tester:test;
		CREATE person:test SET name="Tobias";
		SELECT *, $person.name AS test FROM tester;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(res[4].Result, ShouldHaveLength, 1)
		So(data.Consume(res[4].Result[0]).Get("meta.tb").Data(), ShouldEqual, "tester")
		So(data.Consume(res[4].Result[0]).Get("meta.id").Data(), ShouldEqual, "test")
		So(data.Consume(res[4].Result[0]).Get("test").Data(), ShouldEqual, "Tobias")

	})

	Convey("Select 'id.name' parameter from a direct `param` record fetch", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		LET person = person:test;
		CREATE tester:test;
		CREATE person:test SET name="Tobias";
		SELECT *, $person.id.name AS test FROM tester;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(res[4].Result, ShouldHaveLength, 1)
		So(data.Consume(res[4].Result[0]).Get("meta.tb").Data(), ShouldEqual, "tester")
		So(data.Consume(res[4].Result[0]).Get("meta.id").Data(), ShouldEqual, "test")
		So(data.Consume(res[4].Result[0]).Get("test").Data(), ShouldEqual, "Tobias")

	})

	Convey("Select 'id.id.id.name' parameter from a direct `param` record fetch", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		LET person = person:test;
		CREATE tester:test;
		CREATE person:test SET name="Tobias";
		SELECT *, $person.id.id.id.name AS test FROM tester;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(res[4].Result, ShouldHaveLength, 1)
		So(data.Consume(res[4].Result[0]).Get("meta.tb").Data(), ShouldEqual, "tester")
		So(data.Consume(res[4].Result[0]).Get("meta.id").Data(), ShouldEqual, "test")
		So(data.Consume(res[4].Result[0]).Get("test").Data(), ShouldEqual, "Tobias")

	})

	Convey("Select $parent parameter from a subquery `param`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE tester:test;
		CREATE person:test SET name="Tobias";
		SELECT *, (SELECT $parent FROM tester LIMIT 1) AS test FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("meta.tb").Data(), ShouldEqual, "person")
		So(data.Consume(res[3].Result[0]).Get("meta.id").Data(), ShouldEqual, "test")
		So(data.Consume(res[3].Result[0]).Get("name").Data(), ShouldEqual, "Tobias")
		So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldResemble, map[string]interface{}{
			"id": &sql.Thing{"person", "test"},
			"meta": map[string]interface{}{
				"id": "test",
				"tb": "person",
			},
			"name": "Tobias",
		})

	})

	Convey("Select 'id' parameter from a subquery `param`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE tester:test;
		CREATE person:test SET name="Tobias";
		SELECT *, (SELECT $parent.id FROM tester LIMIT 1) AS test FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("meta.tb").Data(), ShouldEqual, "person")
		So(data.Consume(res[3].Result[0]).Get("meta.id").Data(), ShouldEqual, "test")
		So(data.Consume(res[3].Result[0]).Get("name").Data(), ShouldEqual, "Tobias")
		So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldResemble, &sql.Thing{"person", "test"})

	})

	Convey("Select 'name' parameter from a subquery `param`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE tester:test;
		CREATE person:test SET name="Tobias";
		SELECT *, (SELECT $parent.name FROM tester LIMIT 1) AS test FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("meta.tb").Data(), ShouldEqual, "person")
		So(data.Consume(res[3].Result[0]).Get("meta.id").Data(), ShouldEqual, "test")
		So(data.Consume(res[3].Result[0]).Get("name").Data(), ShouldEqual, "Tobias")
		So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldEqual, "Tobias")

	})

	Convey("Select 'id.name' parameter from a subquery `param`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE tester:test;
		CREATE person:test SET name="Tobias";
		SELECT *, (SELECT $parent.id.name FROM tester LIMIT 1) AS test FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("meta.tb").Data(), ShouldEqual, "person")
		So(data.Consume(res[3].Result[0]).Get("meta.id").Data(), ShouldEqual, "test")
		So(data.Consume(res[3].Result[0]).Get("name").Data(), ShouldEqual, "Tobias")
		So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldEqual, "Tobias")

	})

	Convey("Select 'id.id.id.name' parameter from a subquery `param`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE tester:test;
		CREATE person:test SET name="Tobias";
		SELECT *, (SELECT $parent.id.id.id.name FROM tester LIMIT 1) AS test FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("meta.tb").Data(), ShouldEqual, "person")
		So(data.Consume(res[3].Result[0]).Get("meta.id").Data(), ShouldEqual, "test")
		So(data.Consume(res[3].Result[0]).Get("name").Data(), ShouldEqual, "Tobias")
		So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldEqual, "Tobias")

	})

	Convey("Filter using VOID to find records where the field is not set", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1;
		CREATE person:2 SET test = null;
		CREATE person:3 SET test = true;
		CREATE person:4 SET test = "Test";
		CREATE person:5 SET test = "1000";
		SELECT test FROM person WHERE test IS VOID;
		SELECT test FROM person WHERE test IS NOT VOID;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 8)
		So(res[6].Result, ShouldHaveLength, 1)
		So(res[7].Result, ShouldHaveLength, 4)

	})

	Convey("Filter using NULL to find records where the field is `null`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1;
		CREATE person:2 SET test = null;
		CREATE person:3 SET test = true;
		CREATE person:4 SET test = "Test";
		CREATE person:5 SET test = "1000";
		SELECT test FROM person WHERE test IS NULL;
		SELECT test FROM person WHERE test IS NOT NULL;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 8)
		So(res[6].Result, ShouldHaveLength, 1)
		So(res[7].Result, ShouldHaveLength, 4)

	})

	Convey("Filter using EMPTY to find records where the field is not set or `null`", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1;
		CREATE person:2 SET test = null;
		CREATE person:3 SET test = true;
		CREATE person:4 SET test = "Test";
		CREATE person:5 SET test = "1000";
		SELECT test FROM person WHERE test IS EMPTY;
		SELECT test FROM person WHERE test IS NOT EMPTY;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 8)
		So(res[6].Result, ShouldHaveLength, 2)
		So(res[7].Result, ShouldHaveLength, 3)

	})

	Convey("Filter using OR boolean logic to find records", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1 SET test = "one";
		CREATE person:2 SET test = "two";
		CREATE person:3 SET test = "tre";
		SELECT test FROM person WHERE ( (test = "one") OR (test = "two") );
		SELECT test FROM person WHERE ( test = "one" OR test = "two" );
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 6)
		So(res[4].Result, ShouldHaveLength, 2)
		// IMPORTANT enable test
		SkipSo(res[5].Result, ShouldHaveLength, 2)

	})

	Convey("Filter using AND boolean logic to find records", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1 SET test = "one";
		CREATE person:2 SET test = "two";
		CREATE person:3 SET test = "tre";
		SELECT test FROM person WHERE ( (test = "one") AND ( (test != "two") AND (test != "tre") ) );
		SELECT test FROM person WHERE ( test = "one" AND (test != "two" AND test != "tre") );
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 6)
		So(res[4].Result, ShouldHaveLength, 1)
		// IMPORTANT enable test
		SkipSo(res[5].Result, ShouldHaveLength, 1)

	})

	Convey("Filter records using an id subquery", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1 SET name="Tobias";
		CREATE person:2 SET name="Silvana";
		CREATE person:3 SET name="Jonathan";
		CREATE person:4 SET name="Benjamin";
		CREATE person:5 SET name="Alexander";
		SELECT * FROM person WHERE id IN (SELECT id FROM person);
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 7)
		So(res[6].Result, ShouldHaveLength, 5)
		So(data.Consume(res[6].Result[0]).Get("name").Data(), ShouldEqual, "Tobias")
		So(data.Consume(res[6].Result[4]).Get("name").Data(), ShouldEqual, "Alexander")

	})

	Convey("Filter records using an id subquery, with a limit of 1", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1 SET name="Tobias";
		CREATE person:2 SET name="Silvana";
		CREATE person:3 SET name="Jonathan";
		CREATE person:4 SET name="Benjamin";
		CREATE person:5 SET name="Alexander";
		SELECT * FROM person WHERE id = (SELECT id FROM person LIMIT 1);
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 7)
		So(res[6].Result, ShouldHaveLength, 1)
		So(data.Consume(res[6].Result[0]).Get("name").Data(), ShouldEqual, "Tobias")

	})

	Convey("Filter records using an single field subquery", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1 SET name="Tobias";
		CREATE person:2 SET name="Silvana";
		CREATE person:3 SET name="Jonathan";
		CREATE person:4 SET name="Benjamin";
		CREATE person:5 SET name="Alexander";
		SELECT * FROM person WHERE name IN (SELECT name FROM person ORDER BY name);
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 7)
		So(res[6].Result, ShouldHaveLength, 5)
		So(data.Consume(res[6].Result[0]).Get("name").Data(), ShouldEqual, "Tobias")
		So(data.Consume(res[6].Result[4]).Get("name").Data(), ShouldEqual, "Alexander")

	})

	Convey("Filter records using a single field subquery, with a limit of 1", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1 SET name="Tobias";
		CREATE person:2 SET name="Silvana";
		CREATE person:3 SET name="Jonathan";
		CREATE person:4 SET name="Benjamin";
		CREATE person:5 SET name="Alexander";
		SELECT * FROM person WHERE name = (SELECT name FROM person ORDER BY name LIMIT 1);
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 7)
		So(res[6].Result, ShouldHaveLength, 1)
		So(data.Consume(res[6].Result[0]).Get("name").Data(), ShouldEqual, "Alexander")

	})

	Convey("Filter records using a single field subquery, with a limit of 1", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:test SET age=30;
		CREATE person:1 SET name="Tobias", age=30;
		CREATE person:2 SET name="Silvana", age=27;
		CREATE person:3 SET name="Jonathan", age=32;
		CREATE person:4 SET name="Benjamin", age=31;
		CREATE person:5 SET name="Alexander", age=22;
		SELECT * FROM person WHERE age >= person:⟨1⟩.age ORDER BY name;
		SELECT * FROM person WHERE age >= person:test.age ORDER BY name;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 9)
		So(res[7].Result, ShouldHaveLength, 4)
		So(data.Consume(res[7].Result[0]).Get("name").Data(), ShouldEqual, nil)
		So(data.Consume(res[7].Result[1]).Get("name").Data(), ShouldEqual, "Benjamin")
		So(data.Consume(res[7].Result[2]).Get("name").Data(), ShouldEqual, "Jonathan")
		So(data.Consume(res[7].Result[3]).Get("name").Data(), ShouldEqual, "Tobias")
		So(res[8].Result, ShouldHaveLength, 4)
		So(data.Consume(res[8].Result[0]).Get("name").Data(), ShouldEqual, nil)
		So(data.Consume(res[8].Result[1]).Get("name").Data(), ShouldEqual, "Benjamin")
		So(data.Consume(res[8].Result[2]).Get("name").Data(), ShouldEqual, "Jonathan")
		So(data.Consume(res[8].Result[3]).Get("name").Data(), ShouldEqual, "Tobias")

	})

	Convey("Group records by field", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:1..10|;
		UPDATE person:1 SET test = true;
		UPDATE person:2 SET test = false;
		UPDATE person:3 SET test = false;
		UPDATE person:4 SET test = true;
		UPDATE person:5 SET test = nil;
		SELECT test FROM person GROUP BY test ORDER BY test;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 8)
		So(res[7].Result, ShouldHaveLength, 3)
		So(data.Consume(res[7].Result[0]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[7].Result[1]).Get("test").Data(), ShouldEqual, false)
		So(data.Consume(res[7].Result[2]).Get("test").Data(), ShouldEqual, true)

	})

	Convey("Group and count records by field", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:1..5|;
		UPDATE person:1 SET test = true;
		UPDATE person:2 SET test = false;
		UPDATE person:3 SET test = false;
		UPDATE person:4 SET test = true;
		UPDATE person:5 SET test = nil;
		SELECT test, count(*) AS total FROM person GROUP BY test ORDER BY test;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 8)
		So(res[7].Result, ShouldHaveLength, 3)
		So(data.Consume(res[7].Result[0]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[7].Result[0]).Get("total").Data(), ShouldEqual, 1)
		So(data.Consume(res[7].Result[1]).Get("test").Data(), ShouldEqual, false)
		So(data.Consume(res[7].Result[1]).Get("total").Data(), ShouldEqual, 2)
		So(data.Consume(res[7].Result[2]).Get("test").Data(), ShouldEqual, true)
		So(data.Consume(res[7].Result[2]).Get("total").Data(), ShouldEqual, 2)

	})

	Convey("Group and count records by field with alias", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:1..5|;
		UPDATE person:1 SET test = "something";
		UPDATE person:2 SET test = "nothing";
		UPDATE person:3 SET test = "nothing";
		UPDATE person:4 SET test = "something";
		UPDATE person:5 SET test = nil;
		SELECT string.length(test) AS test, count(*) AS total FROM person GROUP BY test ORDER BY test;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 8)
		So(res[7].Result, ShouldHaveLength, 3)
		So(data.Consume(res[7].Result[0]).Get("test").Data(), ShouldEqual, 0)
		So(data.Consume(res[7].Result[0]).Get("total").Data(), ShouldEqual, 1)
		So(data.Consume(res[7].Result[1]).Get("test").Data(), ShouldEqual, 7)
		So(data.Consume(res[7].Result[1]).Get("total").Data(), ShouldEqual, 2)
		So(data.Consume(res[7].Result[2]).Get("test").Data(), ShouldEqual, 9)
		So(data.Consume(res[7].Result[2]).Get("total").Data(), ShouldEqual, 2)

	})

	Convey("Group and retrieve distinct records by field", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:1..5|;
		UPDATE person:1 SET test = "Hello";
		UPDATE person:2 SET test = "World";
		UPDATE person:3 SET test = "World";
		UPDATE person:4 SET test = "Hello";
		UPDATE person:5 SET test = "Hello";
		SELECT test, distinct(id) AS docs FROM person GROUP BY test ORDER BY test;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 8)
		So(res[7].Result, ShouldHaveLength, 2)
		So(data.Consume(res[7].Result[0]).Get("test").Data(), ShouldEqual, "Hello")
		So(data.Consume(res[7].Result[0]).Get("docs").Data(), ShouldContain, sql.NewThing("person", 1))
		So(data.Consume(res[7].Result[0]).Get("docs").Data(), ShouldContain, sql.NewThing("person", 4))
		So(data.Consume(res[7].Result[0]).Get("docs").Data(), ShouldContain, sql.NewThing("person", 5))
		So(data.Consume(res[7].Result[1]).Get("test").Data(), ShouldEqual, "World")
		So(data.Consume(res[7].Result[1]).Get("docs").Data(), ShouldContain, sql.NewThing("person", 2))
		So(data.Consume(res[7].Result[1]).Get("docs").Data(), ShouldContain, sql.NewThing("person", 3))

	})

	Convey("Order records ascending", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:1..10|;
		UPDATE person:3 SET test = "ändrew";
		UPDATE person:5 SET test = "Another";
		UPDATE person:7 SET test = "alexander";
		UPDATE person:9 SET test = "Alexander";
		UPDATE person:2 SET test = "Tobie";
		UPDATE person:4 SET test = "1000";
		UPDATE person:6 SET test = "2";
		UPDATE person:8 SET test = null;
		SELECT test FROM person ORDER BY test ASC;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 11)
		So(res[10].Result, ShouldHaveLength, 10)
		So(data.Consume(res[10].Result[0]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[10].Result[1]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[10].Result[2]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[10].Result[3]).Get("test").Data(), ShouldEqual, "1000")
		So(data.Consume(res[10].Result[4]).Get("test").Data(), ShouldEqual, "2")
		So(data.Consume(res[10].Result[5]).Get("test").Data(), ShouldEqual, "Alexander")
		So(data.Consume(res[10].Result[6]).Get("test").Data(), ShouldEqual, "Another")
		So(data.Consume(res[10].Result[7]).Get("test").Data(), ShouldEqual, "Tobie")
		So(data.Consume(res[10].Result[8]).Get("test").Data(), ShouldEqual, "alexander")
		So(data.Consume(res[10].Result[9]).Get("test").Data(), ShouldEqual, "ändrew")

	})

	Convey("Order records descending", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:1..10|;
		UPDATE person:3 SET test = "ändrew";
		UPDATE person:5 SET test = "Another";
		UPDATE person:7 SET test = "alexander";
		UPDATE person:9 SET test = "Alexander";
		UPDATE person:2 SET test = "Tobie";
		UPDATE person:4 SET test = "1000";
		UPDATE person:6 SET test = "2";
		UPDATE person:8 SET test = null;
		SELECT test FROM person ORDER BY test DESC;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 11)
		So(res[10].Result, ShouldHaveLength, 10)
		So(data.Consume(res[10].Result[0]).Get("test").Data(), ShouldEqual, "ändrew")
		So(data.Consume(res[10].Result[1]).Get("test").Data(), ShouldEqual, "alexander")
		So(data.Consume(res[10].Result[2]).Get("test").Data(), ShouldEqual, "Tobie")
		So(data.Consume(res[10].Result[3]).Get("test").Data(), ShouldEqual, "Another")
		So(data.Consume(res[10].Result[4]).Get("test").Data(), ShouldEqual, "Alexander")
		So(data.Consume(res[10].Result[5]).Get("test").Data(), ShouldEqual, "2")
		So(data.Consume(res[10].Result[6]).Get("test").Data(), ShouldEqual, "1000")
		So(data.Consume(res[10].Result[7]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[10].Result[8]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[10].Result[9]).Get("test").Data(), ShouldEqual, nil)

	})

	Convey("Order records with collation", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:1..10|;
		UPDATE person:3 SET test = "ändrew";
		UPDATE person:5 SET test = "Another";
		UPDATE person:7 SET test = "alexander";
		UPDATE person:9 SET test = "Alexander";
		UPDATE person:2 SET test = "Tobie";
		UPDATE person:4 SET test = "1000";
		UPDATE person:6 SET test = "2";
		UPDATE person:8 SET test = null;
		SELECT test FROM person ORDER BY test COLLATE 'en-GB' ASC;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 11)
		So(res[10].Result, ShouldHaveLength, 10)
		So(data.Consume(res[10].Result[0]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[10].Result[1]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[10].Result[2]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[10].Result[3]).Get("test").Data(), ShouldEqual, "1000")
		So(data.Consume(res[10].Result[4]).Get("test").Data(), ShouldEqual, "2")
		So(data.Consume(res[10].Result[5]).Get("test").Data(), ShouldEqual, "Alexander")
		So(data.Consume(res[10].Result[6]).Get("test").Data(), ShouldEqual, "alexander")
		So(data.Consume(res[10].Result[7]).Get("test").Data(), ShouldEqual, "ändrew")
		So(data.Consume(res[10].Result[8]).Get("test").Data(), ShouldEqual, "Another")
		So(data.Consume(res[10].Result[9]).Get("test").Data(), ShouldEqual, "Tobie")

	})

	Convey("Order records with collation and numeric sorting", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:1..10|;
		UPDATE person:3 SET test = "ändrew";
		UPDATE person:5 SET test = "Another";
		UPDATE person:7 SET test = "alexander";
		UPDATE person:9 SET test = "Alexander";
		UPDATE person:2 SET test = "Tobie";
		UPDATE person:4 SET test = "1000";
		UPDATE person:6 SET test = "2";
		UPDATE person:8 SET test = null;
		SELECT test FROM person ORDER BY test COLLATE 'en-GB' NUMERIC ASC;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 11)
		So(res[10].Result, ShouldHaveLength, 10)
		So(data.Consume(res[10].Result[0]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[10].Result[1]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[10].Result[2]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[10].Result[3]).Get("test").Data(), ShouldEqual, "2")
		So(data.Consume(res[10].Result[4]).Get("test").Data(), ShouldEqual, "1000")
		So(data.Consume(res[10].Result[5]).Get("test").Data(), ShouldEqual, "Alexander")
		So(data.Consume(res[10].Result[6]).Get("test").Data(), ShouldEqual, "alexander")
		So(data.Consume(res[10].Result[7]).Get("test").Data(), ShouldEqual, "ändrew")
		So(data.Consume(res[10].Result[8]).Get("test").Data(), ShouldEqual, "Another")
		So(data.Consume(res[10].Result[9]).Get("test").Data(), ShouldEqual, "Tobie")

	})

	Convey("Order records with collation and numeric sorting using unicode definition", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:1..10|;
		UPDATE person:3 SET test = "ändrew";
		UPDATE person:5 SET test = "Another";
		UPDATE person:7 SET test = "alexander";
		UPDATE person:9 SET test = "Alexander";
		UPDATE person:2 SET test = "Tobie";
		UPDATE person:4 SET test = "1000";
		UPDATE person:6 SET test = "2";
		UPDATE person:8 SET test = null;
		SELECT test FROM person ORDER BY test COLLATE 'en-GB-u-kn-true' ASC;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 11)
		So(res[10].Result, ShouldHaveLength, 10)
		So(data.Consume(res[10].Result[0]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[10].Result[1]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[10].Result[2]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[10].Result[3]).Get("test").Data(), ShouldEqual, "2")
		So(data.Consume(res[10].Result[4]).Get("test").Data(), ShouldEqual, "1000")
		So(data.Consume(res[10].Result[5]).Get("test").Data(), ShouldEqual, "Alexander")
		So(data.Consume(res[10].Result[6]).Get("test").Data(), ShouldEqual, "alexander")
		So(data.Consume(res[10].Result[7]).Get("test").Data(), ShouldEqual, "ändrew")
		So(data.Consume(res[10].Result[8]).Get("test").Data(), ShouldEqual, "Another")
		So(data.Consume(res[10].Result[9]).Get("test").Data(), ShouldEqual, "Tobie")

	})

	Convey("Order records from multiple tables", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:test;
		CREATE entity:test;
		SELECT * FROM person, entity ORDER BY id;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 2)
		So(data.Consume(res[3].Result[0]).Get("meta.tb").Data(), ShouldEqual, "entity")
		So(data.Consume(res[3].Result[1]).Get("meta.tb").Data(), ShouldEqual, "person")

	})

	Convey("Limit records using a number", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:1..10|;
		SELECT * FROM person LIMIT BY 5;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[1].Result, ShouldHaveLength, 10)
		So(res[2].Result, ShouldHaveLength, 5)
		So(data.Consume(res[2].Result[0]).Get("meta.id").Data(), ShouldEqual, 1)
		So(data.Consume(res[2].Result[4]).Get("meta.id").Data(), ShouldEqual, 5)

	})

	Convey("Limit records using a parameter", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		LET count = 5;
		CREATE |person:1..10|;
		SELECT * FROM person LIMIT BY $count;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[2].Result, ShouldHaveLength, 10)
		So(res[3].Result, ShouldHaveLength, 5)
		So(data.Consume(res[3].Result[0]).Get("meta.id").Data(), ShouldEqual, 1)
		So(data.Consume(res[3].Result[4]).Get("meta.id").Data(), ShouldEqual, 5)

	})

	Convey("Limit records using an invalid parameter", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		LET count = "test";
		CREATE |person:1..10|;
		SELECT * FROM person LIMIT BY $count;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[2].Result, ShouldHaveLength, 10)
		So(res[3].Result, ShouldHaveLength, 0)
		So(res[3].Status, ShouldEqual, "ERR")
		So(res[3].Detail, ShouldEqual, "Found 'test' but LIMIT expression must be a number")

	})

	Convey("Limit records using a negative number", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:1..10|;
		SELECT * FROM person LIMIT BY -10;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[1].Result, ShouldHaveLength, 10)
		So(res[2].Result, ShouldHaveLength, 10)

	})

	Convey("Limit records using a minimum number", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:1..10|;
		SELECT * FROM person LIMIT BY 0;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[1].Result, ShouldHaveLength, 10)
		So(res[2].Result, ShouldHaveLength, 0)

	})

	Convey("Limit records using a maximum number", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:1..10|;
		SELECT * FROM person LIMIT BY 100;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[1].Result, ShouldHaveLength, 10)
		So(res[2].Result, ShouldHaveLength, 10)

	})

	Convey("Limit records using a number and start records at a number", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:1..10|;
		SELECT * FROM person LIMIT BY 5 START AT 5;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[1].Result, ShouldHaveLength, 10)
		So(res[2].Result, ShouldHaveLength, 5)
		So(data.Consume(res[2].Result[0]).Get("meta.id").Data(), ShouldEqual, 6)
		So(data.Consume(res[2].Result[4]).Get("meta.id").Data(), ShouldEqual, 10)

	})

	Convey("Start records using a number", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:1..10|;
		SELECT * FROM person START AT 5;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[1].Result, ShouldHaveLength, 10)
		So(res[2].Result, ShouldHaveLength, 5)
		So(data.Consume(res[2].Result[0]).Get("meta.id").Data(), ShouldEqual, 6)
		So(data.Consume(res[2].Result[4]).Get("meta.id").Data(), ShouldEqual, 10)

	})

	Convey("Start records using a parameter", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		LET count = 5;
		CREATE |person:1..10|;
		SELECT * FROM person START AT $count;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[2].Result, ShouldHaveLength, 10)
		So(res[3].Result, ShouldHaveLength, 5)
		So(data.Consume(res[3].Result[0]).Get("meta.id").Data(), ShouldEqual, 6)
		So(data.Consume(res[3].Result[4]).Get("meta.id").Data(), ShouldEqual, 10)

	})

	Convey("Start records using an invalid parameter", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		LET count = "test";
		CREATE |person:1..10|;
		SELECT * FROM person START AT $count;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[2].Result, ShouldHaveLength, 10)
		So(res[3].Result, ShouldHaveLength, 0)
		So(res[3].Status, ShouldEqual, "ERR")
		So(res[3].Detail, ShouldEqual, "Found 'test' but START expression must be a number")

	})

	Convey("Start records using a negative number", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:1..10|;
		SELECT * FROM person START AT -10;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[1].Result, ShouldHaveLength, 10)
		So(res[2].Result, ShouldHaveLength, 10)

	})

	Convey("Start records using a lower minimum number", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:1..10|;
		SELECT * FROM person START AT 0;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[1].Result, ShouldHaveLength, 10)
		So(res[2].Result, ShouldHaveLength, 10)

	})

	Convey("Start records using a greater maximum number", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:1..10|;
		SELECT * FROM person START AT 100;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[1].Result, ShouldHaveLength, 10)
		So(res[2].Result, ShouldHaveLength, 0)

	})

	Convey("Start records using a number and limit records by a number", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:1..10|;
		SELECT * FROM person LIMIT BY 5 START AT 5;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[1].Result, ShouldHaveLength, 10)
		So(res[2].Result, ShouldHaveLength, 5)
		So(data.Consume(res[2].Result[0]).Get("meta.id").Data(), ShouldEqual, 6)
		So(data.Consume(res[2].Result[4]).Get("meta.id").Data(), ShouldEqual, 10)

	})

	Convey("Version records using a datetime", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE |person:1..10|;
		SELECT * FROM person VERSION "2017-01-01";
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[1].Result, ShouldHaveLength, 10)
		So(res[2].Result, ShouldHaveLength, 0)
		So(res[3].Result, ShouldHaveLength, 10)

	})

	Convey("Version records using a date parameter", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		LET date = "2017-01-01";
		CREATE |person:1..10|;
		SELECT * FROM person VERSION $date;
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[2].Result, ShouldHaveLength, 10)
		So(res[3].Result, ShouldHaveLength, 0)
		So(res[4].Result, ShouldHaveLength, 10)

	})

	Convey("Version records using a time parameter", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		LET time = "2017-01-01T15:04:05+07:00";
		CREATE |person:1..10|;
		SELECT * FROM person VERSION $time;
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[2].Result, ShouldHaveLength, 10)
		So(res[3].Result, ShouldHaveLength, 0)
		So(res[4].Result, ShouldHaveLength, 10)

	})

	Convey("Version records using an invalid parameter", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		LET time = "test";
		CREATE |person:1..10|;
		SELECT * FROM person VERSION $time;
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[2].Result, ShouldHaveLength, 10)
		So(res[3].Result, ShouldHaveLength, 0)
		So(res[3].Status, ShouldEqual, "ERR")
		So(res[3].Detail, ShouldEqual, "Found 'test' but VERSION expression must be a date or time")
		So(res[4].Result, ShouldHaveLength, 10)

	})

	Convey("Test version on a thing", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		LET old = time.now();
		CREATE person:test;
		UPDATE person:test SET test = 1;
		LET one = time.now();
		UPDATE person:test SET test = 2;
		LET two = time.now();
		UPDATE person:test SET test = 3;
		LET tre = time.now();
		SELECT * FROM person VERSION $old;
		SELECT * FROM person VERSION $one;
		SELECT * FROM person VERSION $two;
		SELECT * FROM person VERSION $tre;
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 14)
		So(res[3].Result, ShouldHaveLength, 1)
		So(res[5].Result, ShouldHaveLength, 1)
		So(res[7].Result, ShouldHaveLength, 1)
		So(res[9].Result, ShouldHaveLength, 0)
		So(res[10].Result, ShouldHaveLength, 1)
		So(data.Consume(res[10].Result[0]).Get("test").Data(), ShouldEqual, 1)
		So(res[11].Result, ShouldHaveLength, 1)
		So(data.Consume(res[11].Result[0]).Get("test").Data(), ShouldEqual, 2)
		So(res[12].Result, ShouldHaveLength, 1)
		So(data.Consume(res[12].Result[0]).Get("test").Data(), ShouldEqual, 3)
		So(res[13].Result, ShouldHaveLength, 1)
		So(data.Consume(res[13].Result[0]).Get("test").Data(), ShouldEqual, 3)

	})

	Convey("Test version on a table", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		LET old = time.now();
		CREATE |person:1..3|;
		UPDATE person:1, person:2, person:3 SET test = 1;
		LET one = time.now();
		UPDATE person:1, person:2, person:3 SET test = 2;
		LET two = time.now();
		UPDATE person:1, person:2, person:3 SET test = 3;
		LET tre = time.now();
		SELECT * FROM person VERSION $old;
		SELECT * FROM person VERSION $one;
		SELECT * FROM person VERSION $two;
		SELECT * FROM person VERSION $tre;
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 14)
		So(res[3].Result, ShouldHaveLength, 3)
		So(res[5].Result, ShouldHaveLength, 3)
		So(res[7].Result, ShouldHaveLength, 3)
		So(res[9].Result, ShouldHaveLength, 0)
		So(res[10].Result, ShouldHaveLength, 3)
		So(data.Consume(res[10].Result).Get("0.test").Data(), ShouldEqual, 1)
		So(data.Consume(res[10].Result).Get("1.test").Data(), ShouldEqual, 1)
		So(data.Consume(res[10].Result).Get("2.test").Data(), ShouldEqual, 1)
		So(res[11].Result, ShouldHaveLength, 3)
		So(data.Consume(res[11].Result).Get("0.test").Data(), ShouldEqual, 2)
		So(data.Consume(res[11].Result).Get("1.test").Data(), ShouldEqual, 2)
		So(data.Consume(res[11].Result).Get("2.test").Data(), ShouldEqual, 2)
		So(res[12].Result, ShouldHaveLength, 3)
		So(data.Consume(res[12].Result).Get("0.test").Data(), ShouldEqual, 3)
		So(data.Consume(res[12].Result).Get("1.test").Data(), ShouldEqual, 3)
		So(data.Consume(res[12].Result).Get("2.test").Data(), ShouldEqual, 3)
		So(res[13].Result, ShouldHaveLength, 3)
		So(data.Consume(res[13].Result).Get("0.test").Data(), ShouldEqual, 3)
		So(data.Consume(res[13].Result).Get("1.test").Data(), ShouldEqual, 3)
		So(data.Consume(res[13].Result).Get("2.test").Data(), ShouldEqual, 3)

	})

}
