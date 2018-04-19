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

func TestDefine(t *testing.T) {

	Convey("Define a namespace", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE NAMESPACE test;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Status, ShouldEqual, "OK")

	})

	Convey("Define a database", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE DATABASE test;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Status, ShouldEqual, "OK")

	})

	Convey("Define a scope", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE SCOPE test SESSION 1h
			SIGNUP AS (
				IF $ip IN ["127.0.0.1", "213.172.165.134"] THEN
					(CREATE user SET email=$user, pass=bcrypt.generate($pass))
				END
			)
			SIGNIN AS (
				SELECT * FROM user WHERE email=$user AND bcrypt.compare(pass, $pass)
			)
			ON SIGNUP (
				UPDATE $id SET times.created=time.now();
				CREATE activity SET kind="signup", user=$id;
			)
			ON SIGNIN (
				UPDATE $id SET times.login=time.now();
				CREATE activity SET kind="signin", user=$id;
			)
		;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 2)
		So(res[1].Status, ShouldEqual, "OK")

	})

	Convey("Define a schemaless table", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE TABLE person SCHEMALESS;
		DEFINE FIELD test ON person TYPE boolean;
		UPDATE person:test SET test=true, other="text";
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldEqual, true)
		So(data.Consume(res[3].Result[0]).Get("other").Data(), ShouldEqual, "text")

	})

	Convey("Define a schemafull table", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD test ON person TYPE boolean;
		UPDATE person:test SET test=true, other="text";
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(data.Consume(res[3].Result[0]).Data(), ShouldResemble, map[string]interface{}{
			"id": &sql.Thing{"person", "test"},
			"meta": map[string]interface{}{
				"tb": "person",
				"id": "test",
			},
			"test": true,
		})

	})

	Convey("Define a schemafull table with nil values", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD test ON person TYPE boolean;
		UPDATE person:test SET test=true, other=NULL;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(data.Consume(res[3].Result[0]).Data(), ShouldResemble, map[string]interface{}{
			"id": &sql.Thing{"person", "test"},
			"meta": map[string]interface{}{
				"tb": "person",
				"id": "test",
			},
			"test": true,
		})

	})

	Convey("Define a schemafull table with nested records", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD test ON person TYPE record (person);
		UPDATE person:test SET test=person:other;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(data.Consume(res[3].Result[0]).Data(), ShouldResemble, map[string]interface{}{
			"id": &sql.Thing{"person", "test"},
			"meta": map[string]interface{}{
				"tb": "person",
				"id": "test",
			},
			"test": &sql.Thing{"person", "other"},
		})

	})

	Convey("Define a schemafull table with nested set records", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD test ON person TYPE array;
		DEFINE FIELD test.* ON person TYPE record (person);
		UPDATE person:test SET test=[], test+=person:one, test+=person:two;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(data.Consume(res[4].Result[0]).Data(), ShouldResemble, map[string]interface{}{
			"id": &sql.Thing{"person", "test"},
			"meta": map[string]interface{}{
				"tb": "person",
				"id": "test",
			},
			"test": []interface{}{
				&sql.Thing{"person", "one"},
				&sql.Thing{"person", "two"},
			},
		})

	})

	Convey("Convert a schemaless to schemafull table, and ensure schemaless fields are still output", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE TABLE person SCHEMALESS;
		UPDATE person:test SET test=true, other="text";
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD test ON person TYPE boolean;
		SELECT * FROM person;
		DEFINE FIELD other ON person TYPE string;
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 8)
		So(data.Consume(res[2].Result[0]).Get("test").Data(), ShouldEqual, true)
		So(data.Consume(res[2].Result[0]).Get("other").Data(), ShouldEqual, "text")
		So(data.Consume(res[5].Result[0]).Get("test").Data(), ShouldEqual, true)
		So(data.Consume(res[5].Result[0]).Get("other").Data(), ShouldEqual, "text")
		So(data.Consume(res[7].Result[0]).Get("test").Data(), ShouldEqual, true)
		So(data.Consume(res[7].Result[0]).Get("other").Data(), ShouldEqual, "text")

	})

	Convey("Define a drop table", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE TABLE person DROP;
		UPDATE person:test;
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 0)

	})

	Convey("Define a foreign table", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE TABLE temp AS SELECT name FROM person;
		UPDATE person:test SET name="Test", test=true;
		SELECT * FROM person;
		SELECT * FROM temp;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("meta.id").Data(), ShouldEqual, "test")
		So(data.Consume(res[3].Result[0]).Get("meta.tb").Data(), ShouldEqual, "person")
		So(data.Consume(res[3].Result[0]).Get("name").Data(), ShouldEqual, "Test")
		So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldEqual, true)
		So(res[4].Result, ShouldHaveLength, 1)
		So(data.Consume(res[4].Result[0]).Get("meta.id").Data(), ShouldEqual, "test")
		So(data.Consume(res[4].Result[0]).Get("meta.tb").Data(), ShouldEqual, "temp")
		So(data.Consume(res[4].Result[0]).Get("name").Data(), ShouldEqual, "Test")
		So(data.Consume(res[4].Result[0]).Get("test").Data(), ShouldEqual, nil)

	})

	Convey("Define a table with permission specified so only specified records are visible", t, func() {

		setupDB()

		func() {

			txt := `
			USE NS test DB test;
			DEFINE TABLE person PERMISSIONS FOR SELECT WHERE string.startsWith(name, "J") FOR CREATE, UPDATE FULL;
			`

			res, err := Execute(setupKV(), txt, nil)
			So(err, ShouldBeNil)
			So(res, ShouldHaveLength, 2)

		}()

		func() {

			txt := `
			USE NS test DB test;
			UPDATE person:1 SET name="Tobie";
			UPDATE person:2 SET name="Jaime";
			SELECT * FROM person;
			`

			res, err := Execute(setupSC(), txt, nil)
			So(err, ShouldBeNil)
			So(res, ShouldHaveLength, 4)
			So(res[1].Result, ShouldHaveLength, 1)
			So(res[2].Result, ShouldHaveLength, 1)
			So(res[3].Result, ShouldHaveLength, 1)
			So(data.Consume(res[3].Result[0]).Get("meta.id").Data(), ShouldEqual, 2)

		}()

	})

	Convey("Assert the value of a field", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE FIELD test ON person TYPE number ASSERT $after >= 0 AND $after <= 10;
		UPDATE person:1;
		UPDATE person:2 SET test = 5;
		UPDATE person:3 SET test = 50;
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 6)
		So(res[2].Status, ShouldEqual, "ERR_FD")
		So(res[3].Status, ShouldEqual, "OK")
		So(res[4].Status, ShouldEqual, "ERR_FD")
		So(res[5].Result, ShouldHaveLength, 1)

	})

	Convey("Assert the value of a field if it has been set", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE FIELD test ON person TYPE number ASSERT IF $after != null THEN $after >= 0 AND $after <= 10 ELSE true END;
		UPDATE person:1;
		UPDATE person:2 SET test = 5;
		UPDATE person:3 SET test = 50;
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 6)
		So(res[2].Status, ShouldEqual, "OK")
		So(res[3].Status, ShouldEqual, "OK")
		So(res[4].Status, ShouldEqual, "ERR_FD")
		So(res[5].Result, ShouldHaveLength, 2)

	})

	Convey("Specify the priority of a field so that it is processed after any dependent fields", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE FIELD name.first ON person;
		DEFINE FIELD name.last ON person;
		DEFINE FIELD name.full ON person VALUE string.join(' ', name.first, name.last) PRIORITY 10;
		DEFINE FIELD name.alias ON person VALUE string.join(' ', name.full, "(aka. Toboman)") PRIORITY 20;
		UPDATE person:test SET name.first="Tobias", name.last="Ottoman";
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 6)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[2].Status, ShouldEqual, "OK")
		So(res[3].Status, ShouldEqual, "OK")
		So(res[4].Status, ShouldEqual, "OK")
		So(res[5].Result, ShouldHaveLength, 1)
		So(data.Consume(res[5].Result[0]).Get("name.first").Data(), ShouldEqual, "Tobias")
		So(data.Consume(res[5].Result[0]).Get("name.last").Data(), ShouldEqual, "Ottoman")
		So(data.Consume(res[5].Result[0]).Get("name.full").Data(), ShouldEqual, "Tobias Ottoman")
		So(data.Consume(res[5].Result[0]).Get("name.alias").Data(), ShouldEqual, "Tobias Ottoman (aka. Toboman)")

	})

	Convey("Specify the permissions of a field so that it is only visible to the correct authentication levels", t, func() {

		setupDB()

		func() {

			txt := `
			USE NS test DB test;
			DEFINE TABLE person PERMISSIONS FULL;
			DEFINE FIELD name ON person PERMISSIONS FULL;
			DEFINE FIELD pass ON person PERMISSIONS NONE;
			DEFINE FIELD test ON person PERMISSIONS FOR CREATE, UPDATE FULL FOR SELECT NONE;
			DEFINE FIELD temp ON person PERMISSIONS NONE;
			DEFINE FIELD temp.test ON person PERMISSIONS FULL;
			UPDATE person:test SET name="Tobias", pass="qhmyjahdc4", test="k5n87urq8l", temp.test="zw3wf5ls39";
			SELECT * FROM person;
			`

			res, err := Execute(setupKV(), txt, nil)
			So(err, ShouldBeNil)
			So(res, ShouldHaveLength, 9)
			So(res[7].Result, ShouldHaveLength, 1)
			So(data.Consume(res[7].Result[0]).Get("name").Data(), ShouldEqual, "Tobias")
			So(data.Consume(res[7].Result[0]).Get("pass").Data(), ShouldEqual, "qhmyjahdc4")
			So(data.Consume(res[7].Result[0]).Get("test").Data(), ShouldEqual, "k5n87urq8l")
			So(data.Consume(res[7].Result[0]).Get("temp.test").Data(), ShouldEqual, "zw3wf5ls39")
			So(res[8].Result, ShouldHaveLength, 1)
			So(data.Consume(res[8].Result[0]).Get("name").Data(), ShouldEqual, "Tobias")
			So(data.Consume(res[8].Result[0]).Get("pass").Data(), ShouldEqual, "qhmyjahdc4")
			So(data.Consume(res[8].Result[0]).Get("test").Data(), ShouldEqual, "k5n87urq8l")
			So(data.Consume(res[8].Result[0]).Get("temp.test").Data(), ShouldEqual, "zw3wf5ls39")

		}()

		func() {

			txt := `
			USE NS test DB test;
			CREATE person:1 SET name="Silvana", pass="1f65flhfvq", test="35aptguqoj", temp.test="h08ryx3519";
			UPDATE person:2 SET name="Jonathan", pass="8k796m5mmj", test="1lzdhd6wzg", temp.test="xurnxp8a1e";
			SELECT * FROM person ORDER BY name;
			`

			res, err := Execute(setupSC(), txt, nil)
			So(err, ShouldBeNil)
			So(res, ShouldHaveLength, 4)
			So(res[1].Result, ShouldHaveLength, 1)
			So(data.Consume(res[1].Result[0]).Get("name").Data(), ShouldEqual, "Silvana")
			So(data.Consume(res[1].Result[0]).Get("pass").Data(), ShouldEqual, nil)
			So(data.Consume(res[1].Result[0]).Get("test").Data(), ShouldEqual, nil)
			So(data.Consume(res[1].Result[0]).Get("temp.test").Data(), ShouldEqual, nil)
			So(res[2].Result, ShouldHaveLength, 1)
			So(data.Consume(res[2].Result[0]).Get("name").Data(), ShouldEqual, "Jonathan")
			So(data.Consume(res[2].Result[0]).Get("pass").Data(), ShouldEqual, nil)
			So(data.Consume(res[2].Result[0]).Get("test").Data(), ShouldEqual, nil)
			So(data.Consume(res[2].Result[0]).Get("temp.test").Data(), ShouldEqual, nil)
			So(res[3].Result, ShouldHaveLength, 3)
			So(data.Consume(res[3].Result[0]).Get("name").Data(), ShouldEqual, "Jonathan")
			So(data.Consume(res[3].Result[0]).Get("pass").Data(), ShouldEqual, nil)
			So(data.Consume(res[3].Result[0]).Get("test").Data(), ShouldEqual, nil)
			So(data.Consume(res[3].Result[1]).Get("name").Data(), ShouldEqual, "Silvana")
			So(data.Consume(res[3].Result[1]).Get("pass").Data(), ShouldEqual, nil)
			So(data.Consume(res[3].Result[1]).Get("test").Data(), ShouldEqual, nil)
			So(data.Consume(res[3].Result[2]).Get("name").Data(), ShouldEqual, "Tobias")
			So(data.Consume(res[3].Result[2]).Get("pass").Data(), ShouldEqual, nil)
			So(data.Consume(res[3].Result[2]).Get("test").Data(), ShouldEqual, nil)
			So(data.Consume(res[3].Result[2]).Get("temp.test").Data(), ShouldEqual, nil)

		}()

		func() {

			txt := `
			USE NS test DB test;
			SELECT * FROM person ORDER BY name;
			`

			res, err := Execute(setupKV(), txt, nil)
			So(err, ShouldBeNil)
			So(res, ShouldHaveLength, 2)
			So(res[1].Result, ShouldHaveLength, 3)
			So(data.Consume(res[1].Result[0]).Get("name").Data(), ShouldEqual, "Jonathan")
			So(data.Consume(res[1].Result[0]).Get("pass").Data(), ShouldEqual, nil)
			So(data.Consume(res[1].Result[0]).Get("test").Data(), ShouldEqual, "1lzdhd6wzg")
			So(data.Consume(res[1].Result[1]).Get("name").Data(), ShouldEqual, "Silvana")
			So(data.Consume(res[1].Result[1]).Get("pass").Data(), ShouldEqual, nil)
			So(data.Consume(res[1].Result[1]).Get("test").Data(), ShouldEqual, "35aptguqoj")
			So(data.Consume(res[1].Result[2]).Get("name").Data(), ShouldEqual, "Tobias")
			So(data.Consume(res[1].Result[2]).Get("pass").Data(), ShouldEqual, "qhmyjahdc4")
			So(data.Consume(res[1].Result[2]).Get("test").Data(), ShouldEqual, "k5n87urq8l")

		}()

	})

	Convey("Define an event when a value changes", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE EVENT test ON person WHEN test > 1000 THEN (CREATE temp; CREATE test);
		UPDATE person:test SET test = 1000;
		UPDATE person:test SET test = 4000;
		UPDATE person:test SET test = 2000;
		UPDATE person:test SET test = 6000;
		SELECT * FROM temp;
		SELECT * FROM test;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 8)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(res[4].Result, ShouldHaveLength, 1)
		So(res[5].Result, ShouldHaveLength, 1)
		So(res[6].Result, ShouldHaveLength, 3)
		So(res[7].Result, ShouldHaveLength, 3)

	})

	Convey("Define an event when a value increases", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE EVENT test ON person WHEN $before.test < $after.test THEN (CREATE temp; CREATE test);
		UPDATE person:test SET test = 1000;
		UPDATE person:test SET test = 4000;
		UPDATE person:test SET test = 2000;
		UPDATE person:test SET test = 6000;
		SELECT * FROM temp;
		SELECT * FROM test;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 8)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(res[4].Result, ShouldHaveLength, 1)
		So(res[5].Result, ShouldHaveLength, 1)
		So(res[6].Result, ShouldHaveLength, 2)
		So(res[7].Result, ShouldHaveLength, 2)

	})

	Convey("Define an event when a value increases beyond a threshold", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE EVENT test ON person WHEN $before.test < 5000 AND $after.test > 5000 THEN (CREATE temp; CREATE test);
		UPDATE person:test SET test = 1000;
		UPDATE person:test SET test = 4000;
		UPDATE person:test SET test = 2000;
		UPDATE person:test SET test = 6000;
		SELECT * FROM temp;
		SELECT * FROM test;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 8)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(res[4].Result, ShouldHaveLength, 1)
		So(res[5].Result, ShouldHaveLength, 1)
		So(res[6].Result, ShouldHaveLength, 1)
		So(res[7].Result, ShouldHaveLength, 1)

	})

	Convey("Define an event for both CREATE and UPDATE events separately", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE EVENT created ON person WHEN $method = "CREATE" THEN (CREATE created);
		DEFINE EVENT updated ON person WHEN $method = "UPDATE" THEN (CREATE updated);
		CREATE person:test SET test = 1000;
		UPDATE person:test SET test = 4000;
		UPDATE person:test SET test = 2000;
		UPDATE person:test SET test = 2000;
		UPDATE person:test SET test = 6000;
		SELECT * FROM created;
		SELECT * FROM updated;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 10)
		So(res[3].Result, ShouldHaveLength, 1)
		So(res[4].Result, ShouldHaveLength, 1)
		So(res[5].Result, ShouldHaveLength, 1)
		So(res[6].Result, ShouldHaveLength, 1)
		So(res[7].Result, ShouldHaveLength, 1)
		So(res[8].Result, ShouldHaveLength, 1)
		So(res[9].Result, ShouldHaveLength, 3)

	})

	Convey("Define an event when a value changes and set a foreign key on another table", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE EVENT test ON person WHEN $before.fk != $after.fk THEN (UPDATE $after.fk SET fk = $this);
		UPDATE person:test SET fk = other:test;
		SELECT * FROM other;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("fk").Data(), ShouldResemble, &sql.Thing{"person", "test"})

	})

	Convey("Define an event when a value changes and update a foreign key array on another table", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE EVENT test ON person WHEN $before.fk != $after.fk THEN (UPDATE $after.fk SET fks += $this);
		UPDATE person:one SET fk = other:test;
		UPDATE person:two SET fk = other:test;
		SELECT * FROM other;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(res[4].Result, ShouldHaveLength, 1)
		So(data.Consume(res[4].Result[0]).Get("fks").Data(), ShouldResemble, []interface{}{
			&sql.Thing{"person", "one"},
			&sql.Thing{"person", "two"},
		})

	})

	Convey("Define an event when a value changes and update and delete from a foreign key array on another table", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE EVENT test ON person WHEN $before.fk != $after.fk THEN (
			IF $method != "DELETE" THEN
				(UPDATE $after.fk SET fks += $this)
			ELSE
				(UPDATE $before.fk SET fks -= $this)
			END
		);
		UPDATE person:one SET fk = other:test;
		UPDATE person:two SET fk = other:test;
		DELETE FROM person;
		SELECT * FROM other;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 6)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(res[4].Result, ShouldHaveLength, 0)
		So(res[5].Result, ShouldHaveLength, 1)
		So(data.Consume(res[5].Result[0]).Get("fks.length").Data(), ShouldEqual, 0)

	})

	Convey("Define an event on a table, and ensure it is not output with records", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE EVENT test ON person WHEN true THEN (CREATE test);
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[2].Result, ShouldHaveLength, 0)

	})

	Convey("Define an field on a table, and ensure it is not output with records", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE FIELD test ON person;
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[2].Result, ShouldHaveLength, 0)

	})

	Convey("Define an index on a table, and ensure it is not output with records", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE INDEX test ON person COLUMNS test;
		SELECT * FROM person;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[2].Result, ShouldHaveLength, 0)

	})

	Convey("Define an index on a table, and ensure it allows duplicate record values", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE INDEX test ON person COLUMNS account, email;
		UPDATE person:one SET account="demo", email="info@demo.com";
		UPDATE person:one SET account="demo", email="info@demo.com";
		UPDATE person:one SET account="demo", email="info@demo.com";
		UPDATE person:two SET account="demo", email="info@demo.com";
		UPDATE person:tre SET account="demo", email="info@demo.com";
		SELECT * FROM person ORDER BY meta.id;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 8)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[2].Status, ShouldEqual, "OK")
		So(res[3].Result, ShouldHaveLength, 1)
		So(res[3].Status, ShouldEqual, "OK")
		So(res[4].Result, ShouldHaveLength, 1)
		So(res[4].Status, ShouldEqual, "OK")
		So(res[5].Result, ShouldHaveLength, 1)
		So(res[5].Status, ShouldEqual, "OK")
		So(res[6].Result, ShouldHaveLength, 1)
		So(res[6].Status, ShouldEqual, "OK")
		So(res[7].Result, ShouldHaveLength, 3)
		So(data.Consume(res[7].Result[0]).Get("id").Data(), ShouldResemble, &sql.Thing{"person", "one"})
		So(data.Consume(res[7].Result[1]).Get("id").Data(), ShouldResemble, &sql.Thing{"person", "tre"})
		So(data.Consume(res[7].Result[2]).Get("id").Data(), ShouldResemble, &sql.Thing{"person", "two"})

	})

	Convey("Define a unique index on a table, and ensure it prevents duplicate record values", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE INDEX test ON person COLUMNS account, email UNIQUE;
		UPDATE person:one SET account="demo", email="info@demo.com";
		UPDATE person:one SET account="demo", email="info@demo.com";
		UPDATE person:one SET account="demo", email="info@demo.com";
		UPDATE person:two SET account="demo", email="info@demo.com";
		UPDATE person:tre SET account="demo", email="info@demo.com";
		SELECT * FROM person ORDER BY meta.id;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 8)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[2].Status, ShouldEqual, "OK")
		So(res[3].Result, ShouldHaveLength, 1)
		So(res[3].Status, ShouldEqual, "OK")
		So(res[4].Result, ShouldHaveLength, 1)
		So(res[4].Status, ShouldEqual, "OK")
		So(res[5].Result, ShouldHaveLength, 0)
		So(res[5].Status, ShouldEqual, "ERR_IX")
		So(res[6].Result, ShouldHaveLength, 0)
		So(res[6].Status, ShouldEqual, "ERR_IX")
		So(res[7].Result, ShouldHaveLength, 1)
		So(data.Consume(res[7].Result[0]).Get("id").Data(), ShouldResemble, &sql.Thing{"person", "one"})

	})

	Convey("Redefine a unique index on a table, and ensure it prevents duplicate record values", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE INDEX test ON person COLUMNS account, email UNIQUE;
		UPDATE person:one SET account="demo", email="info@demo.com";
		UPDATE person:two SET account="demo", email="info@demo.com";
		UPDATE person:tre SET account="demo", email="info@demo.com";
		SELECT * FROM person ORDER BY meta.id;
		DEFINE INDEX test ON person COLUMNS account, email UNIQUE;
		UPDATE person:one SET account="demo", email="info@demo.com";
		UPDATE person:two SET account="demo", email="info@demo.com";
		UPDATE person:tre SET account="demo", email="info@demo.com";
		SELECT * FROM person ORDER BY meta.id;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 11)
		So(res[1].Status, ShouldEqual, "OK")
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[2].Status, ShouldEqual, "OK")
		So(res[3].Result, ShouldHaveLength, 0)
		So(res[3].Status, ShouldEqual, "ERR_IX")
		So(res[4].Result, ShouldHaveLength, 0)
		So(res[4].Status, ShouldEqual, "ERR_IX")
		So(res[5].Result, ShouldHaveLength, 1)
		So(data.Consume(res[5].Result[0]).Get("id").Data(), ShouldResemble, &sql.Thing{"person", "one"})
		So(res[6].Status, ShouldEqual, "OK")
		So(res[7].Result, ShouldHaveLength, 1)
		So(res[7].Status, ShouldEqual, "OK")
		So(res[8].Result, ShouldHaveLength, 0)
		So(res[8].Status, ShouldEqual, "ERR_IX")
		So(res[9].Result, ShouldHaveLength, 0)
		So(res[9].Status, ShouldEqual, "ERR_IX")
		So(res[10].Result, ShouldHaveLength, 1)
		So(data.Consume(res[10].Result[0]).Get("id").Data(), ShouldResemble, &sql.Thing{"person", "one"})

	})

}
