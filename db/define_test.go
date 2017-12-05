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
		UPDATE @person:test SET test=true, other="text";
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
		UPDATE @person:test SET test=true, other="text";
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
		UPDATE @person:test SET test=true, other=NULL;
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
		UPDATE @person:test SET test=person:other;
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
		UPDATE @person:test SET test=[], test+=person:one, test+=person:two;
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

	Convey("Define a drop table", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE TABLE person DROP;
		UPDATE @person:test;
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
		UPDATE @person:test SET name="Test", test=true;
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
			UPDATE @person:1 SET name="Tobie";
			UPDATE @person:2 SET name="Jaime";
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
		DEFINE FIELD test ON person TYPE number ASSERT ($after >= 0) AND ($after <= 10);
		UPDATE @person:1;
		UPDATE @person:2 SET test = 5;
		UPDATE @person:3 SET test = 50;
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
		DEFINE FIELD test ON person TYPE number ASSERT IF $after != null THEN ($after >= 0) AND ($after <= 10) ELSE true END;
		UPDATE @person:1;
		UPDATE @person:2 SET test = 5;
		UPDATE @person:3 SET test = 50;
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

	Convey("Define an event when a value changes", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE EVENT test ON person WHEN test > 1000 THEN (CREATE temp);
		UPDATE @person:test SET test = 1000;
		UPDATE @person:test SET test = 4000;
		UPDATE @person:test SET test = 2000;
		UPDATE @person:test SET test = 6000;
		SELECT * FROM temp;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 7)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(res[4].Result, ShouldHaveLength, 1)
		So(res[5].Result, ShouldHaveLength, 1)
		So(res[6].Result, ShouldHaveLength, 3)

	})

	Convey("Define an event when a value increases", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE EVENT test ON person WHEN $before.test < $after.test THEN (CREATE temp);
		UPDATE @person:test SET test = 1000;
		UPDATE @person:test SET test = 4000;
		UPDATE @person:test SET test = 2000;
		UPDATE @person:test SET test = 6000;
		SELECT * FROM temp;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 7)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(res[4].Result, ShouldHaveLength, 1)
		So(res[5].Result, ShouldHaveLength, 1)
		So(res[6].Result, ShouldHaveLength, 2)

	})

	Convey("Define an event when a value increases beyond a threshold", t, func() {

		setupDB()

		// IMPORTANT enable test

		txt := `
		USE NS test DB test;
		# DEFINE EVENT test ON person WHEN $before.test < 5000 AND $after.test > 5000 THEN (CREATE temp);
		DEFINE EVENT test ON person WHEN ($before.test < 5000) AND ($after.test > 5000) THEN (CREATE temp);
		UPDATE @person:test SET test = 1000;
		UPDATE @person:test SET test = 4000;
		UPDATE @person:test SET test = 2000;
		UPDATE @person:test SET test = 6000;
		SELECT * FROM temp;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 7)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(res[4].Result, ShouldHaveLength, 1)
		So(res[5].Result, ShouldHaveLength, 1)
		So(res[6].Result, ShouldHaveLength, 1)

	})

	Convey("Define an event on a table, and ensure it is not output with records", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		DEFINE EVENT test ON person WHEN true THEN false;
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

}
