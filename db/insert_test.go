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

func TestInsert(t *testing.T) {

	Convey("Insert with invalid value", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		INSERT 1 INTO user;
		INSERT "one" INTO user;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 3)
		So(res[1].Status, ShouldEqual, "ERR")
		So(res[1].Detail, ShouldEqual, "Can not execute INSERT query using value '1'")
		So(res[2].Status, ShouldEqual, "ERR")
		So(res[2].Detail, ShouldEqual, "Can not execute INSERT query using value 'one'")

	})

	Convey("Insert a set of ids from one table to another table", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1 SET test="one";
		CREATE person:2 SET test="two";
		CREATE person:3 SET test="tre";
		INSERT (SELECT id FROM person) INTO user;
		SELECT * FROM person, user;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 6)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(res[4].Result, ShouldHaveLength, 3)
		So(data.Consume(res[4].Result[0]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[4].Result[0]).Get("id").Data(), ShouldResemble, sql.NewThing("user", 1))
		So(data.Consume(res[4].Result[1]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[4].Result[1]).Get("id").Data(), ShouldResemble, sql.NewThing("user", 2))
		So(data.Consume(res[4].Result[2]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[4].Result[2]).Get("id").Data(), ShouldResemble, sql.NewThing("user", 3))
		So(res[5].Result, ShouldHaveLength, 6)
		So(data.Consume(res[5].Result[0]).Get("test").Data(), ShouldEqual, "one")
		So(data.Consume(res[5].Result[0]).Get("id").Data(), ShouldResemble, sql.NewThing("person", 1))
		So(data.Consume(res[5].Result[1]).Get("test").Data(), ShouldEqual, "two")
		So(data.Consume(res[5].Result[1]).Get("id").Data(), ShouldResemble, sql.NewThing("person", 2))
		So(data.Consume(res[5].Result[2]).Get("test").Data(), ShouldEqual, "tre")
		So(data.Consume(res[5].Result[2]).Get("id").Data(), ShouldResemble, sql.NewThing("person", 3))
		So(data.Consume(res[5].Result[3]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[5].Result[3]).Get("id").Data(), ShouldResemble, sql.NewThing("user", 1))
		So(data.Consume(res[5].Result[4]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[5].Result[4]).Get("id").Data(), ShouldResemble, sql.NewThing("user", 2))
		So(data.Consume(res[5].Result[5]).Get("test").Data(), ShouldEqual, nil)
		So(data.Consume(res[5].Result[5]).Get("id").Data(), ShouldResemble, sql.NewThing("user", 3))

	})

	Convey("Insert a set of records from one table to another table", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		CREATE person:1 SET test="one";
		CREATE person:2 SET test="two";
		CREATE person:3 SET test="tre";
		INSERT (SELECT * FROM person) INTO user;
		SELECT * FROM person, user;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 6)
		So(res[1].Result, ShouldHaveLength, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(res[3].Result, ShouldHaveLength, 1)
		So(res[4].Result, ShouldHaveLength, 3)
		So(data.Consume(res[4].Result[0]).Get("test").Data(), ShouldEqual, "one")
		So(data.Consume(res[4].Result[0]).Get("id").Data(), ShouldResemble, sql.NewThing("user", 1))
		So(data.Consume(res[4].Result[1]).Get("test").Data(), ShouldEqual, "two")
		So(data.Consume(res[4].Result[1]).Get("id").Data(), ShouldResemble, sql.NewThing("user", 2))
		So(data.Consume(res[4].Result[2]).Get("test").Data(), ShouldEqual, "tre")
		So(data.Consume(res[4].Result[2]).Get("id").Data(), ShouldResemble, sql.NewThing("user", 3))
		So(res[5].Result, ShouldHaveLength, 6)
		So(data.Consume(res[5].Result[0]).Get("test").Data(), ShouldEqual, "one")
		So(data.Consume(res[5].Result[0]).Get("id").Data(), ShouldResemble, sql.NewThing("person", 1))
		So(data.Consume(res[5].Result[1]).Get("test").Data(), ShouldEqual, "two")
		So(data.Consume(res[5].Result[1]).Get("id").Data(), ShouldResemble, sql.NewThing("person", 2))
		So(data.Consume(res[5].Result[2]).Get("test").Data(), ShouldEqual, "tre")
		So(data.Consume(res[5].Result[2]).Get("id").Data(), ShouldResemble, sql.NewThing("person", 3))
		So(data.Consume(res[5].Result[3]).Get("test").Data(), ShouldEqual, "one")
		So(data.Consume(res[5].Result[3]).Get("id").Data(), ShouldResemble, sql.NewThing("user", 1))
		So(data.Consume(res[5].Result[4]).Get("test").Data(), ShouldEqual, "two")
		So(data.Consume(res[5].Result[4]).Get("id").Data(), ShouldResemble, sql.NewThing("user", 2))
		So(data.Consume(res[5].Result[5]).Get("test").Data(), ShouldEqual, "tre")
		So(data.Consume(res[5].Result[5]).Get("id").Data(), ShouldResemble, sql.NewThing("user", 3))

	})

	Convey("Insert a set of records from data with an ID", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		LET data = { "id": 360, "admin":true, "login":"joe" };
		INSERT $data INTO users;
		INSERT $data INTO users;
		SELECT * FROM users;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[2].Result, ShouldHaveLength, 1)
		So(data.Consume(res[2].Result[0]).Get("login").Data(), ShouldEqual, "joe")
		So(data.Consume(res[2].Result[0]).Get("meta.id").Data(), ShouldEqual, 360)
		So(res[3].Status, ShouldEqual, "ERR_EX")
		So(res[3].Detail, ShouldEqual, "Database record 'users:360' already exists")
		So(res[4].Result, ShouldHaveLength, 1)
		So(data.Consume(res[4].Result[0]).Get("login").Data(), ShouldEqual, "joe")
		So(data.Consume(res[4].Result[0]).Get("meta.id").Data(), ShouldEqual, 360)

	})

	Convey("Insert a set of records from data without an ID", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		LET data = { "admin":true, "login":"tom" };
		INSERT $data INTO users;
		SELECT * FROM users;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[2].Result, ShouldHaveLength, 1)
		So(data.Consume(res[2].Result[0]).Get("login").Data(), ShouldEqual, "tom")
		So(data.Consume(res[2].Result[0]).Get("meta.id").Data(), ShouldHaveLength, 20)
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("login").Data(), ShouldEqual, "tom")
		So(data.Consume(res[3].Result[0]).Get("meta.id").Data(), ShouldHaveLength, 20)

	})

	Convey("Insert a set of records from an array of data with IDs", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		LET data = [
		 { "id": 360, "admin":true, "login":"joe" },
		 { "id": 200, "admin":false, "login":"mike" },
		 { "id": "test", "admin":false, "login":"tester" },
		];
		INSERT $data INTO users;
		INSERT $data INTO users;
		SELECT * FROM users;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[2].Result, ShouldHaveLength, 3)
		So(data.Consume(res[2].Result[0]).Get("login").Data(), ShouldEqual, "joe")
		So(data.Consume(res[2].Result[0]).Get("meta.id").Data(), ShouldEqual, 360)
		So(data.Consume(res[2].Result[1]).Get("login").Data(), ShouldEqual, "mike")
		So(data.Consume(res[2].Result[1]).Get("meta.id").Data(), ShouldEqual, 200)
		So(data.Consume(res[2].Result[2]).Get("login").Data(), ShouldEqual, "tester")
		So(data.Consume(res[2].Result[2]).Get("meta.id").Data(), ShouldEqual, "test")
		So(res[3].Status, ShouldEqual, "ERR_EX")
		So(res[4].Result, ShouldHaveLength, 3)
		So(data.Consume(res[4].Result[0]).Get("login").Data(), ShouldEqual, "mike")
		So(data.Consume(res[4].Result[0]).Get("meta.id").Data(), ShouldEqual, 200)
		So(data.Consume(res[4].Result[1]).Get("login").Data(), ShouldEqual, "joe")
		So(data.Consume(res[4].Result[1]).Get("meta.id").Data(), ShouldEqual, 360)
		So(data.Consume(res[4].Result[2]).Get("login").Data(), ShouldEqual, "tester")
		So(data.Consume(res[4].Result[2]).Get("meta.id").Data(), ShouldEqual, "test")

	})

	Convey("Insert a set of records from an array of data without IDs", t, func() {

		setupDB()

		txt := `
		USE NS test DB test;
		LET data = [
		 { "admin":true, "login":"tom" },
		];
		INSERT $data INTO users;
		SELECT * FROM users;
		`

		res, err := Execute(setupKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 4)
		So(res[2].Result, ShouldHaveLength, 1)
		So(data.Consume(res[2].Result[0]).Get("login").Data(), ShouldEqual, "tom")
		So(data.Consume(res[2].Result[0]).Get("meta.id").Data(), ShouldHaveLength, 20)
		So(res[3].Result, ShouldHaveLength, 1)
		So(data.Consume(res[3].Result[0]).Get("login").Data(), ShouldEqual, "tom")
		So(data.Consume(res[3].Result[0]).Get("meta.id").Data(), ShouldHaveLength, 20)

	})

}
