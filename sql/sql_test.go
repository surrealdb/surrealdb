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

package sql

import (
	"testing"
	"time"

	"github.com/abcum/fibre"
	"github.com/abcum/surreal/cnf"
	. "github.com/smartystreets/goconvey/convey"
)

type tester struct {
	skip bool
	sql  string
	err  string
	res  Statement
}

func testerr(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

var c *fibre.Context

func testsql(t *testing.T, test tester) {

	if test.skip {
		Convey(" ❗️ "+test.sql, t, nil)
		return
	}

	s, e := Parse(c, test.sql, nil)

	Convey(test.sql, t, func() {

		if test.err == "" {
			So(e, ShouldBeNil)
			So(s, ShouldResemble, test.res)
		}

		if test.err != "" {
			Convey(testerr(e), func() {
				So(testerr(e), ShouldResemble, test.err)
			})
		}

	})

}

func TestMain(t *testing.T) {

	cnf.Settings = &cnf.Options{}
	cnf.Settings.DB.Base = "*"

	auth := &cnf.Auth{}
	auth.Kind = cnf.AuthKV
	auth.Possible.NS = "*"
	auth.Selected.NS = "*"
	auth.Possible.DB = "*"
	auth.Selected.DB = "*"

	c = fibre.NewContext(nil, nil, nil)
	c.Set("auth", auth)

	var tests = []tester{
		{
			sql: `USE`,
			err: "Found `` but expected `NAMESPACE, NS, DATABASE, DB`",
		},
		{
			sql: `USE NAMESPACE`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `USE NAMESPACE ''`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `USE NAMESPACE name`,
			res: &Query{Statements: []Statement{&UseStatement{
				NS: "name",
			}}},
		},
		{
			sql: `USE NAMESPACE 1`,
			err: "Found `1` but expected `name`",
		},
		{
			sql: `USE NAMESPACE 1.3000`,
			err: "Found `1.3000` but expected `name`",
		},
		{
			sql: `USE NAMESPACE 123.123.123.123`,
			res: &Query{Statements: []Statement{&UseStatement{
				NS: "123.123.123.123",
			}}},
		},
		{
			sql: `USE NAMESPACE {"some":"thing"}`,
			err: "Found `{\"some\":\"thing\"}` but expected `name`",
		},
		{
			sql: `USE NAMESPACE name something`,
			err: "Found `something` but expected `EOF, ;`",
		},
		{
			sql: `USE DATABASE`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `USE DATABASE ''`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `USE DATABASE name`,
			res: &Query{Statements: []Statement{&UseStatement{
				DB: "name",
			}}},
		},
		{
			sql: `USE DATABASE 1`,
			err: "Found `1` but expected `name`",
		},
		{
			sql: `USE DATABASE 1.3000`,
			err: "Found `1.3000` but expected `name`",
		},
		{
			sql: `USE DATABASE 123.123.123.123`,
			res: &Query{Statements: []Statement{&UseStatement{
				DB: "123.123.123.123",
			}}},
		},
		{
			sql: `USE DATABASE {"some":"thing"}`,
			err: "Found `{\"some\":\"thing\"}` but expected `name`",
		},
		{
			sql: `USE DATABASE name something`,
			err: "Found `something` but expected `EOF, ;`",
		},
		{
			sql: "USE NAMESPACE `*` DATABASE `*`",
			res: &Query{Statements: []Statement{&UseStatement{
				NS: "*",
				DB: "*",
			}}},
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

// Ensure the parser can parse a multi-statement query.
func Test_Parse_General(t *testing.T) {

	s := `SELECT a FROM b`
	q, err := Parse(c, s, nil)

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(q.Statements) != 1 {
		t.Fatalf("unexpected statement count: %d", len(q.Statements))
	}

}

// Ensure the parser can parse a multi-statement query.
func Test_Parse_General_Single(t *testing.T) {

	s := `SELECT a FROM b`
	q, err := Parse(c, s, nil)

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(q.Statements) != 1 {
		t.Fatalf("unexpected statement count: %d", len(q.Statements))
	}

}

// Ensure the parser can parse a multi-statement query.
func Test_Parse_General_Multi(t *testing.T) {

	s := `SELECT a FROM b; SELECT c FROM d`
	q, err := Parse(c, s, nil)

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(q.Statements) != 2 {
		t.Fatalf("unexpected statement count: %d", len(q.Statements))
	}

}

func Test_Parse_Queries_Malformed(t *testing.T) {

	var tests = []tester{
		{
			sql: ``,
			err: "Your SQL query is empty",
		},
		{
			sql: "SELECT ` FROM person",
			err: "Found ` FROM person` but expected `expression`",
		},
		{
			sql: `SELECT ' FROM person`,
			err: "Found ` FROM person` but expected `expression`",
		},
		{
			sql: `SELECT " FROM person`,
			err: "Found ` FROM person` but expected `expression`",
		},
		{
			sql: `SELECT "\" FROM person`,
			err: "Found `\" FROM person` but expected `expression`",
		},
		{
			sql: `!`,
			err: "Found `!` but expected `USE, INFO, BEGIN, CANCEL, COMMIT, LET, RETURN, SELECT, CREATE, UPDATE, DELETE, RELATE, DEFINE, REMOVE`",
		},
		{
			sql: `SELECT * FROM person;;;`,
			err: "Found `;` but expected `USE, INFO, BEGIN, CANCEL, COMMIT, LET, RETURN, SELECT, CREATE, UPDATE, DELETE, RELATE, DEFINE, REMOVE`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Let(t *testing.T) {

	var tests = []tester{
		{
			sql: `LET`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `LET name`,
			err: "Found `` but expected `=`",
		},
		{
			sql: `LET name =`,
			err: "Found `=` but expected `expression`",
		},
		{
			sql: `LET name = true`,
			res: &Query{Statements: []Statement{&LetStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"name"},
				What: true,
			}}},
		},
		{
			sql: `LET name = false`,
			res: &Query{Statements: []Statement{&LetStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"name"},
				What: false,
			}}},
		},
		{
			sql: `LET name = "test"`,
			res: &Query{Statements: []Statement{&LetStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"name"},
				What: &Value{"test"},
			}}},
		},
		{
			sql: `LET name = 1`,
			res: &Query{Statements: []Statement{&LetStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"name"},
				What: int64(1),
			}}},
		},
		{
			sql: `LET name = 1.0`,
			res: &Query{Statements: []Statement{&LetStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"name"},
				What: float64(1),
			}}},
		},
		{
			sql: `LET name = 1.1`,
			res: &Query{Statements: []Statement{&LetStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"name"},
				What: float64(1.1),
			}}},
		},
		{
			sql: `LET name = @thing:test`,
			res: &Query{Statements: []Statement{&LetStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"name"},
				What: &Thing{TB: "thing", ID: "test"},
			}}},
		},
		{
			sql: `LET name = {"key": "val"}`,
			res: &Query{Statements: []Statement{&LetStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"name"},
				What: Object{"key": "val"},
			}}},
		},
		{
			sql: `LET name = ["key", "val"]`,
			res: &Query{Statements: []Statement{&LetStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"name"},
				What: Array{"key", "val"},
			}}},
		},
		{
			sql: `LET name = $test`,
			res: &Query{Statements: []Statement{&LetStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"name"},
				What: &Param{ID: "test"},
			}}},
		},
		{
			sql: `LET name = {"key"::"val"}`,
			err: "Invalid JSON: {\"key\"::\"val\"}",
		},
		{
			sql: `LET name = "test" something`,
			err: "Found `something` but expected `EOF, ;`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Return(t *testing.T) {

	var tests = []tester{
		{
			sql: `RETURN`,
			err: "Found `` but expected `expression`",
		},
		{
			sql: `RETURN true`,
			res: &Query{Statements: []Statement{&ReturnStatement{
				KV: "*", NS: "*", DB: "*",
				What: true,
			}}},
		},
		{
			sql: `RETURN true`,
			res: &Query{Statements: []Statement{&ReturnStatement{
				KV: "*", NS: "*", DB: "*",
				What: true,
			}}},
		},
		{
			sql: `RETURN false`,
			res: &Query{Statements: []Statement{&ReturnStatement{
				KV: "*", NS: "*", DB: "*",
				What: false,
			}}},
		},
		{
			sql: `RETURN "test"`,
			res: &Query{Statements: []Statement{&ReturnStatement{
				KV: "*", NS: "*", DB: "*",
				What: &Value{"test"},
			}}},
		},
		{
			sql: `RETURN 1`,
			res: &Query{Statements: []Statement{&ReturnStatement{
				KV: "*", NS: "*", DB: "*",
				What: int64(1),
			}}},
		},
		{
			sql: `RETURN 1.0`,
			res: &Query{Statements: []Statement{&ReturnStatement{
				KV: "*", NS: "*", DB: "*",
				What: float64(1),
			}}},
		},
		{
			sql: `RETURN 1.1`,
			res: &Query{Statements: []Statement{&ReturnStatement{
				KV: "*", NS: "*", DB: "*",
				What: float64(1.1),
			}}},
		},
		{
			sql: `RETURN @thing:test`,
			res: &Query{Statements: []Statement{&ReturnStatement{
				KV: "*", NS: "*", DB: "*",
				What: &Thing{TB: "thing", ID: "test"},
			}}},
		},
		{
			sql: `RETURN {"key": "val"}`,
			res: &Query{Statements: []Statement{&ReturnStatement{
				KV: "*", NS: "*", DB: "*",
				What: Object{"key": "val"},
			}}},
		},
		{
			sql: `RETURN ["key", "val"]`,
			res: &Query{Statements: []Statement{&ReturnStatement{
				KV: "*", NS: "*", DB: "*",
				What: Array{"key", "val"},
			}}},
		},
		{
			sql: `RETURN $test`,
			res: &Query{Statements: []Statement{&ReturnStatement{
				KV: "*", NS: "*", DB: "*",
				What: &Param{ID: "test"},
			}}},
		},
		{
			sql: `RETURN {"key"::"val"}`,
			err: "Invalid JSON: {\"key\"::\"val\"}",
		},
		{
			sql: `RETURN $test something`,
			err: "Found `something` but expected `EOF, ;`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Select(t *testing.T) {

	date, _ := time.Parse("2006-01-02", "1987-06-22")
	nano, _ := time.Parse(time.RFC3339, "1987-06-22T08:30:30.511Z")

	var tests = []tester{
		{
			sql: `SELECT`,
			err: "Found `` but expected `expression`",
		},
		{
			sql: `SELECT FROM`,
			err: "Found `FROM` but expected `expression`",
		},
		{
			sql: `SELECT *`,
			err: "Found `` but expected `FROM`",
		},
		{
			sql: `SELECT * FROM`,
			err: "Found `` but expected `table, or thing`",
		},
		{
			sql: `SELECT * FROM per!son`,
			err: "Found `!` but expected `EOF, ), ;`",
		},
		{
			sql: `SELECT * FROM person;`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
			}}},
		},
		{
			sql: `SELECT * FROM @`,
			err: "Found `@` but expected `table, or thing`",
		},
		{
			sql: `SELECT * FROM @person`,
			err: "Found `@person` but expected `table, or thing`",
		},
		{
			sql: `SELECT * FROM @person:`,
			err: "Found `@person:` but expected `table, or thing`",
		},
		{
			sql: `SELECT * FROM @person WHERE`,
			err: "Found `@person` but expected `table, or thing`",
		},
		{
			sql: `SELECT * FROM person:uuid`,
			err: "Found `:` but expected `EOF, ), ;`",
		},
		{
			sql: "SELECT * FROM 111",
			err: "Found `111` but expected `table, or thing`",
		},
		{
			sql: "SELECT * FROM `111`",
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"111"}},
			}}},
		},
		{
			sql: "SELECT * FROM `2006-01-02`",
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"2006-01-02"}},
			}}},
		},
		{
			sql: "SELECT * FROM `2006-01-02T15:04:05+07:00`",
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"2006-01-02T15:04:05+07:00"}},
			}}},
		},
		{
			sql: "SELECT * FROM `2006-01-02T15:04:05.999999999+07:00`",
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"2006-01-02T15:04:05.999999999+07:00"}},
			}}},
		},
		{
			sql: `SELECT * FROM person`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
			}}},
		},
		{
			sql: `SELECT * FROM person, tweet`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}, &Table{"tweet"}},
			}}},
		},
		{
			sql: `SELECT * FROM @111:1a`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "111", ID: "1a"}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:1a`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "person", ID: "1a"}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:⟨1a⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "person", ID: "1a"}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:{1a}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "person", ID: "1a"}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:123456`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "person", ID: int64(123456)}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:⟨123456⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "person", ID: int64(123456)}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:{123456}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "person", ID: int64(123456)}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:123.456`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "person", ID: float64(123.456)}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:⟨123.456⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "person", ID: float64(123.456)}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:{123.456}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "person", ID: float64(123.456)}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:123.456.789.012`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "person", ID: "123.456.789.012"}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:⟨123.456.789.012⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "person", ID: "123.456.789.012"}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:{123.456.789.012}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "person", ID: "123.456.789.012"}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:⟨1987-06-22⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "person", ID: date}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:{1987-06-22}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "person", ID: date}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:⟨1987-06-22T08:30:30.511Z⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "person", ID: nano}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:{1987-06-22T08:30:30.511Z}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "person", ID: nano}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:⟨A250C5A3-948F-4657-88AD-FF5F27B5B24E⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "person", ID: "A250C5A3-948F-4657-88AD-FF5F27B5B24E"}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:{A250C5A3-948F-4657-88AD-FF5F27B5B24E}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "person", ID: "A250C5A3-948F-4657-88AD-FF5F27B5B24E"}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:⟨8250C5A3-948F-4657-88AD-FF5F27B5B24E⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "person", ID: "8250C5A3-948F-4657-88AD-FF5F27B5B24E"}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:{8250C5A3-948F-4657-88AD-FF5F27B5B24E}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "person", ID: "8250C5A3-948F-4657-88AD-FF5F27B5B24E"}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:⟨Tobie Morgan Hitchcock⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "person", ID: "Tobie Morgan Hitchcock"}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:{Tobie Morgan Hitchcock}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "person", ID: "Tobie Morgan Hitchcock"}},
			}}},
		},
		{
			sql: `SELECT * FROM @⟨email addresses⟩:⟨tobie@abcum.com⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "email addresses", ID: "tobie@abcum.com"}},
			}}},
		},
		{
			sql: `SELECT * FROM @{email addresses}:{tobie@abcum.com}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "email addresses", ID: "tobie@abcum.com"}},
			}}},
		},
		{
			sql: `SELECT * FROM @⟨email addresses⟩:⟨tobie+spam@abcum.com⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "email addresses", ID: "tobie+spam@abcum.com"}},
			}}},
		},
		{
			sql: `SELECT * FROM @{email addresses}:{tobie+spam@abcum.com}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Thing{TB: "email addresses", ID: "tobie+spam@abcum.com"}},
			}}},
		},
		{
			sql: `SELECT * FROM @{person}test:id`,
			err: "Found `@person` but expected `table, or thing`",
		},
		{
			sql: `SELECT * FROM @⟨email addresses⟩:⟨this\qis\nodd⟩`,
			err: "Found `@email addresses:thisqis\nodd` but expected `table, or thing`",
		},
		{
			sql: `SELECT * FROM @{email addresses}:{this\qis\nodd}`,
			err: "Found `@email addresses:thisqis\nodd` but expected `table, or thing`",
		},
		{
			sql: `SELECT *, temp AS test FROM person`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{
					{Expr: &All{}},
					{Expr: &Ident{"temp"}, Alias: &Ident{"test"}},
				},
				What: []Expr{&Table{"person"}},
			}}},
		},
		{
			sql: "SELECT `email addresses` AS emails FROM person",
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{
					{Expr: &Ident{"email addresses"}, Alias: &Ident{"emails"}},
				},
				What: []Expr{&Table{"person"}},
			}}},
		},
		{
			sql: "SELECT emails AS `email addresses` FROM person",
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{
					{Expr: &Ident{"emails"}, Alias: &Ident{"email addresses"}},
				},
				What: []Expr{&Table{"person"}},
			}}},
		},
		{
			sql: "SELECT * FROM person WHERE id = '\x0A'",
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"id"}, Op: EQ, RHS: &Value{"\n"}},
			}}},
		},
		{
			sql: "SELECT * FROM person WHERE id = '\x0D'",
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"id"}, Op: EQ, RHS: &Value{"\r"}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE id = "\b\n\r\t"`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"id"}, Op: EQ, RHS: &Value{"\n\r\t"}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE`,
			err: "Found `` but expected `expression`",
		},
		{
			sql: `SELECT * FROM person WHERE id`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &Ident{"id"},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE id = `,
			err: "Found `` but expected `expression`",
		},
		{
			sql: `SELECT * FROM person WHERE id = 1`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"id"}, Op: EQ, RHS: int64(1)},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE old = EMPTY`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"old"}, Op: EQ, RHS: &Empty{}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE old != EMPTY`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"old"}, Op: NEQ, RHS: &Empty{}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE old = MISSING`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"old"}, Op: EQ, RHS: &Void{}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE old != MISSING`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"old"}, Op: NEQ, RHS: &Void{}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE old IS EMPTY`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"old"}, Op: EQ, RHS: &Empty{}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE old IS NOT EMPTY`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"old"}, Op: NEQ, RHS: &Empty{}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE old IS MISSING`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"old"}, Op: EQ, RHS: &Void{}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE old IS NOT MISSING`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"old"}, Op: NEQ, RHS: &Void{}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE old = true`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"old"}, Op: EQ, RHS: true},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE old = false`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"old"}, Op: EQ, RHS: false},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE id != null AND id > 13.9 AND id < 31 AND id >= 15 AND id <= 29.9`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{
					LHS: &BinaryExpression{
						LHS: &BinaryExpression{
							LHS: &BinaryExpression{
								LHS: &BinaryExpression{
									LHS: &Ident{ID: "id"},
									Op:  NEQ,
									RHS: &Null{},
								},
								Op: AND,
								RHS: &BinaryExpression{
									LHS: &Ident{ID: "id"},
									Op:  GT,
									RHS: float64(13.9),
								},
							},
							Op: AND,
							RHS: &BinaryExpression{
								LHS: &Ident{ID: "id"},
								Op:  LT,
								RHS: int64(31),
							},
						},
						Op: AND,
						RHS: &BinaryExpression{
							LHS: &Ident{ID: "id"},
							Op:  GTE,
							RHS: int64(15),
						},
					},
					Op: AND,
					RHS: &BinaryExpression{
						LHS: &Ident{ID: "id"},
						Op:  LTE,
						RHS: float64(29.9),
					},
				},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE test IN ["London":"Paris"]`,
			err: `Invalid JSON: ["London":"Paris"]`,
		},
		{
			sql: `SELECT * FROM person WHERE test IN ["London","Paris"]`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"test"}, Op: INS, RHS: Array{"London", "Paris"}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE test IS IN ["London","Paris"]`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"test"}, Op: INS, RHS: Array{"London", "Paris"}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE test IS NOT IN ["London","Paris"]`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"test"}, Op: NIS, RHS: Array{"London", "Paris"}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE ["London","Paris"] CONTAINS test`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: Array{"London", "Paris"}, Op: SIN, RHS: &Ident{"test"}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE ["London","Paris"] CONTAINS NOT test`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: Array{"London", "Paris"}, Op: SNI, RHS: &Ident{"test"}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE test = {`,
			err: "Found `{` but expected `expression`",
		},
		{
			sql: `SELECT * FROM person WHERE test = {"name","London"}`,
			err: `Invalid JSON: {"name","London"}`,
		},
		{
			sql: "SELECT * FROM person WHERE {\"name\":\"\x0A\"} = test",
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: Object{"name": "\n"}, Op: EQ, RHS: &Ident{"test"}},
			}}},
		},
		{
			sql: "SELECT * FROM person WHERE test = {\"name\":\"\x0A\"}",
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"test"}, Op: EQ, RHS: Object{"name": "\n"}},
			}}},
		},
		{
			sql: "SELECT * FROM person WHERE test = {\"name\":\"\x0D\"}",
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"test"}, Op: EQ, RHS: Object{"name": "\r"}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE test = {"name":"London"}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"test"}, Op: EQ, RHS: Object{"name": "London"}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE test = {"name":"\b\t\r\n\f\"\\"}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"test"}, Op: EQ, RHS: Object{"name": "\b\t\r\n\f\"\\"}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE test = {"name":{"f":"first", "l":"last"}}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}}},
				What: []Expr{&Table{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"test"}, Op: EQ, RHS: Object{"name": map[string]interface{}{"f": "first", "l": "last"}}},
			}}},
		},
	}

	bday1, _ := time.Parse("2006-01-02", "1987-06-22")
	bday2, _ := time.Parse(time.RFC3339, "1987-06-22T08:00:00Z")
	bday3, _ := time.Parse(time.RFC3339, "1987-06-22T08:30:00.193943735Z")
	bday4, _ := time.Parse(time.RFC3339, "2016-03-14T11:19:31.193943735Z")

	tests = append(tests, tester{
		sql: `SELECT * FROM person WHERE bday >= "1987-06-22" AND bday >= "1987-06-22T08:00:00Z" AND bday >= "1987-06-22T08:30:00.193943735Z" AND bday <= "2016-03-14T11:19:31.193943735Z"`,
		res: &Query{Statements: []Statement{&SelectStatement{
			KV: "*", NS: "*", DB: "*",
			Expr: []*Field{{Expr: &All{}}},
			What: []Expr{&Table{"person"}},
			Cond: &BinaryExpression{
				LHS: &BinaryExpression{
					LHS: &BinaryExpression{
						LHS: &BinaryExpression{
							LHS: &Ident{ID: "bday"},
							Op:  GTE,
							RHS: bday1,
						},
						Op: AND,
						RHS: &BinaryExpression{
							LHS: &Ident{ID: "bday"},
							Op:  GTE,
							RHS: bday2,
						},
					},
					Op: AND,
					RHS: &BinaryExpression{
						LHS: &Ident{ID: "bday"},
						Op:  GTE,
						RHS: bday3,
					},
				},
				Op: AND,
				RHS: &BinaryExpression{
					LHS: &Ident{ID: "bday"},
					Op:  LTE,
					RHS: bday4,
				},
			},
		}}},
	})

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Create(t *testing.T) {

	var tests = []tester{
		{
			sql: `CREATE`,
			err: "Found `` but expected `table, or thing`",
		},
		{
			sql: `CREATE person`,
			res: &Query{Statements: []Statement{&CreateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{
					&Table{"person"},
				},
				Echo: AFTER,
			}}},
		},
		{
			sql: `CREATE person SET 123`,
			err: "Found `123` but expected `field name`",
		},
		{
			sql: `CREATE person SET firstname`,
			err: "Found `` but expected `=, +=, -=`",
		},
		{
			sql: `CREATE person SET firstname = VOID`,
			res: &Query{Statements: []Statement{&CreateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Data: &DataExpression{Data: []*ItemExpression{{LHS: &Ident{"firstname"}, Op: EQ, RHS: &Void{}}}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `CREATE person SET firstname = EMPTY`,
			res: &Query{Statements: []Statement{&CreateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Data: &DataExpression{Data: []*ItemExpression{{LHS: &Ident{"firstname"}, Op: EQ, RHS: &Empty{}}}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `CREATE person SET firstname = "Tobie" something`,
			err: "Found `something` but expected `EOF, ), ;`",
		},
		{
			sql: `CREATE person SET firstname = "Tobie"`,
			res: &Query{Statements: []Statement{&CreateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Data: &DataExpression{Data: []*ItemExpression{{LHS: &Ident{"firstname"}, Op: EQ, RHS: &Value{"Tobie"}}}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `CREATE person MERGE something`,
			err: "Found `something` but expected `json`",
		},
		{
			sql: `CREATE person MERGE {"firstname"::"Tobie"}`,
			err: "Found `{\"firstname\"::\"Tobie\"}` but expected `json`",
		},
		{
			sql: `CREATE person MERGE {"firstname":"Tobie"} something`,
			err: "Found `something` but expected `EOF, ), ;`",
		},
		{
			sql: `CREATE person MERGE {"firstname":"Tobie"}`,
			res: &Query{Statements: []Statement{&CreateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Data: &MergeExpression{Data: Object{"firstname": "Tobie"}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `CREATE person CONTENT something`,
			err: "Found `something` but expected `json`",
		},
		{
			sql: `CREATE person CONTENT {"firstname"::"Tobie"}`,
			err: "Found `{\"firstname\"::\"Tobie\"}` but expected `json`",
		},
		{
			sql: `CREATE person CONTENT {"firstname":"Tobie"} something`,
			err: "Found `something` but expected `EOF, ), ;`",
		},
		{
			sql: `CREATE person CONTENT {"firstname":"Tobie"}`,
			res: &Query{Statements: []Statement{&CreateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Data: &ContentExpression{Data: Object{"firstname": "Tobie"}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `CREATE person RETURN ID`,
			res: &Query{Statements: []Statement{&CreateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Echo: ID,
			}}},
		},
		{
			sql: `CREATE person RETURN NONE`,
			res: &Query{Statements: []Statement{&CreateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Echo: NONE,
			}}},
		},
		{
			sql: `CREATE person RETURN BOTH`,
			res: &Query{Statements: []Statement{&CreateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Echo: BOTH,
			}}},
		},
		{
			sql: `CREATE person RETURN DIFF`,
			res: &Query{Statements: []Statement{&CreateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Echo: DIFF,
			}}},
		},
		{
			sql: `CREATE person RETURN BEFORE`,
			res: &Query{Statements: []Statement{&CreateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Echo: BEFORE,
			}}},
		},
		{
			sql: `CREATE person RETURN AFTER`,
			res: &Query{Statements: []Statement{&CreateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `CREATE person RETURN SOMETHING`,
			err: "Found `SOMETHING` but expected `ID, NONE, INFO, BOTH, DIFF, BEFORE, AFTER`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Update(t *testing.T) {

	var tests = []tester{
		{
			sql: `UPDATE`,
			err: "Found `` but expected `table, or thing`",
		},
		{
			sql: `UPDATE person`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{
					&Table{"person"},
				},
				Echo: AFTER,
			}}},
		},
		{
			sql: `UPDATE person SET 123`,
			err: "Found `123` but expected `field name`",
		},
		{
			sql: `UPDATE person SET firstname`,
			err: "Found `` but expected `=, +=, -=`",
		},
		{
			sql: `UPDATE person SET firstname = VOID`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Data: &DataExpression{Data: []*ItemExpression{{LHS: &Ident{"firstname"}, Op: EQ, RHS: &Void{}}}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `UPDATE person SET firstname = EMPTY`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Data: &DataExpression{Data: []*ItemExpression{{LHS: &Ident{"firstname"}, Op: EQ, RHS: &Empty{}}}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `UPDATE person SET firstname = "Tobie" something`,
			err: "Found `something` but expected `EOF, ), ;`",
		},
		{
			sql: `UPDATE person SET firstname = "Tobie"`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Data: &DataExpression{Data: []*ItemExpression{{LHS: &Ident{"firstname"}, Op: EQ, RHS: &Value{"Tobie"}}}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `UPDATE person DIFF something`,
			err: "Found `something` but expected `json`",
		},
		{
			sql: `UPDATE person DIFF {} something`,
			err: "Found `{}` but expected `json`",
		},
		{
			sql: `UPDATE person DIFF [] something`,
			err: "Found `something` but expected `EOF, ), ;`",
		},
		{
			sql: `UPDATE person DIFF []`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Data: &DiffExpression{Data: Array{}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `UPDATE person MERGE something`,
			err: "Found `something` but expected `json`",
		},
		{
			sql: `UPDATE person MERGE {"firstname"::"Tobie"}`,
			err: "Found `{\"firstname\"::\"Tobie\"}` but expected `json`",
		},
		{
			sql: `UPDATE person MERGE {"firstname":"Tobie"} something`,
			err: "Found `something` but expected `EOF, ), ;`",
		},
		{
			sql: `UPDATE person MERGE {"firstname":"Tobie"}`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Data: &MergeExpression{Data: Object{"firstname": "Tobie"}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `UPDATE person CONTENT something`,
			err: "Found `something` but expected `json`",
		},
		{
			sql: `UPDATE person CONTENT {"firstname"::"Tobie"}`,
			err: "Found `{\"firstname\"::\"Tobie\"}` but expected `json`",
		},
		{
			sql: `UPDATE person CONTENT {"firstname":"Tobie"} something`,
			err: "Found `something` but expected `EOF, ), ;`",
		},
		{
			sql: `UPDATE person CONTENT {"firstname":"Tobie"}`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Data: &ContentExpression{Data: Object{"firstname": "Tobie"}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `UPDATE person RETURN ID`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Echo: ID,
			}}},
		},
		{
			sql: `UPDATE person RETURN NONE`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Echo: NONE,
			}}},
		},
		{
			sql: `UPDATE person RETURN BOTH`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Echo: BOTH,
			}}},
		},
		{
			sql: `UPDATE person RETURN DIFF`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Echo: DIFF,
			}}},
		},
		{
			sql: `UPDATE person RETURN BEFORE`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Echo: BEFORE,
			}}},
		},
		{
			sql: `UPDATE person RETURN AFTER`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `UPDATE person RETURN SOMETHING`,
			err: "Found `SOMETHING` but expected `ID, NONE, INFO, BOTH, DIFF, BEFORE, AFTER`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Delete(t *testing.T) {

	var tests = []tester{
		{
			sql: `DELETE`,
			err: "Found `` but expected `table, or thing`",
		},
		{
			sql: `DELETE FROM`,
			err: "Found `` but expected `table, or thing`",
		},
		{
			sql: `DELETE person`,
			res: &Query{Statements: []Statement{&DeleteStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{
					&Table{"person"},
				},
				Echo: NONE,
			}}},
		},
		{
			sql: `DELETE person RETURN ID`,
			res: &Query{Statements: []Statement{&DeleteStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Echo: ID,
			}}},
		},
		{
			sql: `DELETE person RETURN NONE`,
			res: &Query{Statements: []Statement{&DeleteStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Echo: NONE,
			}}},
		},
		{
			sql: `DELETE person RETURN BOTH`,
			res: &Query{Statements: []Statement{&DeleteStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Echo: BOTH,
			}}},
		},
		{
			sql: `DELETE person RETURN DIFF`,
			res: &Query{Statements: []Statement{&DeleteStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Echo: DIFF,
			}}},
		},
		{
			sql: `DELETE person RETURN BEFORE`,
			res: &Query{Statements: []Statement{&DeleteStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Echo: BEFORE,
			}}},
		},
		{
			sql: `DELETE person RETURN AFTER`,
			res: &Query{Statements: []Statement{&DeleteStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Table{"person"}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `DELETE person RETURN SOMETHING`,
			err: "Found `SOMETHING` but expected `ID, NONE, INFO, BOTH, DIFF, BEFORE, AFTER`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Relate(t *testing.T) {

	var tests = []tester{}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Record(t *testing.T) {

	var tests = []tester{}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Define(t *testing.T) {

	var tests = []tester{
		{
			sql: `DEFINE`,
			err: "Found `` but expected `NAMESPACE, DATABASE, LOGIN, TOKEN, SCOPE, TABLE, FIELD, INDEX, VIEW`",
		},
		// ----------------------------------------------------------------------
		{
			sql: `DEFINE TABLE`,
			err: "Found `` but expected `table`",
		},
		{
			sql: `DEFINE TABLE 111`,
			err: "Found `111` but expected `table`",
		},
		{
			sql: `DEFINE TABLE 111.111`,
			err: "Found `111.111` but expected `table`",
		},
		{
			sql: `DEFINE TABLE person`,
			res: &Query{Statements: []Statement{&DefineTableStatement{
				KV: "*", NS: "*", DB: "*",
				What: Tables{&Table{"person"}},
			}}},
		},
		{
			sql: `DEFINE TABLE person something`,
			err: "Found `something` but expected `EOF, ;`",
		},
		{
			sql: `DEFINE TABLE person SCHEMALESS`,
			res: &Query{Statements: []Statement{&DefineTableStatement{
				KV: "*", NS: "*", DB: "*",
				What: Tables{&Table{"person"}},
				Full: false,
			}}},
		},
		{
			sql: `DEFINE TABLE person SCHEMAFULL`,
			res: &Query{Statements: []Statement{&DefineTableStatement{
				KV: "*", NS: "*", DB: "*",
				What: Tables{&Table{"person"}},
				Full: true,
			}}},
		},
		{
			sql: `DEFINE TABLE person SCHEMALESS something`,
			err: "Found `something` but expected `EOF, ;`",
		},
		{
			sql: `DEFINE TABLE person SCHEMAFULL something`,
			err: "Found `something` but expected `EOF, ;`",
		},
		// ----------------------------------------------------------------------
		{
			sql: `DEFINE FIELD`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `DEFINE FIELD temp`,
			err: "Found `` but expected `ON`",
		},
		{
			sql: `DEFINE FIELD temp ON`,
			err: "Found `` but expected `table`",
		},
		{
			sql: `DEFINE FIELD temp ON person`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE`,
			err: "Found `` but expected `any, url, uuid, color, email, phone, array, object, domain, record, string, number, double, custom, boolean, password, datetime, latitude, longitude`",
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE something`,
			err: "Found `something` but expected `any, url, uuid, color, email, phone, array, object, domain, record, string, number, double, custom, boolean, password, datetime, latitude, longitude`",
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Type: "any",
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE url`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Type: "url",
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE email`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Type: "email",
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE phone`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Type: "phone",
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE array`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Type: "array",
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE object`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Type: "object",
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE string`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Type: "string",
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE number`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Type: "number",
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE double`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Type: "double",
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE custom`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Type: "custom",
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE custom ENUM ["default","notdefault"]`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Type: "custom",
				Enum: Array{"default", "notdefault"},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE custom ENUM ["default" "notdefault"]`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Type: "custom",
				Enum: Array{"default", "notdefault"},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any DEFAULT true`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name:    &Ident{"temp"},
				What:    Tables{&Table{"person"}},
				Type:    "any",
				Default: true,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any DEFAULT false`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name:    &Ident{"temp"},
				What:    Tables{&Table{"person"}},
				Type:    "any",
				Default: false,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any DEFAULT 100`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name:    &Ident{"temp"},
				What:    Tables{&Table{"person"}},
				Type:    "any",
				Default: int64(100),
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any DEFAULT "default"`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name:    &Ident{"temp"},
				What:    Tables{&Table{"person"}},
				Type:    "any",
				Default: &Value{"default"},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any DEFAULT "this\nis\nsome\ntext"`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name:    &Ident{"temp"},
				What:    Tables{&Table{"person"}},
				Type:    "any",
				Default: &Value{"this\nis\nsome\ntext"},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any DEFAULT {"default":true}`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name:    &Ident{"temp"},
				What:    Tables{&Table{"person"}},
				Type:    "any",
				Default: Object{"default": true},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any DEFAULT something`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name:    &Ident{"temp"},
				What:    Tables{&Table{"person"}},
				Type:    "any",
				Default: &Ident{"something"},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any MIN`,
			err: "Found `` but expected `number`",
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any MIN 1`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Type: "any",
				Min:  1,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any MIN 1.0`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Type: "any",
				Min:  1,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any MIN 1.0001`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Type: "any",
				Min:  1.0001,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any MIN something`,
			err: "Found `something` but expected `number`",
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any MAX`,
			err: "Found `` but expected `number`",
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any MAX 100`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Type: "any",
				Max:  100,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any MAX 100.0`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Type: "any",
				Max:  100,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any MAX 100.0001`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Type: "any",
				Max:  100.0001,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any MAX something`,
			err: "Found `something` but expected `number`",
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any CODE`,
			err: "Found `` but expected `script`",
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any CODE "return doc.data.id"`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Type: "any",
				Code: "return doc.data.id",
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any CODE something`,
			err: "Found `something` but expected `script`",
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any MATCH`,
			err: "Found `` but expected `regular expression`",
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any MATCH /.*/`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name:  &Ident{"temp"},
				What:  Tables{&Table{"person"}},
				Type:  "any",
				Match: ".*",
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any MATCH something`,
			err: "Found `something` but expected `regular expression`",
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any NOTNULL`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name:    &Ident{"temp"},
				What:    Tables{&Table{"person"}},
				Type:    "any",
				Notnull: true,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any NOTNULL true`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name:    &Ident{"temp"},
				What:    Tables{&Table{"person"}},
				Type:    "any",
				Notnull: true,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any NOTNULL false`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name:    &Ident{"temp"},
				What:    Tables{&Table{"person"}},
				Type:    "any",
				Notnull: false,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any READONLY`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name:     &Ident{"temp"},
				What:     Tables{&Table{"person"}},
				Type:     "any",
				Readonly: true,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any READONLY true`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name:     &Ident{"temp"},
				What:     Tables{&Table{"person"}},
				Type:     "any",
				Readonly: true,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any READONLY false`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name:     &Ident{"temp"},
				What:     Tables{&Table{"person"}},
				Type:     "any",
				Readonly: false,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any MANDATORY`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name:      &Ident{"temp"},
				What:      Tables{&Table{"person"}},
				Type:      "any",
				Mandatory: true,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any MANDATORY true`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name:      &Ident{"temp"},
				What:      Tables{&Table{"person"}},
				Type:      "any",
				Mandatory: true,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any MANDATORY false`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name:      &Ident{"temp"},
				What:      Tables{&Table{"person"}},
				Type:      "any",
				Mandatory: false,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any VALIDATE`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name:     &Ident{"temp"},
				What:     Tables{&Table{"person"}},
				Type:     "any",
				Validate: true,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any VALIDATE true`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name:     &Ident{"temp"},
				What:     Tables{&Table{"person"}},
				Type:     "any",
				Validate: true,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any VALIDATE false`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name:     &Ident{"temp"},
				What:     Tables{&Table{"person"}},
				Type:     "any",
				Validate: false,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any something`,
			err: "Found `something` but expected `EOF, ;`",
		},
		// ----------------------------------------------------------------------
		{
			sql: `DEFINE INDEX`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `DEFINE INDEX temp`,
			err: "Found `` but expected `ON`",
		},
		{
			sql: `DEFINE INDEX temp ON`,
			err: "Found `` but expected `table`",
		},
		{
			sql: `DEFINE INDEX temp ON person`,
			err: "Found `` but expected `COLUMNS`",
		},
		{
			sql: `DEFINE INDEX temp ON person COLUMNS`,
			err: "Found `` but expected `name, or expression`",
		},
		{
			sql: `DEFINE INDEX temp ON person COLUMNS firstname, lastname`,
			res: &Query{Statements: []Statement{&DefineIndexStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Cols: Idents{&Ident{"firstname"}, &Ident{"lastname"}},
				Uniq: false,
			}}},
		},
		{
			sql: `DEFINE INDEX temp ON person COLUMNS firstname, lastname UNIQUE`,
			res: &Query{Statements: []Statement{&DefineIndexStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Cols: Idents{&Ident{"firstname"}, &Ident{"lastname"}},
				Uniq: true,
			}}},
		},
		{
			sql: `DEFINE INDEX temp ON person COLUMNS firstname, lastname something UNIQUE`,
			err: "Found `something` but expected `EOF, ;`",
		},
		{
			sql: `DEFINE INDEX temp ON person COLUMNS firstname, lastname UNIQUE something`,
			err: "Found `something` but expected `EOF, ;`",
		},
		// ----------------------------------------------------------------------
		{
			sql: `DEFINE VIEW`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `DEFINE VIEW temp`,
			err: "Found `` but expected `AS`",
		},
		{
			sql: `DEFINE VIEW temp AS`,
			err: "Found `` but expected `SELECT`",
		},
		{
			sql: `DEFINE VIEW temp AS SELECT`,
			err: "Found `` but expected `expression`",
		},
		{
			sql: `DEFINE VIEW temp AS SELECT *`,
			err: "Found `` but expected `FROM`",
		},
		{
			sql: `DEFINE VIEW temp AS SELECT * FROM`,
			err: "Found `` but expected `table`",
		},
		{
			sql: `DEFINE VIEW temp AS SELECT * FROM person`,
			res: &Query{Statements: []Statement{&DefineViewStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				Expr: []*Field{{Expr: &All{}}},
				What: Tables{&Table{"person"}},
			}}},
		},
		{
			sql: `DEFINE VIEW temp AS SELECT * FROM person something`,
			err: "Found `something` but expected `EOF, ;`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Remove(t *testing.T) {

	var tests = []tester{
		{
			sql: `REMOVE`,
			err: "Found `` but expected `NAMESPACE, DATABASE, LOGIN, TOKEN, SCOPE, TABLE, FIELD, INDEX, VIEW`",
		},
		// ----------------------------------------------------------------------
		{
			sql: `REMOVE TABLE`,
			err: "Found `` but expected `table`",
		},
		{
			sql: `REMOVE TABLE 111`,
			err: "Found `111` but expected `table`",
		},
		{
			sql: `REMOVE TABLE 111.111`,
			err: "Found `111.111` but expected `table`",
		},
		{
			sql: `REMOVE TABLE person`,
			res: &Query{Statements: []Statement{&RemoveTableStatement{
				KV: "*", NS: "*", DB: "*",
				What: Tables{&Table{"person"}},
			}}},
		},
		{
			sql: `REMOVE TABLE person something`,
			err: "Found `something` but expected `EOF, ;`",
		},
		// ----------------------------------------------------------------------
		{
			sql: `REMOVE FIELD`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `REMOVE FIELD temp`,
			err: "Found `` but expected `ON`",
		},
		{
			sql: `REMOVE FIELD temp ON`,
			err: "Found `` but expected `table`",
		},
		{
			sql: `REMOVE FIELD temp ON person`,
			res: &Query{Statements: []Statement{&RemoveFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
			}}},
		},
		{
			sql: `REMOVE FIELD temp ON person something`,
			err: "Found `something` but expected `EOF, ;`",
		},
		// ----------------------------------------------------------------------
		{
			sql: `REMOVE INDEX`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `REMOVE INDEX temp`,
			err: "Found `` but expected `ON`",
		},
		{
			sql: `REMOVE INDEX temp ON`,
			err: "Found `` but expected `table`",
		},
		{
			sql: `REMOVE INDEX temp ON person`,
			res: &Query{Statements: []Statement{&RemoveIndexStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
			}}},
		},
		{
			sql: `REMOVE INDEX temp ON person something`,
			err: "Found `something` but expected `EOF, ;`",
		},
		// ----------------------------------------------------------------------
		{
			sql: `REMOVE VIEW`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `REMOVE VIEW temp`,
			res: &Query{Statements: []Statement{&RemoveViewStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
			}}},
		},
		{
			sql: `REMOVE VIEW temp something`,
			err: "Found `something` but expected `EOF, ;`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Begin(t *testing.T) {

	var tests = []tester{
		{
			sql: `BEGIN`,
			res: &Query{Statements: []Statement{&BeginStatement{}}},
		},
		{
			sql: `BEGIN something`,
			err: "Found `something` but expected `EOF, ;`",
		},
		{
			sql: `BEGIN TRANSACTION`,
			res: &Query{Statements: []Statement{&BeginStatement{}}},
		},
		{
			sql: `BEGIN TRANSACTION something`,
			err: "Found `something` but expected `EOF, ;`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Cancel(t *testing.T) {

	var tests = []tester{
		{
			sql: `CANCEL`,
			res: &Query{Statements: []Statement{&CancelStatement{}}},
		},
		{
			sql: `CANCEL something`,
			err: "Found `something` but expected `EOF, ;`",
		},
		{
			sql: `CANCEL TRANSACTION`,
			res: &Query{Statements: []Statement{&CancelStatement{}}},
		},
		{
			sql: `CANCEL TRANSACTION something`,
			err: "Found `something` but expected `EOF, ;`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Commit(t *testing.T) {

	var tests = []tester{
		{
			sql: `Commit`,
			res: &Query{Statements: []Statement{&CommitStatement{}}},
		},
		{
			sql: `Commit something`,
			err: "Found `something` but expected `EOF, ;`",
		},
		{
			sql: `Commit TRANSACTION`,
			res: &Query{Statements: []Statement{&CommitStatement{}}},
		},
		{
			sql: `Commit TRANSACTION something`,
			err: "Found `something` but expected `EOF, ;`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}
