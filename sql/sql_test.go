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
	"fmt"
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
	str  string
	res  Statement
}

func testerr(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

var c *fibre.Context

type Selfer interface{}

func testsql(t *testing.T, test tester) {

	if test.skip {
		Convey(" ❗️ "+test.sql, t, nil)
		return
	}

	s, e := Parse(c, test.sql)

	Convey(test.sql, t, func() {

		if test.err != "" {
			Convey(testerr(e), func() {
				So(testerr(e), ShouldResemble, test.err)
			})
		}

		if test.err == "" {
			So(e, ShouldBeNil)
			So(s, ShouldResemble, test.res)
			if test.str != "" {
				So(fmt.Sprint(test.res.(*Query).Statements[0]), ShouldEqual, test.str)
			} else {
				So(fmt.Sprint(test.res.(*Query).Statements[0]), ShouldEqual, test.sql)
			}
		}

	})

}

func TestMain(t *testing.T) {

	cnf.Settings = &cnf.Options{}
	cnf.Settings.DB.Path = "memory"
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
			err: "Found `` but expected `NAMESPACE, DATABASE, NS, DB`",
		},
		{
			sql: `USE NAMESPACE`,
			err: "Found `` but expected `IDENT, STRING, NUMBER, DOUBLE, DATE, TIME`",
		},
		{
			sql: `USE NAMESPACE ''`,
			err: "Found `` but expected `namespace name`",
		},
		{
			sql: `USE NAMESPACE name`,
			res: &Query{Statements: []Statement{&UseStatement{
				NS: "name",
			}}},
		},
		{
			sql: `USE NAMESPACE 1`,
			res: &Query{Statements: []Statement{&UseStatement{
				NS: "1",
			}}},
		},
		{
			sql: `USE NAMESPACE 1.3000`,
			res: &Query{Statements: []Statement{&UseStatement{
				NS: "1.3000",
			}}},
		},
		{
			sql: `USE NAMESPACE 123.123.123.123`,
			res: &Query{Statements: []Statement{&UseStatement{
				NS: "123.123.123.123",
			}}},
		},
		{
			sql: `USE NAMESPACE {"some":"thing"}`,
			err: "Found `{\"some\":\"thing\"}` but expected `IDENT, STRING, NUMBER, DOUBLE, DATE, TIME`",
		},
		{
			sql: `USE NAMESPACE name something`,
			err: "Found `something` but expected `;`",
		},
		{
			sql: `USE DATABASE`,
			err: "Found `` but expected `IDENT, STRING, NUMBER, DOUBLE, DATE, TIME`",
		},
		{
			sql: `USE DATABASE ''`,
			err: "Found `` but expected `database name`",
		},
		{
			sql: `USE DATABASE name`,
			res: &Query{Statements: []Statement{&UseStatement{
				DB: "name",
			}}},
		},
		{
			sql: `USE DATABASE 1`,
			res: &Query{Statements: []Statement{&UseStatement{
				DB: "1",
			}}},
		},
		{
			sql: `USE DATABASE 1.3000`,
			res: &Query{Statements: []Statement{&UseStatement{
				DB: "1.3000",
			}}},
		},
		{
			sql: `USE DATABASE 123.123.123.123`,
			res: &Query{Statements: []Statement{&UseStatement{
				DB: "123.123.123.123",
			}}},
		},
		{
			sql: `USE DATABASE {}`,
			err: "Found `{}` but expected `IDENT, STRING, NUMBER, DOUBLE, DATE, TIME`",
		},
		{
			sql: `USE DATABASE name something`,
			err: "Found `something` but expected `;`",
		},
		{
			sql: `BEGIN; USE NS name DB name; COMMIT;`,
			err: "You can't change NAMESPACE or DATABASE inside of a transaction",
		},
		{
			sql: "USE NS `*` DB `*`",
			str: "USE NAMESPACE `*` DATABASE `*`",
			res: &Query{Statements: []Statement{&UseStatement{
				NS: "*",
				DB: "*",
			}}},
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
	q, err := Parse(c, s)

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(q.Statements) != 1 {
		t.Fatalf("unexpected statement count: %d", len(q.Statements))
	}

}

// Ensure the parser can parse a multi-statement query.
func Test_Parse_General_Single(t *testing.T) {

	s := `SELECT a FROM b`
	q, err := Parse(c, s)

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(q.Statements) != 1 {
		t.Fatalf("unexpected statement count: %d", len(q.Statements))
	}

}

// Ensure the parser can parse a multi-statement query.
func Test_Parse_General_Multi(t *testing.T) {

	s := `SELECT a FROM b; SELECT c FROM d`
	q, err := Parse(c, s)

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
			err: "Found `!` but expected `USE, INFO, BEGIN, CANCEL, COMMIT, IF, LET, RETURN, LIVE, KILL, SELECT, CREATE, UPDATE, DELETE, RELATE, INSERT, UPSERT, DEFINE, REMOVE`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Info(t *testing.T) {

	var tests = []tester{
		{
			sql: `INFO`,
			err: "Found `` but expected `FOR`",
		},
		{
			sql: `INFO FOR`,
			err: "Found `` but expected `NAMESPACE, DATABASE, TABLE`",
		},
		{
			sql: `INFO FOR NAMESPACE`,
			res: &Query{Statements: []Statement{&InfoStatement{
				KV: "*", NS: "*", DB: "*",
				Kind: NAMESPACE,
			}}},
		},
		{
			sql: `INFO FOR DATABASE`,
			res: &Query{Statements: []Statement{&InfoStatement{
				KV: "*", NS: "*", DB: "*",
				Kind: DATABASE,
			}}},
		},
		{
			sql: `INFO FOR TABLE`,
			err: "Found `` but expected `table`",
		},
		{
			sql: `INFO FOR TABLE test`,
			res: &Query{Statements: []Statement{&InfoStatement{
				KV: "*", NS: "*", DB: "*",
				Kind: TABLE,
				What: &Table{"test"},
			}}},
		},
		{
			sql: `INFO FOR TABLE test something`,
			err: "Found `something` but expected `;`",
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
				RW: false, KV: "*", NS: "*", DB: "*",
				Name: &Ident{"name"},
				What: true,
			}}},
		},
		{
			sql: `LET name = false`,
			res: &Query{Statements: []Statement{&LetStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Name: &Ident{"name"},
				What: false,
			}}},
		},
		{
			sql: `LET name = "test"`,
			res: &Query{Statements: []Statement{&LetStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Name: &Ident{"name"},
				What: &Value{"test"},
			}}},
		},
		{
			sql: `LET name = 1`,
			res: &Query{Statements: []Statement{&LetStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Name: &Ident{"name"},
				What: float64(1),
			}}},
		},
		{
			sql: `LET name = 1.0`,
			str: `LET name = 1`,
			res: &Query{Statements: []Statement{&LetStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Name: &Ident{"name"},
				What: float64(1),
			}}},
		},
		{
			sql: `LET name = 1.1`,
			res: &Query{Statements: []Statement{&LetStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Name: &Ident{"name"},
				What: float64(1.1),
			}}},
		},
		{
			sql: `LET name = thing:test`,
			res: &Query{Statements: []Statement{&LetStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Name: &Ident{"name"},
				What: &Thing{TB: "thing", ID: "test"},
			}}},
		},
		{
			sql: `LET name = thing:test`,
			res: &Query{Statements: []Statement{&LetStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Name: &Ident{"name"},
				What: &Thing{TB: "thing", ID: "test"},
			}}},
		},
		{
			sql: `LET name = @thing:test`,
			str: `LET name = thing:test`,
			res: &Query{Statements: []Statement{&LetStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Name: &Ident{"name"},
				What: &Thing{TB: "thing", ID: "test"},
			}}},
		},
		{
			sql: `LET name = {"key": "val"}`,
			str: `LET name = {"key":"val"}`,
			res: &Query{Statements: []Statement{&LetStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Name: &Ident{"name"},
				What: map[string]interface{}{"key": "val"},
			}}},
		},
		{
			sql: `LET name = ["key", "val"]`,
			str: `LET name = ["key","val"]`,
			res: &Query{Statements: []Statement{&LetStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Name: &Ident{"name"},
				What: []interface{}{"key", "val"},
			}}},
		},
		{
			sql: `LET name = $test`,
			res: &Query{Statements: []Statement{&LetStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Name: &Ident{"name"},
				What: &Param{ID: "test"},
			}}},
		},
		{
			sql: `LET name = (CREATE person)`,
			res: &Query{Statements: []Statement{&LetStatement{
				RW: true, KV: "*", NS: "*", DB: "*",
				Name: &Ident{"name"},
				What: &SubExpression{Expr: &CreateStatement{
					KV: "*", NS: "*", DB: "*",
					What: Exprs{&Ident{ID: "person"}},
					Echo: AFTER,
				}},
			}}},
		},
		{
			sql: `LET name = "test" something`,
			err: "Found `something` but expected `;`",
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
				RW: false, KV: "*", NS: "*", DB: "*",
				What: Exprs{true},
			}}},
		},
		{
			sql: `RETURN true`,
			res: &Query{Statements: []Statement{&ReturnStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				What: Exprs{true},
			}}},
		},
		{
			sql: `RETURN false`,
			res: &Query{Statements: []Statement{&ReturnStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				What: Exprs{false},
			}}},
		},
		{
			sql: `RETURN "test"`,
			res: &Query{Statements: []Statement{&ReturnStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				What: Exprs{&Value{"test"}},
			}}},
		},
		{
			sql: `RETURN 1`,
			res: &Query{Statements: []Statement{&ReturnStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				What: Exprs{float64(1)},
			}}},
		},
		{
			sql: `RETURN 1.0`,
			str: `RETURN 1`,
			res: &Query{Statements: []Statement{&ReturnStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				What: Exprs{float64(1)},
			}}},
		},
		{
			sql: `RETURN 1.1`,
			res: &Query{Statements: []Statement{&ReturnStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				What: Exprs{float64(1.1)},
			}}},
		},
		{
			sql: `RETURN @thing:test`,
			str: `RETURN thing:test`,
			res: &Query{Statements: []Statement{&ReturnStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				What: Exprs{&Thing{TB: "thing", ID: "test"}},
			}}},
		},
		{
			sql: `RETURN {"key": "val"}`,
			str: `RETURN {"key":"val"}`,
			res: &Query{Statements: []Statement{&ReturnStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				What: Exprs{map[string]interface{}{"key": "val"}},
			}}},
		},
		{
			sql: `RETURN ["key", "val"]`,
			str: `RETURN ["key","val"]`,
			res: &Query{Statements: []Statement{&ReturnStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				What: Exprs{[]interface{}{"key", "val"}},
			}}},
		},
		{
			sql: `RETURN $test`,
			res: &Query{Statements: []Statement{&ReturnStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				What: Exprs{&Param{ID: "test"}},
			}}},
		},
		{
			sql: `RETURN (CREATE person)`,
			res: &Query{Statements: []Statement{&ReturnStatement{
				RW: true, KV: "*", NS: "*", DB: "*",
				What: Exprs{&SubExpression{Expr: &CreateStatement{
					KV: "*", NS: "*", DB: "*",
					What: Exprs{&Ident{ID: "person"}},
					Echo: AFTER,
				}}},
			}}},
		},
		{
			sql: `RETURN $test something`,
			err: "Found `something` but expected `;`",
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
			err: "Found `` but expected `expression`",
		},
		{
			sql: `SELECT * FROM per!son`,
			err: "Found `!` but expected `;`",
		},
		{
			sql: `SELECT * FROM person`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
			}}},
		},
		{
			sql: `SELECT * FROM @`,
			err: "Found `@` but expected `expression`",
		},
		{
			sql: `SELECT * FROM @person`,
			err: "Found `@person` but expected `expression`",
		},
		{
			sql: `SELECT * FROM @person:`,
			err: "Found `@person:` but expected `expression`",
		},
		{
			sql: `SELECT * FROM @person WHERE`,
			err: "Found `@person` but expected `expression`",
		},
		{
			sql: "SELECT * FROM 111",
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{float64(111)},
			}}},
		},
		{
			sql: "SELECT * FROM `111`",
			str: "SELECT * FROM 111",
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"111"}},
			}}},
		},
		{
			sql: "SELECT * FROM `2006-01-02`",
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"2006-01-02"}},
			}}},
		},
		{
			sql: "SELECT * FROM `2006-01-02T15:04:05+07:00`",
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"2006-01-02T15:04:05+07:00"}},
			}}},
		},
		{
			sql: "SELECT * FROM `2006-01-02T15:04:05.999999999+07:00`",
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"2006-01-02T15:04:05.999999999+07:00"}},
			}}},
		},
		{
			sql: `SELECT * FROM person`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
			}}},
		},
		{
			sql: `SELECT * FROM person, tweet`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}, &Ident{"tweet"}},
			}}},
		},
		{
			sql: `SELECT * FROM person:1a`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Thing{TB: "person", ID: "1a"}},
			}}},
		},
		{
			sql: `SELECT * FROM person:⟨1a⟩`,
			str: `SELECT * FROM person:1a`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Thing{TB: "person", ID: "1a"}},
			}}},
		},
		{
			sql: `SELECT * FROM person:123456`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Thing{TB: "person", ID: float64(123456)}},
			}}},
		},
		{
			sql: `SELECT * FROM person:⟨123456⟩`,
			str: `SELECT * FROM person:123456`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Thing{TB: "person", ID: float64(123456)}},
			}}},
		},
		{
			sql: `SELECT * FROM person:123.456`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Thing{TB: "person", ID: float64(123.456)}},
			}}},
		},
		{
			sql: `SELECT * FROM person:⟨123.456⟩`,
			str: `SELECT * FROM person:123.456`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Thing{TB: "person", ID: float64(123.456)}},
			}}},
		},
		{
			sql: `SELECT * FROM person:123.456.789.012`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Thing{TB: "person", ID: "123.456.789.012"}},
			}}},
		},
		{
			sql: `SELECT * FROM person:⟨123.456.789.012⟩`,
			str: `SELECT * FROM person:123.456.789.012`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Thing{TB: "person", ID: "123.456.789.012"}},
			}}},
		},
		{
			sql: `SELECT * FROM person:⟨1987-06-22⟩`,
			str: `SELECT * FROM person:⟨1987-06-22T00:00:00Z⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Thing{TB: "person", ID: date}},
			}}},
		},
		{
			sql: `SELECT * FROM person:⟨1987-06-22T08:30:30.511Z⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Thing{TB: "person", ID: nano}},
			}}},
		},
		{
			sql: `SELECT * FROM person:⟨A250C5A3-948F-4657-88AD-FF5F27B5B24E⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Thing{TB: "person", ID: "A250C5A3-948F-4657-88AD-FF5F27B5B24E"}},
			}}},
		},
		{
			sql: `SELECT * FROM person:⟨8250C5A3-948F-4657-88AD-FF5F27B5B24E⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Thing{TB: "person", ID: "8250C5A3-948F-4657-88AD-FF5F27B5B24E"}},
			}}},
		},
		{
			sql: `SELECT * FROM person:⟨Tobie Morgan Hitchcock⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Thing{TB: "person", ID: "Tobie Morgan Hitchcock"}},
			}}},
		},
		{
			sql: `SELECT * FROM ⟨email addresses⟩:⟨tobie@abcum.com⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Thing{TB: "email addresses", ID: "tobie@abcum.com"}},
			}}},
		},
		{
			sql: `SELECT * FROM ⟨email addresses⟩:⟨tobie+spam@abcum.com⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Thing{TB: "email addresses", ID: "tobie+spam@abcum.com"}},
			}}},
		},
		{
			sql: `SELECT *, temp AS test FROM person`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{
					{Expr: &All{}, Field: "*"},
					{Expr: &Ident{"temp"}, Field: "test", Alias: "test"},
				},
				What: []Expr{&Ident{"person"}},
			}}},
		},
		{
			sql: "SELECT `email addresses` AS emails FROM person",
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{
					{Expr: &Ident{"email addresses"}, Field: "emails", Alias: "emails"},
				},
				What: []Expr{&Ident{"person"}},
			}}},
		},
		{
			sql: "SELECT emails AS `email addresses` FROM person",
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{
					{Expr: &Ident{"emails"}, Field: "email addresses", Alias: "email addresses"},
				},
				What: []Expr{&Ident{"person"}},
			}}},
		},
		{
			sql: `SELECT * FROM (CREATE person)`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: true, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{
					{Expr: &All{}, Field: "*"},
				},
				What: Exprs{&SubExpression{Expr: &CreateStatement{
					KV: "*", NS: "*", DB: "*",
					What: Exprs{&Ident{ID: "person"}},
					Echo: AFTER,
				}}},
			}}},
		},
		{
			sql: "SELECT * FROM person WHERE id = \"\x0A\"",
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"id"}, Op: EQ, RHS: &Value{"\n"}},
			}}},
		},
		{
			sql: "SELECT * FROM person WHERE id = \"\x0D\"",
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"id"}, Op: EQ, RHS: &Value{"\r"}},
			}}},
		},
		{
			sql: "SELECT * FROM person WHERE id = \"\b\n\r\t\"",
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"id"}, Op: EQ, RHS: &Value{"\b\n\r\t"}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE`,
			err: "Found `` but expected `expression`",
		},
		{
			sql: `SELECT * FROM person WHERE id`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
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
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"id"}, Op: EQ, RHS: float64(1)},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE old = EMPTY`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"old"}, Op: EQ, RHS: &Empty{}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE old != EMPTY`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"old"}, Op: NEQ, RHS: &Empty{}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE old = MISSING`,
			str: `SELECT * FROM person WHERE old = VOID`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"old"}, Op: EQ, RHS: &Void{}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE old != MISSING`,
			str: `SELECT * FROM person WHERE old != VOID`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"old"}, Op: NEQ, RHS: &Void{}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE old IS EMPTY`,
			str: `SELECT * FROM person WHERE old = EMPTY`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"old"}, Op: EQ, RHS: &Empty{}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE old IS NOT EMPTY`,
			str: `SELECT * FROM person WHERE old != EMPTY`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"old"}, Op: NEQ, RHS: &Empty{}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE old IS MISSING`,
			str: `SELECT * FROM person WHERE old = VOID`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"old"}, Op: EQ, RHS: &Void{}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE old IS NOT MISSING`,
			str: `SELECT * FROM person WHERE old != VOID`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"old"}, Op: NEQ, RHS: &Void{}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE old = true`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"old"}, Op: EQ, RHS: true},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE old = false`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"old"}, Op: EQ, RHS: false},
			}}},
		},
		/*{
			sql: `SELECT * FROM person WHERE id != null AND id > 13.9 AND id < 31 AND id >= 15 AND id <= 29.9`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
				Cond: &BinaryExpression{
					LHS: &BinaryExpression{
						LHS: &BinaryExpression{
							LHS: &BinaryExpression{
								LHS: &BinaryExpression{
									LHS: &Ident{ID: "id"},
									Op:  NEQ,
									RHS: nil,
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
								RHS: float64(31),
							},
						},
						Op: AND,
						RHS: &BinaryExpression{
							LHS: &Ident{ID: "id"},
							Op:  GTE,
							RHS: float64(15),
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
		},*/
		{
			sql: `SELECT * FROM person WHERE test IN ["London","Paris"]`,
			str: `SELECT * FROM person WHERE test ∈ ["London","Paris"]`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"test"}, Op: INS, RHS: []interface{}{"London", "Paris"}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE test IS IN ["London","Paris"]`,
			str: `SELECT * FROM person WHERE test ∈ ["London","Paris"]`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"test"}, Op: INS, RHS: []interface{}{"London", "Paris"}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE test IS NOT IN ["London","Paris"]`,
			str: `SELECT * FROM person WHERE test ∉ ["London","Paris"]`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"test"}, Op: NIS, RHS: []interface{}{"London", "Paris"}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE ["London","Paris"] CONTAINS test`,
			str: `SELECT * FROM person WHERE ["London","Paris"] ∋ test`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
				Cond: &BinaryExpression{LHS: []interface{}{"London", "Paris"}, Op: SIN, RHS: &Ident{"test"}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE ["London","Paris"] CONTAINS NOT test`,
			str: `SELECT * FROM person WHERE ["London","Paris"] ∌ test`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
				Cond: &BinaryExpression{LHS: []interface{}{"London", "Paris"}, Op: SNI, RHS: &Ident{"test"}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE test = {`,
			err: "Found `{` but expected `expression`",
		},
		{
			sql: `SELECT * FROM person WHERE test = {"name":"London"}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"test"}, Op: EQ, RHS: map[string]interface{}{"name": "London"}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE test = {"name":{"f":"first","l":"last"}}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: []Expr{&Ident{"person"}},
				Cond: &BinaryExpression{LHS: &Ident{"test"}, Op: EQ, RHS: map[string]interface{}{"name": map[string]interface{}{"f": "first", "l": "last"}}},
			}}},
		},
		{
			sql: `SELECT * FROM person TIMEOUT 1s`,
			res: &Query{Statements: []Statement{&SelectStatement{
				RW: false, KV: "*", NS: "*", DB: "*",
				Expr:    []*Field{{Expr: &All{}, Field: "*"}},
				What:    []Expr{&Ident{"person"}},
				Timeout: 1 * time.Second,
			}}},
		},
		{
			sql: `SELECT * FROM person TIMEOUT null`,
			err: "Found `null` but expected `duration`",
		},
	}

	/*bday1, _ := time.Parse("2006-01-02", "1987-06-22")
	bday2, _ := time.Parse(time.RFC3339, "1987-06-22T08:00:00Z")
	bday3, _ := time.Parse(time.RFC3339, "1987-06-22T08:30:00.193943735Z")
	bday4, _ := time.Parse(time.RFC3339, "2016-03-14T11:19:31.193943735Z")

	tests = append(tests, tester{
		sql: `SELECT * FROM person WHERE bday >= "1987-06-22" AND bday >= "1987-06-22T08:00:00Z" AND bday >= "1987-06-22T08:30:00.193943735Z" AND bday <= "2016-03-14T11:19:31.193943735Z"`,
		res: &Query{Statements: []Statement{&SelectStatement{
			RW: true, KV: "*", NS: "*", DB: "*",
			Expr: []*Field{{Expr: &All{}, Field: "*"}},
			What: []Expr{&Ident{"person"}},
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
	})*/

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Create(t *testing.T) {

	var tests = []tester{
		{
			sql: `CREATE`,
			err: "Found `` but expected `expression`",
		},
		{
			sql: `CREATE person`,
			res: &Query{Statements: []Statement{&CreateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
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
				What: []Expr{&Ident{"person"}},
				Data: &DataExpression{Data: []*ItemExpression{{LHS: &Ident{"firstname"}, Op: EQ, RHS: &Void{}}}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `CREATE person SET firstname = EMPTY`,
			res: &Query{Statements: []Statement{&CreateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Data: &DataExpression{Data: []*ItemExpression{{LHS: &Ident{"firstname"}, Op: EQ, RHS: &Empty{}}}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `CREATE person SET firstname = "Tobie" something`,
			err: "Found `something` but expected `;`",
		},
		{
			sql: `CREATE person SET firstname = "Tobie"`,
			res: &Query{Statements: []Statement{&CreateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
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
			err: "Found `something` but expected `;`",
		},
		{
			sql: `CREATE person MERGE {"firstname":"Tobie"}`,
			res: &Query{Statements: []Statement{&CreateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Data: &MergeExpression{Data: map[string]interface{}{"firstname": "Tobie"}},
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
			err: "Found `something` but expected `;`",
		},
		{
			sql: `CREATE person CONTENT {"firstname":"Tobie"}`,
			res: &Query{Statements: []Statement{&CreateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Data: &ContentExpression{Data: map[string]interface{}{"firstname": "Tobie"}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `CREATE person RETURN`,
			err: "Found `` but expected `NONE, INFO, BOTH, DIFF, BEFORE, AFTER`",
		},
		{
			sql: `CREATE person RETURN NONE`,
			res: &Query{Statements: []Statement{&CreateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Echo: NONE,
			}}},
		},
		{
			sql: `CREATE person RETURN BOTH`,
			res: &Query{Statements: []Statement{&CreateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Echo: BOTH,
			}}},
		},
		{
			sql: `CREATE person RETURN DIFF`,
			res: &Query{Statements: []Statement{&CreateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Echo: DIFF,
			}}},
		},
		{
			sql: `CREATE person RETURN BEFORE`,
			res: &Query{Statements: []Statement{&CreateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Echo: BEFORE,
			}}},
		},
		{
			sql: `CREATE person RETURN AFTER`,
			str: `CREATE person`,
			res: &Query{Statements: []Statement{&CreateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `CREATE person TIMEOUT 1s`,
			res: &Query{Statements: []Statement{&CreateStatement{
				KV: "*", NS: "*", DB: "*",
				What:    []Expr{&Ident{"person"}},
				Echo:    AFTER,
				Timeout: 1 * time.Second,
			}}},
		},
		{
			sql: `CREATE person TIMEOUT null`,
			err: "Found `null` but expected `duration`",
		},
		{
			sql: `CREATE person something`,
			err: "Found `something` but expected `;`",
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
			err: "Found `` but expected `expression`",
		},
		{
			sql: `UPDATE person`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
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
				What: []Expr{&Ident{"person"}},
				Data: &DataExpression{Data: []*ItemExpression{{LHS: &Ident{"firstname"}, Op: EQ, RHS: &Void{}}}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `UPDATE person SET firstname = EMPTY`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Data: &DataExpression{Data: []*ItemExpression{{LHS: &Ident{"firstname"}, Op: EQ, RHS: &Empty{}}}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `UPDATE person SET firstname = "Tobie" something`,
			err: "Found `something` but expected `;`",
		},
		{
			sql: `UPDATE person SET firstname = "Tobie"`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
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
			err: "Found `something` but expected `;`",
		},
		{
			sql: `UPDATE person DIFF []`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Data: &DiffExpression{Data: []interface{}{}},
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
			err: "Found `something` but expected `;`",
		},
		{
			sql: `UPDATE person MERGE {"firstname":"Tobie"}`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Data: &MergeExpression{Data: map[string]interface{}{"firstname": "Tobie"}},
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
			err: "Found `something` but expected `;`",
		},
		{
			sql: `UPDATE person CONTENT {"firstname":"Tobie"}`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Data: &ContentExpression{Data: map[string]interface{}{"firstname": "Tobie"}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `UPDATE person RETURN`,
			err: "Found `` but expected `NONE, INFO, BOTH, DIFF, BEFORE, AFTER`",
		},
		{
			sql: `UPDATE person RETURN NONE`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Echo: NONE,
			}}},
		},
		{
			sql: `UPDATE person RETURN BOTH`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Echo: BOTH,
			}}},
		},
		{
			sql: `UPDATE person RETURN DIFF`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Echo: DIFF,
			}}},
		},
		{
			sql: `UPDATE person RETURN BEFORE`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Echo: BEFORE,
			}}},
		},
		{
			sql: `UPDATE person RETURN AFTER`,
			str: `UPDATE person`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `UPDATE person TIMEOUT 1s`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				KV: "*", NS: "*", DB: "*",
				What:    []Expr{&Ident{"person"}},
				Echo:    AFTER,
				Timeout: 1 * time.Second,
			}}},
		},
		{
			sql: `UPDATE person TIMEOUT null`,
			err: "Found `null` but expected `duration`",
		},
		{
			sql: `UPDATE person something`,
			err: "Found `something` but expected `;`",
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
			err: "Found `` but expected `expression`",
		},
		{
			sql: `DELETE FROM`,
			err: "Found `` but expected `expression`",
		},
		{
			sql: `DELETE person`,
			res: &Query{Statements: []Statement{&DeleteStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Echo: NONE,
			}}},
		},
		{
			sql: `DELETE AND EXPUNGE person`,
			res: &Query{Statements: []Statement{&DeleteStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Hard: true,
				Echo: NONE,
			}}},
		},
		{
			sql: `DELETE person RETURN`,
			err: "Found `` but expected `NONE, INFO, BOTH, DIFF, BEFORE, AFTER`",
		},
		{
			sql: `DELETE person RETURN NONE`,
			str: `DELETE person`,
			res: &Query{Statements: []Statement{&DeleteStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Echo: NONE,
			}}},
		},
		{
			sql: `DELETE person RETURN BOTH`,
			res: &Query{Statements: []Statement{&DeleteStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Echo: BOTH,
			}}},
		},
		{
			sql: `DELETE person RETURN DIFF`,
			res: &Query{Statements: []Statement{&DeleteStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Echo: DIFF,
			}}},
		},
		{
			sql: `DELETE person RETURN BEFORE`,
			res: &Query{Statements: []Statement{&DeleteStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Echo: BEFORE,
			}}},
		},
		{
			sql: `DELETE person RETURN AFTER`,
			res: &Query{Statements: []Statement{&DeleteStatement{
				KV: "*", NS: "*", DB: "*",
				What: []Expr{&Ident{"person"}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `DELETE person TIMEOUT 1s`,
			res: &Query{Statements: []Statement{&DeleteStatement{
				KV: "*", NS: "*", DB: "*",
				What:    []Expr{&Ident{"person"}},
				Echo:    NONE,
				Timeout: 1 * time.Second,
			}}},
		},
		{
			sql: `DELETE person TIMEOUT null`,
			err: "Found `null` but expected `duration`",
		},
		{
			sql: `DELETE person something`,
			err: "Found `something` but expected `;`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Relate(t *testing.T) {

	var tests = []tester{
		{
			sql: `RELATE`,
			err: "Found `` but expected `table`",
		},
		{
			sql: `RELATE purchase`,
			err: "Found `` but expected `FROM`",
		},
		{
			sql: `RELATE purchase FROM`,
			err: "Found `` but expected `expression`",
		},
		{
			sql: `RELATE purchase FROM person`,
			err: "Found `` but expected `TO, WITH`",
		},
		{
			sql: `RELATE purchase FROM person WITH`,
			err: "Found `` but expected `expression`",
		},
		{
			sql: `RELATE purchase FROM person WITH item`,
			res: &Query{Statements: []Statement{&RelateStatement{
				KV: "*", NS: "*", DB: "*",
				Type: &Table{"purchase"},
				From: []Expr{&Ident{"person"}},
				With: []Expr{&Ident{"item"}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `RELATE purchase FROM person WITH item UNIQUE`,
			res: &Query{Statements: []Statement{&RelateStatement{
				KV: "*", NS: "*", DB: "*",
				Type: &Table{"purchase"},
				From: []Expr{&Ident{"person"}},
				With: []Expr{&Ident{"item"}},
				Uniq: true,
				Echo: AFTER,
			}}},
		},
		{
			sql: `RELATE purchase FROM person WITH item SET 123`,
			err: "Found `123` but expected `field name`",
		},
		{
			sql: `RELATE purchase FROM person WITH item SET firstname`,
			err: "Found `` but expected `=, +=, -=`",
		},
		{
			sql: `RELATE purchase FROM person WITH item SET public = true`,
			res: &Query{Statements: []Statement{&RelateStatement{
				KV: "*", NS: "*", DB: "*",
				Type: &Table{"purchase"},
				From: []Expr{&Ident{"person"}},
				With: []Expr{&Ident{"item"}},
				Data: &DataExpression{Data: []*ItemExpression{{LHS: &Ident{"public"}, Op: EQ, RHS: true}}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `RELATE purchase FROM person WITH item RETURN`,
			err: "Found `` but expected `NONE, INFO, BOTH, DIFF, BEFORE, AFTER`",
		},
		{
			sql: `RELATE purchase FROM person WITH item RETURN NONE`,
			res: &Query{Statements: []Statement{&RelateStatement{
				KV: "*", NS: "*", DB: "*",
				Type: &Table{"purchase"},
				From: []Expr{&Ident{"person"}},
				With: []Expr{&Ident{"item"}},
				Echo: NONE,
			}}},
		},
		{
			sql: `RELATE purchase FROM person WITH item RETURN BOTH`,
			res: &Query{Statements: []Statement{&RelateStatement{
				KV: "*", NS: "*", DB: "*",
				Type: &Table{"purchase"},
				From: []Expr{&Ident{"person"}},
				With: []Expr{&Ident{"item"}},
				Echo: BOTH,
			}}},
		},
		{
			sql: `RELATE purchase FROM person WITH item RETURN DIFF`,
			res: &Query{Statements: []Statement{&RelateStatement{
				KV: "*", NS: "*", DB: "*",
				Type: &Table{"purchase"},
				From: []Expr{&Ident{"person"}},
				With: []Expr{&Ident{"item"}},
				Echo: DIFF,
			}}},
		},
		{
			sql: `RELATE purchase FROM person WITH item RETURN BEFORE`,
			res: &Query{Statements: []Statement{&RelateStatement{
				KV: "*", NS: "*", DB: "*",
				Type: &Table{"purchase"},
				From: []Expr{&Ident{"person"}},
				With: []Expr{&Ident{"item"}},
				Echo: BEFORE,
			}}},
		},
		{
			sql: `RELATE purchase FROM person WITH item RETURN AFTER`,
			str: `RELATE purchase FROM person WITH item`,
			res: &Query{Statements: []Statement{&RelateStatement{
				KV: "*", NS: "*", DB: "*",
				Type: &Table{"purchase"},
				From: []Expr{&Ident{"person"}},
				With: []Expr{&Ident{"item"}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `RELATE purchase FROM person WITH item TIMEOUT 1s`,
			res: &Query{Statements: []Statement{&RelateStatement{
				KV: "*", NS: "*", DB: "*",
				Type:    &Table{"purchase"},
				From:    []Expr{&Ident{"person"}},
				With:    []Expr{&Ident{"item"}},
				Echo:    AFTER,
				Timeout: 1 * time.Second,
			}}},
		},
		{
			sql: `RELATE purchase FROM person WITH item TIMEOUT null`,
			err: "Found `null` but expected `duration`",
		},
		{
			sql: `RELATE purchase FROM person WITH item something`,
			err: "Found `something` but expected `;`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Insert(t *testing.T) {

	var tests = []tester{
		{
			sql: `INSERT`,
			err: "Found `` but expected `expression`",
		},
		{
			sql: `INSERT ["one","two","tre"]`,
			err: "Found `` but expected `INTO`",
		},
		{
			sql: `INSERT ["one","two","tre"] INTO`,
			err: "Found `` but expected `table`",
		},
		{
			sql: `INSERT ["one","two","tre"] INTO person`,
			res: &Query{Statements: []Statement{&InsertStatement{
				KV: "*", NS: "*", DB: "*",
				Data: []interface{}{"one", "two", "tre"},
				Into: &Table{"person"},
				Echo: AFTER,
			}}},
		},
		{
			sql: `INSERT ["one","two","tre"] INTO person RETURN`,
			err: "Found `` but expected `NONE, INFO, BOTH, DIFF, BEFORE, AFTER`",
		},
		{
			sql: `INSERT ["one","two","tre"] INTO person RETURN NONE`,
			res: &Query{Statements: []Statement{&InsertStatement{
				KV: "*", NS: "*", DB: "*",
				Data: []interface{}{"one", "two", "tre"},
				Into: &Table{"person"},
				Echo: NONE,
			}}},
		},
		{
			sql: `INSERT ["one","two","tre"] INTO person RETURN INFO`,
			res: &Query{Statements: []Statement{&InsertStatement{
				KV: "*", NS: "*", DB: "*",
				Data: []interface{}{"one", "two", "tre"},
				Into: &Table{"person"},
				Echo: INFO,
			}}},
		},
		{
			sql: `INSERT ["one","two","tre"] INTO person RETURN BOTH`,
			res: &Query{Statements: []Statement{&InsertStatement{
				KV: "*", NS: "*", DB: "*",
				Data: []interface{}{"one", "two", "tre"},
				Into: &Table{"person"},
				Echo: BOTH,
			}}},
		},
		{
			sql: `INSERT ["one","two","tre"] INTO person RETURN DIFF`,
			res: &Query{Statements: []Statement{&InsertStatement{
				KV: "*", NS: "*", DB: "*",
				Data: []interface{}{"one", "two", "tre"},
				Into: &Table{"person"},
				Echo: DIFF,
			}}},
		},
		{
			sql: `INSERT ["one","two","tre"] INTO person RETURN BEFORE`,
			res: &Query{Statements: []Statement{&InsertStatement{
				KV: "*", NS: "*", DB: "*",
				Data: []interface{}{"one", "two", "tre"},
				Into: &Table{"person"},
				Echo: BEFORE,
			}}},
		},
		{
			sql: `INSERT ["one","two","tre"] INTO person RETURN AFTER`,
			str: `INSERT ["one","two","tre"] INTO person`,
			res: &Query{Statements: []Statement{&InsertStatement{
				KV: "*", NS: "*", DB: "*",
				Data: []interface{}{"one", "two", "tre"},
				Into: &Table{"person"},
				Echo: AFTER,
			}}},
		},
		{
			sql: `INSERT ["one","two","tre"] INTO person TIMEOUT 1s`,
			res: &Query{Statements: []Statement{&InsertStatement{
				KV: "*", NS: "*", DB: "*",
				Data:    []interface{}{"one", "two", "tre"},
				Into:    &Table{"person"},
				Echo:    AFTER,
				Timeout: 1 * time.Second,
			}}},
		},
		{
			sql: `INSERT ["one","two","tre"] INTO person TIMEOUT null`,
			err: "Found `null` but expected `duration`",
		},
		{
			sql: `INSERT ["one","two","tre"] INTO person something`,
			err: "Found `something` but expected `;`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Upsert(t *testing.T) {

	var tests = []tester{
		{
			sql: `UPSERT`,
			err: "Found `` but expected `expression`",
		},
		{
			sql: `UPSERT ["one","two","tre"]`,
			err: "Found `` but expected `INTO`",
		},
		{
			sql: `UPSERT ["one","two","tre"] INTO`,
			err: "Found `` but expected `table`",
		},
		{
			sql: `UPSERT ["one","two","tre"] INTO person`,
			res: &Query{Statements: []Statement{&UpsertStatement{
				KV: "*", NS: "*", DB: "*",
				Data: []interface{}{"one", "two", "tre"},
				Into: &Table{"person"},
				Echo: AFTER,
			}}},
		},
		{
			sql: `UPSERT ["one","two","tre"] INTO person RETURN`,
			err: "Found `` but expected `NONE, INFO, BOTH, DIFF, BEFORE, AFTER`",
		},
		{
			sql: `UPSERT ["one","two","tre"] INTO person RETURN NONE`,
			res: &Query{Statements: []Statement{&UpsertStatement{
				KV: "*", NS: "*", DB: "*",
				Data: []interface{}{"one", "two", "tre"},
				Into: &Table{"person"},
				Echo: NONE,
			}}},
		},
		{
			sql: `UPSERT ["one","two","tre"] INTO person RETURN INFO`,
			res: &Query{Statements: []Statement{&UpsertStatement{
				KV: "*", NS: "*", DB: "*",
				Data: []interface{}{"one", "two", "tre"},
				Into: &Table{"person"},
				Echo: INFO,
			}}},
		},
		{
			sql: `UPSERT ["one","two","tre"] INTO person RETURN BOTH`,
			res: &Query{Statements: []Statement{&UpsertStatement{
				KV: "*", NS: "*", DB: "*",
				Data: []interface{}{"one", "two", "tre"},
				Into: &Table{"person"},
				Echo: BOTH,
			}}},
		},
		{
			sql: `UPSERT ["one","two","tre"] INTO person RETURN DIFF`,
			res: &Query{Statements: []Statement{&UpsertStatement{
				KV: "*", NS: "*", DB: "*",
				Data: []interface{}{"one", "two", "tre"},
				Into: &Table{"person"},
				Echo: DIFF,
			}}},
		},
		{
			sql: `UPSERT ["one","two","tre"] INTO person RETURN BEFORE`,
			res: &Query{Statements: []Statement{&UpsertStatement{
				KV: "*", NS: "*", DB: "*",
				Data: []interface{}{"one", "two", "tre"},
				Into: &Table{"person"},
				Echo: BEFORE,
			}}},
		},
		{
			sql: `UPSERT ["one","two","tre"] INTO person RETURN AFTER`,
			str: `UPSERT ["one","two","tre"] INTO person`,
			res: &Query{Statements: []Statement{&UpsertStatement{
				KV: "*", NS: "*", DB: "*",
				Data: []interface{}{"one", "two", "tre"},
				Into: &Table{"person"},
				Echo: AFTER,
			}}},
		},
		{
			sql: `UPSERT ["one","two","tre"] INTO person TIMEOUT 1s`,
			res: &Query{Statements: []Statement{&UpsertStatement{
				KV: "*", NS: "*", DB: "*",
				Data:    []interface{}{"one", "two", "tre"},
				Into:    &Table{"person"},
				Echo:    AFTER,
				Timeout: 1 * time.Second,
			}}},
		},
		{
			sql: `UPSERT ["one","two","tre"] INTO person TIMEOUT null`,
			err: "Found `null` but expected `duration`",
		},
		{
			sql: `UPSERT ["one","two","tre"] INTO person something`,
			err: "Found `something` but expected `;`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Live(t *testing.T) {

	var tests = []tester{
		{
			sql: `LIVE`,
			err: "Found `` but expected `SELECT`",
		},
		{
			sql: `LIVE SELECT`,
			err: "Found `` but expected `expression`",
		},
		{
			sql: `LIVE SELECT *`,
			err: "Found `` but expected `FROM`",
		},
		{
			sql: `LIVE SELECT * FROM`,
			err: "Found `` but expected `table`",
		},
		{
			sql: `LIVE SELECT * FROM person`,
			res: &Query{Statements: []Statement{&LiveStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: &Table{"person"},
			}}},
		},
		{
			sql: `LIVE SELECT * FROM person WHERE`,
			err: "Found `` but expected `expression`",
		},
		{
			sql: `LIVE SELECT * FROM person WHERE public = true`,
			res: &Query{Statements: []Statement{&LiveStatement{
				KV: "*", NS: "*", DB: "*",
				Expr: []*Field{{Expr: &All{}, Field: "*"}},
				What: &Table{"person"},
				Cond: &BinaryExpression{
					LHS: &Ident{"public"},
					Op:  EQ,
					RHS: true,
				},
			}}},
		},
		{
			sql: `LIVE SELECT * FROM person WHERE public = true something`,
			err: "Found `something` but expected `;`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Kill(t *testing.T) {

	var tests = []tester{
		{
			sql: `KILL`,
			err: "Found `` but expected `string`",
		},
		{
			sql: `KILL null`,
			err: "Found `null` but expected `string`",
		},
		{
			sql: `KILL 1`,
			err: "Found `1` but expected `string`",
		},
		{
			sql: `KILL 1.3000`,
			err: "Found `1.3000` but expected `string`",
		},
		{
			sql: `KILL identifier`,
			err: "Found `identifier` but expected `string`",
		},
		{
			sql: `KILL "identifier"`,
			res: &Query{Statements: []Statement{&KillStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Value{"identifier"},
			}}},
		},
		{
			sql: `KILL "identifier" something`,
			err: "Found `something` but expected `;`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Define(t *testing.T) {

	var tests = []tester{
		{
			sql: `DEFINE`,
			err: "Found `` but expected `NAMESPACE, DATABASE, LOGIN, TOKEN, SCOPE, TABLE, EVENT, FIELD, INDEX`",
		},
		// ----------------------------------------------------------------------
		{
			sql: `DEFINE NAMESPACE`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `DEFINE NAMESPACE 111`,
			err: "Found `111` but expected `name`",
		},
		{
			sql: `DEFINE NAMESPACE 111.111`,
			err: "Found `111.111` but expected `name`",
		},
		{
			sql: `DEFINE NAMESPACE test`,
			res: &Query{Statements: []Statement{&DefineNamespaceStatement{
				KV: "*", NS: "*", DB: "*", Name: &Ident{"test"},
			}}},
		},
		{
			sql: `DEFINE NAMESPACE test something`,
			err: "Found `something` but expected `;`",
		},
		// ----------------------------------------------------------------------
		{
			sql: `DEFINE DATABASE`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `DEFINE DATABASE 111`,
			err: "Found `111` but expected `name`",
		},
		{
			sql: `DEFINE DATABASE 111.111`,
			err: "Found `111.111` but expected `name`",
		},
		{
			sql: `DEFINE DATABASE test`,
			res: &Query{Statements: []Statement{&DefineDatabaseStatement{
				KV: "*", NS: "*", DB: "*", Name: &Ident{"test"},
			}}},
		},
		{
			sql: `DEFINE DATABASE test something`,
			err: "Found `something` but expected `;`",
		},
		// ----------------------------------------------------------------------
		{
			sql: `DEFINE LOGIN`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `DEFINE LOGIN 111`,
			err: "Found `111` but expected `name`",
		},
		{
			sql: `DEFINE LOGIN 111.111`,
			err: "Found `111.111` but expected `name`",
		},
		{
			sql: `DEFINE LOGIN test`,
			err: "Found `` but expected `ON`",
		},
		{
			sql: `DEFINE LOGIN test ON`,
			err: "Found `` but expected `NAMESPACE, DATABASE`",
		},
		{
			sql: `DEFINE LOGIN test ON something`,
			err: "Found `something` but expected `NAMESPACE, DATABASE`",
		},
		{
			sql: `DEFINE LOGIN test ON NAMESPACE`,
			err: "Found `` but expected `PASSWORD`",
		},
		{
			sql: `DEFINE LOGIN test ON NAMESPACE PASSWORD`,
			err: "Found `` but expected `string`",
		},
		{
			sql: `DEFINE LOGIN test ON NAMESPACE PASSWORD "123456"`,
			str: `DEFINE LOGIN test ON NAMESPACE PASSWORD ********`,
			res: &Query{Statements: []Statement{&DefineLoginStatement{
				KV: "*", NS: "*", DB: "*",
				Kind: NAMESPACE,
				User: &Ident{"test"},
				Pass: []byte("123456"),
			}}},
		},
		{
			sql: `DEFINE LOGIN test ON DATABASE PASSWORD "123456"`,
			str: `DEFINE LOGIN test ON DATABASE PASSWORD ********`,
			res: &Query{Statements: []Statement{&DefineLoginStatement{
				KV: "*", NS: "*", DB: "*",
				Kind: DATABASE,
				User: &Ident{"test"},
				Pass: []byte("123456"),
			}}},
		},
		{
			sql: `DEFINE LOGIN test ON NAMESPACE PASSWORD "123456" something`,
			err: "Found `something` but expected `;`",
		},
		// ----------------------------------------------------------------------
		{
			sql: `DEFINE TOKEN`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `DEFINE TOKEN 111`,
			err: "Found `111` but expected `name`",
		},
		{
			sql: `DEFINE TOKEN 111.111`,
			err: "Found `111.111` but expected `name`",
		},
		{
			sql: `DEFINE TOKEN test`,
			err: "Found `` but expected `ON`",
		},
		{
			sql: `DEFINE TOKEN test ON`,
			err: "Found `` but expected `NAMESPACE, DATABASE, SCOPE`",
		},
		{
			sql: `DEFINE TOKEN test ON something`,
			err: "Found `something` but expected `NAMESPACE, DATABASE, SCOPE`",
		},
		{
			sql: `DEFINE TOKEN test ON NAMESPACE`,
			err: "Found `` but expected `TYPE`",
		},
		{
			sql: `DEFINE TOKEN test ON NAMESPACE TYPE 100`,
			err: "Found `100` but expected `ES256, ES384, ES512, HS256, HS384, HS512, PS256, PS384, PS512, RS256, RS384, RS512`",
		},
		{
			sql: `DEFINE TOKEN test ON NAMESPACE TYPE XX512`,
			err: "Found `XX512` but expected `ES256, ES384, ES512, HS256, HS384, HS512, PS256, PS384, PS512, RS256, RS384, RS512`",
		},
		{
			sql: `DEFINE TOKEN test ON NAMESPACE TYPE HS512`,
			err: "Found `` but expected `VALUE`",
		},
		{
			sql: `DEFINE TOKEN test ON NAMESPACE TYPE HS512`,
			err: "Found `` but expected `VALUE`",
		},
		{
			sql: `DEFINE TOKEN test ON NAMESPACE TYPE HS512 VALUE`,
			err: "Found `` but expected `string`",
		},
		{
			sql: `DEFINE TOKEN test ON NAMESPACE TYPE HS512 VALUE "secret"`,
			str: `DEFINE TOKEN test ON NAMESPACE TYPE HS512 VALUE ********`,
			res: &Query{Statements: []Statement{&DefineTokenStatement{
				KV: "*", NS: "*", DB: "*",
				Kind: NAMESPACE,
				Name: &Ident{"test"},
				Type: "HS512",
				Code: []byte("secret"),
			}}},
		},
		{
			sql: `DEFINE TOKEN test ON DATABASE TYPE HS512 VALUE "secret"`,
			str: `DEFINE TOKEN test ON DATABASE TYPE HS512 VALUE ********`,
			res: &Query{Statements: []Statement{&DefineTokenStatement{
				KV: "*", NS: "*", DB: "*",
				Kind: DATABASE,
				Name: &Ident{"test"},
				Type: "HS512",
				Code: []byte("secret"),
			}}},
		},
		{
			sql: `DEFINE TOKEN test ON SCOPE TYPE HS512 VALUE "secret"`,
			str: `DEFINE TOKEN test ON SCOPE TYPE HS512 VALUE ********`,
			res: &Query{Statements: []Statement{&DefineTokenStatement{
				KV: "*", NS: "*", DB: "*",
				Kind: SCOPE,
				Name: &Ident{"test"},
				Type: "HS512",
				Code: []byte("secret"),
			}}},
		},
		{
			sql: `DEFINE TOKEN test ON SCOPE TYPE HS512 VALUE "secret" something`,
			err: "Found `something` but expected `;`",
		},
		// ----------------------------------------------------------------------
		{
			sql: `DEFINE SCOPE`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `DEFINE SCOPE 111`,
			err: "Found `111` but expected `name`",
		},
		{
			sql: `DEFINE SCOPE 111.111`,
			err: "Found `111.111` but expected `name`",
		},
		{
			sql: `DEFINE SCOPE test`,
			res: &Query{Statements: []Statement{&DefineScopeStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"test"},
			}}},
		},
		{
			sql: `DEFINE SCOPE test SESSION null`,
			err: "Found `null` but expected `duration`",
		},
		{
			sql: `DEFINE SCOPE test SESSION 1h`,
			str: `DEFINE SCOPE test SESSION 1h0m0s`,
			res: &Query{Statements: []Statement{&DefineScopeStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"test"},
				Time: 1 * time.Hour,
			}}},
		},
		{
			sql: `DEFINE SCOPE test SESSION 1d`,
			str: `DEFINE SCOPE test SESSION 24h0m0s`,
			res: &Query{Statements: []Statement{&DefineScopeStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"test"},
				Time: 24 * time.Hour,
			}}},
		},
		{
			sql: `DEFINE SCOPE test SESSION 1w`,
			str: `DEFINE SCOPE test SESSION 168h0m0s`,
			res: &Query{Statements: []Statement{&DefineScopeStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"test"},
				Time: 168 * time.Hour,
			}}},
		},
		{
			sql: `DEFINE SCOPE test SIGNUP AS NONE`,
			err: "Found `NONE` but expected `expression`",
		},
		{
			sql: `DEFINE SCOPE test SIGNUP AS (CREATE person)`,
			res: &Query{Statements: []Statement{&DefineScopeStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"test"},
				Signup: &SubExpression{
					Expr: &CreateStatement{
						KV: "*", NS: "*", DB: "*",
						What: Exprs{&Ident{ID: "person"}},
						Echo: AFTER,
					},
				},
			}}},
		},
		{
			sql: `DEFINE SCOPE test SIGNIN AS NONE`,
			err: "Found `NONE` but expected `expression`",
		},
		{
			sql: `DEFINE SCOPE test SIGNIN AS (SELECT * FROM person)`,
			res: &Query{Statements: []Statement{&DefineScopeStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"test"},
				Signin: &SubExpression{
					Expr: &SelectStatement{
						KV: "*", NS: "*", DB: "*",
						Expr: []*Field{{Expr: &All{}, Field: "*"}},
						What: []Expr{&Ident{"person"}},
					},
				},
			}}},
		},
		{
			sql: `DEFINE SCOPE test CONNECT AS NONE`,
			err: "Found `NONE` but expected `expression`",
		},
		{
			sql: `DEFINE SCOPE test CONNECT AS (SELECT * FROM $id)`,
			res: &Query{Statements: []Statement{&DefineScopeStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"test"},
				Connect: &SubExpression{
					Expr: &SelectStatement{
						KV: "*", NS: "*", DB: "*",
						Expr: []*Field{{Expr: &All{}, Field: "*"}},
						What: []Expr{&Param{"id"}},
					},
				},
			}}},
		},
		{
			sql: `DEFINE SCOPE test something`,
			err: "Found `something` but expected `;`",
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
			err: "Found `something` but expected `;`",
		},
		{
			sql: `DEFINE TABLE person DROP`,
			str: `DEFINE TABLE person DROP`,
			res: &Query{Statements: []Statement{&DefineTableStatement{
				KV: "*", NS: "*", DB: "*",
				What: Tables{&Table{"person"}},
				Drop: true,
			}}},
		},
		{
			sql: `DEFINE TABLE person SCHEMALESS`,
			str: `DEFINE TABLE person`,
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
			sql: `DEFINE TABLE person PERMISSIONS SOME`,
			err: "Found `SOME` but expected `FOR, NONE, FULL, WHERE`",
		},
		{
			sql: `DEFINE TABLE person PERMISSIONS NONE`,
			res: &Query{Statements: []Statement{&DefineTableStatement{
				KV: "*", NS: "*", DB: "*",
				What: Tables{&Table{"person"}},
				Perms: &PermExpression{
					Select: false,
					Create: false,
					Update: false,
					Delete: false,
				},
			}}},
		},
		{
			sql: `DEFINE TABLE person PERMISSIONS FULL`,
			res: &Query{Statements: []Statement{&DefineTableStatement{
				KV: "*", NS: "*", DB: "*",
				What: Tables{&Table{"person"}},
				Perms: &PermExpression{
					Select: true,
					Create: true,
					Update: true,
					Delete: true,
				},
			}}},
		},
		{
			sql: `DEFINE TABLE person PERMISSIONS WHERE public = true`,
			res: &Query{Statements: []Statement{&DefineTableStatement{
				KV: "*", NS: "*", DB: "*",
				What: Tables{&Table{"person"}},
				Perms: &PermExpression{
					Select: &BinaryExpression{LHS: &Ident{"public"}, Op: EQ, RHS: true},
					Create: &BinaryExpression{LHS: &Ident{"public"}, Op: EQ, RHS: true},
					Update: &BinaryExpression{LHS: &Ident{"public"}, Op: EQ, RHS: true},
					Delete: &BinaryExpression{LHS: &Ident{"public"}, Op: EQ, RHS: true},
				},
			}}},
		},
		{
			sql: `DEFINE TABLE person PERMISSIONS FOR select FULL FOR insert, upsert NONE`,
			err: "Found `insert` but expected `SELECT, CREATE, UPDATE, DELETE`",
		},
		{
			sql: `DEFINE TABLE person PERMISSIONS FOR select FULL FOR create, update, delete SOME`,
			err: "Found `SOME` but expected `FULL, NONE, WHERE`",
		},
		{
			sql: `DEFINE TABLE person PERMISSIONS FOR select FULL FOR create, update, delete NONE`,
			res: &Query{Statements: []Statement{&DefineTableStatement{
				KV: "*", NS: "*", DB: "*",
				What: Tables{&Table{"person"}},
				Perms: &PermExpression{
					Select: true,
					Create: false,
					Update: false,
					Delete: false,
				},
			}}},
		},
		{
			sql: `DEFINE TABLE person PERMISSIONS FOR select, create, update WHERE public = true FOR delete NONE`,
			res: &Query{Statements: []Statement{&DefineTableStatement{
				KV: "*", NS: "*", DB: "*",
				What: Tables{&Table{"person"}},
				Perms: &PermExpression{
					Select: &BinaryExpression{LHS: &Ident{"public"}, Op: EQ, RHS: true},
					Create: &BinaryExpression{LHS: &Ident{"public"}, Op: EQ, RHS: true},
					Update: &BinaryExpression{LHS: &Ident{"public"}, Op: EQ, RHS: true},
					Delete: false,
				},
			}}},
		},
		{
			sql: `DEFINE TABLE person AS SELECT nationality, midhinge(age) AS mid FROM users GROUP BY nationality`,
			err: "Found 'mid' but field is not an aggregate function, and is not present in GROUP expression",
		},
		{
			sql: `DEFINE TABLE person AS SELECT nationality, count(*) AS total FROM users WHERE public = true GROUP BY nationality`,
			res: &Query{Statements: []Statement{&DefineTableStatement{
				KV: "*", NS: "*", DB: "*",
				What: Tables{&Table{"person"}},
				Lock: true,
				Expr: Fields{
					&Field{
						Expr:  &Ident{ID: "nationality"},
						Field: "nationality",
					},
					&Field{
						Expr: &FuncExpression{
							Name: "count",
							Args: Exprs{&All{}},
							Aggr: true,
						},
						Field: "total",
						Alias: "total",
					},
				},
				From: Tables{
					&Table{TB: "users"},
				},
				Cond: &BinaryExpression{
					LHS: &Ident{ID: "public"},
					Op:  EQ,
					RHS: true,
				},
				Group: Groups{
					&Group{
						Expr: &Ident{ID: "nationality"},
					},
				},
			}}},
		},
		{
			sql: `DEFINE TABLE person SCHEMALESS something`,
			err: "Found `something` but expected `;`",
		},
		{
			sql: `DEFINE TABLE person SCHEMAFULL something`,
			err: "Found `something` but expected `;`",
		},
		// ----------------------------------------------------------------------
		{
			sql: `DEFINE EVENT`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `DEFINE EVENT temp`,
			err: "Found `` but expected `ON`",
		},
		{
			sql: `DEFINE EVENT temp ON`,
			err: "Found `` but expected `table`",
		},
		{
			sql: `DEFINE EVENT temp ON person`,
			err: "Found `` but expected `WHEN`",
		},
		{
			sql: `DEFINE EVENT temp ON person WHEN`,
			err: "Found `` but expected `expression`",
		},
		{
			sql: `DEFINE EVENT temp ON person WHEN $before.price < $after.price`,
			err: "Found `` but expected `THEN`",
		},
		{
			sql: `DEFINE EVENT temp ON person WHEN $before.price < $after.price THEN`,
			err: "Found `` but expected `expression`",
		},
		{
			sql: `DEFINE EVENT temp ON person WHEN $before.price < $after.price THEN (UPDATE $this SET increased = true)`,
			res: &Query{Statements: []Statement{&DefineEventStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				When: &BinaryExpression{
					LHS: &Param{"before.price"},
					Op:  LT,
					RHS: &Param{"after.price"},
				},
				Then: &SubExpression{
					Expr: &UpdateStatement{
						KV: "*", NS: "*", DB: "*",
						What: Exprs{&Param{"this"}},
						Data: &DataExpression{[]*ItemExpression{
							{
								LHS: &Ident{"increased"},
								Op:  EQ,
								RHS: true,
							},
						}},
						Echo: AFTER,
					},
				},
			}}},
		},
		{
			sql: `DEFINE EVENT temp ON person WHEN $before.price < $after.price THEN (UPDATE $this SET increased = true) something`,
			err: "Found `something` but expected `;`",
		},
		// ----------------------------------------------------------------------
		{
			sql: `DEFINE FIELD`,
			err: "Found `` but expected `name, or expression`",
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
			err: "Found `` but expected `array, boolean, circle, color, datetime, domain, double, email, latitude, longitude, number, object, password, phone, point, polygon, record, string, url, uuid`",
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE something`,
			err: "Found `something` but expected `array, boolean, circle, color, datetime, domain, double, email, latitude, longitude, number, object, password, phone, point, polygon, record, string, url, uuid`",
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
			sql: `DEFINE FIELD temp ON person TYPE record`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Type: "record",
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE record (item)`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Type: "record",
				Kind: "item",
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person VALUE`,
			err: "Found `` but expected `expression`",
		},
		{
			sql: `DEFINE FIELD temp ON person VALUE string.uppercase($value)`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name:  &Ident{"temp"},
				What:  Tables{&Table{"person"}},
				Value: &FuncExpression{Name: "string.uppercase", Args: Exprs{&Param{"value"}}},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person ASSERT`,
			err: "Found `` but expected `expression`",
		},
		{
			sql: `DEFINE FIELD temp ON person ASSERT $value > 0 AND $value < 100`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Assert: &BinaryExpression{
					LHS: &Param{"value"},
					Op:  GT,
					RHS: &BinaryExpression{
						LHS: 0.0,
						Op:  AND,
						RHS: &BinaryExpression{
							LHS: &Param{"value"},
							Op:  LT,
							RHS: 100.0,
						},
					},
				},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person PERMISSIONS SOME`,
			err: "Found `SOME` but expected `FOR, NONE, FULL, WHERE`",
		},
		{
			sql: `DEFINE FIELD temp ON person PERMISSIONS NONE`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Perms: &PermExpression{
					Select: false,
					Create: false,
					Update: false,
					Delete: false,
				},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person PERMISSIONS FULL`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Perms: &PermExpression{
					Select: true,
					Create: true,
					Update: true,
					Delete: true,
				},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person PERMISSIONS WHERE public = true`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Perms: &PermExpression{
					Select: &BinaryExpression{LHS: &Ident{"public"}, Op: EQ, RHS: true},
					Create: &BinaryExpression{LHS: &Ident{"public"}, Op: EQ, RHS: true},
					Update: &BinaryExpression{LHS: &Ident{"public"}, Op: EQ, RHS: true},
					Delete: &BinaryExpression{LHS: &Ident{"public"}, Op: EQ, RHS: true},
				},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person PERMISSIONS FOR select FULL FOR insert, upsert NONE`,
			err: "Found `insert` but expected `SELECT, CREATE, UPDATE, DELETE`",
		},
		{
			sql: `DEFINE FIELD temp ON person PERMISSIONS FOR select FULL FOR create, update, delete SOME`,
			err: "Found `SOME` but expected `FULL, NONE, WHERE`",
		},
		{
			sql: `DEFINE FIELD temp ON person PERMISSIONS FOR select FULL FOR create, update, delete NONE`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Perms: &PermExpression{
					Select: true,
					Create: false,
					Update: false,
					Delete: false,
				},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person PERMISSIONS FOR select, create, update WHERE public = true FOR delete NONE`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
				Perms: &PermExpression{
					Select: &BinaryExpression{LHS: &Ident{"public"}, Op: EQ, RHS: true},
					Create: &BinaryExpression{LHS: &Ident{"public"}, Op: EQ, RHS: true},
					Update: &BinaryExpression{LHS: &Ident{"public"}, Op: EQ, RHS: true},
					Delete: false,
				},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person something`,
			err: "Found `something` but expected `;`",
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
			err: "Found `something` but expected `;`",
		},
		{
			sql: `DEFINE INDEX temp ON person COLUMNS firstname, lastname UNIQUE something`,
			err: "Found `something` but expected `;`",
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
			err: "Found `` but expected `NAMESPACE, DATABASE, LOGIN, TOKEN, SCOPE, TABLE, EVENT, FIELD, INDEX`",
		},
		// ----------------------------------------------------------------------
		{
			sql: `REMOVE NAMESPACE`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `REMOVE NAMESPACE 111`,
			err: "Found `111` but expected `name`",
		},
		{
			sql: `REMOVE NAMESPACE 111.111`,
			err: "Found `111.111` but expected `name`",
		},
		{
			sql: `REMOVE NAMESPACE test`,
			res: &Query{Statements: []Statement{&RemoveNamespaceStatement{
				KV: "*", NS: "*", DB: "*", Name: &Ident{"test"},
			}}},
		},
		{
			sql: `REMOVE NAMESPACE test something`,
			err: "Found `something` but expected `;`",
		},
		// ----------------------------------------------------------------------
		{
			sql: `REMOVE DATABASE`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `REMOVE DATABASE 111`,
			err: "Found `111` but expected `name`",
		},
		{
			sql: `REMOVE DATABASE 111.111`,
			err: "Found `111.111` but expected `name`",
		},
		{
			sql: `REMOVE DATABASE test`,
			res: &Query{Statements: []Statement{&RemoveDatabaseStatement{
				KV: "*", NS: "*", DB: "*", Name: &Ident{"test"},
			}}},
		},
		{
			sql: `REMOVE DATABASE test something`,
			err: "Found `something` but expected `;`",
		},
		// ----------------------------------------------------------------------
		{
			sql: `REMOVE LOGIN`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `REMOVE LOGIN 111`,
			err: "Found `111` but expected `name`",
		},
		{
			sql: `REMOVE LOGIN 111.111`,
			err: "Found `111.111` but expected `name`",
		},
		{
			sql: `REMOVE LOGIN test`,
			err: "Found `` but expected `ON`",
		},
		{
			sql: `REMOVE LOGIN test ON`,
			err: "Found `` but expected `NAMESPACE, DATABASE`",
		},
		{
			sql: `REMOVE LOGIN test ON something`,
			err: "Found `something` but expected `NAMESPACE, DATABASE`",
		},
		{
			sql: `REMOVE LOGIN test ON NAMESPACE`,
			res: &Query{Statements: []Statement{&RemoveLoginStatement{
				KV: "*", NS: "*", DB: "*",
				Kind: NAMESPACE,
				User: &Ident{"test"},
			}}},
		},
		{
			sql: `REMOVE LOGIN test ON DATABASE`,
			res: &Query{Statements: []Statement{&RemoveLoginStatement{
				KV: "*", NS: "*", DB: "*",
				Kind: DATABASE,
				User: &Ident{"test"},
			}}},
		},
		{
			sql: `REMOVE LOGIN test ON DATABASE something`,
			err: "Found `something` but expected `;`",
		},
		// ----------------------------------------------------------------------
		{
			sql: `REMOVE TOKEN`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `REMOVE TOKEN 111`,
			err: "Found `111` but expected `name`",
		},
		{
			sql: `REMOVE TOKEN 111.111`,
			err: "Found `111.111` but expected `name`",
		},
		{
			sql: `REMOVE TOKEN test`,
			err: "Found `` but expected `ON`",
		},
		{
			sql: `REMOVE TOKEN test ON`,
			err: "Found `` but expected `NAMESPACE, DATABASE, SCOPE`",
		},
		{
			sql: `REMOVE TOKEN test ON something`,
			err: "Found `something` but expected `NAMESPACE, DATABASE, SCOPE`",
		},
		{
			sql: `REMOVE TOKEN test ON NAMESPACE`,
			res: &Query{Statements: []Statement{&RemoveTokenStatement{
				KV: "*", NS: "*", DB: "*",
				Kind: NAMESPACE,
				Name: &Ident{"test"},
			}}},
		},
		{
			sql: `REMOVE TOKEN test ON DATABASE`,
			res: &Query{Statements: []Statement{&RemoveTokenStatement{
				KV: "*", NS: "*", DB: "*",
				Kind: DATABASE,
				Name: &Ident{"test"},
			}}},
		},
		{
			sql: `REMOVE TOKEN test ON SCOPE`,
			res: &Query{Statements: []Statement{&RemoveTokenStatement{
				KV: "*", NS: "*", DB: "*",
				Kind: SCOPE,
				Name: &Ident{"test"},
			}}},
		},
		{
			sql: `REMOVE TOKEN test ON DATABASE something`,
			err: "Found `something` but expected `;`",
		},
		// ----------------------------------------------------------------------
		{
			sql: `REMOVE SCOPE`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `REMOVE SCOPE 111`,
			err: "Found `111` but expected `name`",
		},
		{
			sql: `REMOVE SCOPE 111.111`,
			err: "Found `111.111` but expected `name`",
		},
		{
			sql: `REMOVE SCOPE test`,
			res: &Query{Statements: []Statement{&RemoveScopeStatement{
				KV: "*", NS: "*", DB: "*", Name: &Ident{"test"},
			}}},
		},
		{
			sql: `REMOVE SCOPE test something`,
			err: "Found `something` but expected `;`",
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
			err: "Found `something` but expected `;`",
		},
		// ----------------------------------------------------------------------
		{
			sql: `REMOVE EVENT`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `REMOVE EVENT temp`,
			err: "Found `` but expected `ON`",
		},
		{
			sql: `REMOVE EVENT temp ON`,
			err: "Found `` but expected `table`",
		},
		{
			sql: `REMOVE EVENT temp ON person`,
			res: &Query{Statements: []Statement{&RemoveEventStatement{
				KV: "*", NS: "*", DB: "*",
				Name: &Ident{"temp"},
				What: Tables{&Table{"person"}},
			}}},
		},
		{
			sql: `REMOVE EVENT temp ON person something`,
			err: "Found `something` but expected `;`",
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
			err: "Found `something` but expected `;`",
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
			err: "Found `something` but expected `;`",
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
			str: `BEGIN TRANSACTION`,
			res: &Query{Statements: []Statement{&BeginStatement{}}},
		},
		{
			sql: `BEGIN something`,
			err: "Found `something` but expected `;`",
		},
		{
			sql: `BEGIN TRANSACTION`,
			res: &Query{Statements: []Statement{&BeginStatement{}}},
		},
		{
			sql: `BEGIN TRANSACTION something`,
			err: "Found `something` but expected `;`",
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
			str: `CANCEL TRANSACTION`,
			res: &Query{Statements: []Statement{&CancelStatement{}}},
		},
		{
			sql: `CANCEL something`,
			err: "Found `something` but expected `;`",
		},
		{
			sql: `CANCEL TRANSACTION`,
			res: &Query{Statements: []Statement{&CancelStatement{}}},
		},
		{
			sql: `CANCEL TRANSACTION something`,
			err: "Found `something` but expected `;`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Commit(t *testing.T) {

	var tests = []tester{
		{
			sql: `COMMIT`,
			str: `COMMIT TRANSACTION`,
			res: &Query{Statements: []Statement{&CommitStatement{}}},
		},
		{
			sql: `COMMIT something`,
			err: "Found `something` but expected `;`",
		},
		{
			sql: `COMMIT TRANSACTION`,
			res: &Query{Statements: []Statement{&CommitStatement{}}},
		},
		{
			sql: `COMMIT TRANSACTION something`,
			err: "Found `something` but expected `;`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}
