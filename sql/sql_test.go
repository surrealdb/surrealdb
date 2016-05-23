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

	s, e := ParseString(c, test.sql)

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

	c = fibre.NewContext(nil, nil, nil)
	c.Set("KV", "")
	c.Set("NS", "")
	c.Set("DB", "")

}

// Ensure the parser can parse a multi-statement query.
func Test_Parse_General(t *testing.T) {

	s := `SELECT a FROM b`
	q, err := ParseString(c, s)

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(q.Statements) != 1 {
		t.Fatalf("unexpected statement count: %d", len(q.Statements))
	}

}

// Ensure the parser can parse a multi-statement query.
func Test_Parse_General_Single(t *testing.T) {

	s := `SELECT a FROM b`
	q, err := ParseString(c, s)

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(q.Statements) != 1 {
		t.Fatalf("unexpected statement count: %d", len(q.Statements))
	}

}

// Ensure the parser can parse a multi-statement query.
func Test_Parse_General_Multi(t *testing.T) {

	s := `SELECT a FROM b; SELECT c FROM d`
	q, err := ParseString(c, s)

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
			err: "Found ` FROM person` but expected `field name`",
		},
		{
			sql: `SELECT ' FROM person`,
			err: "Found ` FROM person` but expected `field name`",
		},
		{
			sql: `SELECT " FROM person`,
			err: "Found ` FROM person` but expected `field name`",
		},
		{
			sql: `SELECT "\" FROM person`,
			err: "Found `\" FROM person` but expected `field name`",
		},
		{
			sql: `!`,
			err: "Found `!` but expected `USE, SELECT, CREATE, UPDATE, INSERT, UPSERT, MODIFY, DELETE, RELATE, RECORD, DEFINE, RESYNC, REMOVE`",
		},
		{
			sql: `SELECT * FROM person;;;`,
			err: "Found `;` but expected `USE, SELECT, CREATE, UPDATE, INSERT, UPSERT, MODIFY, DELETE, RELATE, RECORD, DEFINE, RESYNC, REMOVE`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Use(t *testing.T) {

	var tests = []tester{
		{
			sql: `USE`,
			err: "Found `` but expected `NAMESPACE, DATABASE, CIPHERKEY`",
		},
		{
			sql: `USE NAMESPACE`,
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
			err: "Found `1` but expected `namespace name`",
		},
		{
			sql: `USE NAMESPACE 1.3000`,
			err: "Found `1.3000` but expected `namespace name`",
		},
		{
			sql: `USE NAMESPACE 123.123.123.123`,
			res: &Query{Statements: []Statement{&UseStatement{
				NS: "123.123.123.123",
			}}},
		},
		{
			sql: `USE NAMESPACE {"some":"thing"}`,
			err: "Found `{\"some\":\"thing\"}` but expected `namespace name`",
		},
		{
			sql: `USE NAMESPACE name something`,
			err: "Found `something` but expected `EOF, ;`",
		},
		{
			sql: `USE NAMESPACE ''`,
			res: &Query{Statements: []Statement{&UseStatement{
				NS: "",
			}}},
		},
		{
			sql: `USE DATABASE`,
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
			sql: `USE DATABASE {"some":"thing"}`,
			err: "Found `{\"some\":\"thing\"}` but expected `database name`",
		},
		{
			sql: `USE DATABASE name something`,
			err: "Found `something` but expected `EOF, ;`",
		},
		{
			sql: `USE DATABASE ''`,
			res: &Query{Statements: []Statement{&UseStatement{
				DB: "",
			}}},
		},
		{
			sql: `USE CIPHERKEY`,
			err: "Found `` but expected `32 bit cipher key`",
		},
		{
			sql: `USE CIPHERKEY 1hg7dbrma8ghe5473kghvie64jgi3ph4`,
			res: &Query{Statements: []Statement{&UseStatement{
				CK: "1hg7dbrma8ghe5473kghvie64jgi3ph4",
			}}},
		},
		{
			sql: `USE CIPHERKEY "1hg7dbrma8ghe5473kghvie64jgi3ph4"`,
			res: &Query{Statements: []Statement{&UseStatement{
				CK: "1hg7dbrma8ghe5473kghvie64jgi3ph4",
			}}},
		},
		{
			sql: `USE CIPHERKEY 1`,
			err: "Found `1` but expected `32 bit cipher key`",
		},
		{
			sql: `USE CIPHERKEY 1.3000`,
			err: "Found `1.3000` but expected `32 bit cipher key`",
		},
		{
			sql: `USE CIPHERKEY 123.123.123.123`,
			err: "Found `123.123.123.123` but expected `32 bit cipher key`",
		},
		{
			sql: `USE CIPHERKEY {"some":"thing"}`,
			err: "Found `{\"some\":\"thing\"}` but expected `32 bit cipher key`",
		},
		{
			sql: `USE CIPHERKEY name something`,
			err: "Found `name` but expected `32 bit cipher key`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Explain(t *testing.T) {

	var tests = []tester{
		{
			sql: `EXPLAIN SELECT ALL FROM person`,
			res: &Query{Statements: []Statement{&SelectStatement{
				EX:   true,
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Table{Name: "person"}},
			}}},
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Select(t *testing.T) {

	var tests = []tester{
		{
			sql: `SELECT`,
			err: "Found `` but expected `field name`",
		},
		{
			sql: `SELECT FROM`,
			err: "Found `FROM` but expected `field name`",
		},
		{
			sql: `SELECT *`,
			err: "Found `` but expected `FROM`",
		},
		{
			sql: `SELECT * FROM`,
			err: "Found `` but expected `table name`",
		},
		{
			sql: `SELECT * FROM per!son`,
			err: "Found `!` but expected `EOF, ;`",
		},
		{
			sql: `SELECT * FROM person;`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Table{Name: "person"}},
			}}},
		},
		{
			sql: `SELECT ALL FROM person;`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Table{Name: "person"}},
			}}},
		},
		{
			sql: `SELECT * FROM @`,
			err: "Found `` but expected `table name`",
		},
		{
			sql: `SELECT * FROM person:uuid`,
			err: "Found `:` but expected `EOF, ;`",
		},
		{
			sql: `SELECT * FROM @person`,
			err: "Found `` but expected `:`",
		},
		{
			sql: `SELECT * FROM @person:`,
			err: "Found `` but expected `table id`",
		},
		{
			sql: `SELECT * FROM person`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Table{Name: "person"}},
			}}},
		},
		{
			sql: `SELECT * FROM person, tweet`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Table{Name: "person"}, &Table{Name: "tweet"}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:1a`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Thing{Table: "person", Thing: "1a", ID: &IdentLiteral{Val: "1a"}}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:123456`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Thing{Table: "person", Thing: "123456", ID: &NumberLiteral{Val: 123456}}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:123.456`,
			err: "Found `123.456` but expected `table id`",
		},
		{
			sql: `SELECT * FROM @person:123.456.789.012`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Thing{Table: "person", Thing: "123.456.789.012", ID: &IdentLiteral{Val: "123.456.789.012"}}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:⟨123.456.789.012⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Thing{Table: "person", Thing: "123.456.789.012", ID: &IdentLiteral{Val: "123.456.789.012"}}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:{123.456.789.012}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Thing{Table: "person", Thing: "123.456.789.012", ID: &IdentLiteral{Val: "123.456.789.012"}}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:⟨A250C5A3-948F-4657-88AD-FF5F27B5B24E⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Thing{Table: "person", Thing: "A250C5A3-948F-4657-88AD-FF5F27B5B24E", ID: &IdentLiteral{Val: "A250C5A3-948F-4657-88AD-FF5F27B5B24E"}}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:{A250C5A3-948F-4657-88AD-FF5F27B5B24E}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Thing{Table: "person", Thing: "A250C5A3-948F-4657-88AD-FF5F27B5B24E", ID: &IdentLiteral{Val: "A250C5A3-948F-4657-88AD-FF5F27B5B24E"}}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:⟨8250C5A3-948F-4657-88AD-FF5F27B5B24E⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Thing{Table: "person", Thing: "8250C5A3-948F-4657-88AD-FF5F27B5B24E", ID: &IdentLiteral{Val: "8250C5A3-948F-4657-88AD-FF5F27B5B24E"}}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:{8250C5A3-948F-4657-88AD-FF5F27B5B24E}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Thing{Table: "person", Thing: "8250C5A3-948F-4657-88AD-FF5F27B5B24E", ID: &IdentLiteral{Val: "8250C5A3-948F-4657-88AD-FF5F27B5B24E"}}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:⟨Tobie Morgan Hitchcock⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Thing{Table: "person", Thing: "Tobie Morgan Hitchcock", ID: &IdentLiteral{Val: "Tobie Morgan Hitchcock"}}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:{Tobie Morgan Hitchcock}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Thing{Table: "person", Thing: "Tobie Morgan Hitchcock", ID: &IdentLiteral{Val: "Tobie Morgan Hitchcock"}}},
			}}},
		},
		{
			sql: `SELECT * FROM @{email addresses}:⟨tobie@abcum.com⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Thing{Table: "email addresses", Thing: "tobie@abcum.com", ID: &IdentLiteral{Val: "tobie@abcum.com"}}},
			}}},
		},
		{
			sql: `SELECT * FROM @{email addresses}:{tobie@abcum.com}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Thing{Table: "email addresses", Thing: "tobie@abcum.com", ID: &IdentLiteral{Val: "tobie@abcum.com"}}},
			}}},
		},
		{
			sql: `SELECT * FROM @{email addresses}:⟨tobie+spam@abcum.com⟩`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Thing{Table: "email addresses", Thing: "tobie+spam@abcum.com", ID: &IdentLiteral{Val: "tobie+spam@abcum.com"}}},
			}}},
		},
		{
			sql: `SELECT * FROM @{email addresses}:{tobie+spam@abcum.com}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Thing{Table: "email addresses", Thing: "tobie+spam@abcum.com", ID: &IdentLiteral{Val: "tobie+spam@abcum.com"}}},
			}}},
		},
		{
			sql: `SELECT * FROM @{email addresses}:⟨this\qis\nodd⟩`,
			err: "Found `thisqis\nodd` but expected `table id`",
		},
		{
			sql: `SELECT * FROM @{email addresses}:{this\qis\nodd}`,
			err: "Found `this` but expected `table id`",
		},
		{
			sql: `SELECT * FROM @{email addresses}:⟨this\nis\nodd⟩`,
			err: "Found `this\nis\nodd` but expected `table id`",
		},
		{
			sql: `SELECT * FROM @{email addresses}:{this\nis\nodd}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Thing{Table: "email addresses", Thing: "this\\nis\\nodd", ID: &IdentLiteral{Val: "this\\nis\\nodd"}}},
			}}},
		},
		{
			sql: `SELECT *, temp AS test FROM person`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{
					{Expr: &Wildcard{}},
					{Expr: &IdentLiteral{Val: "temp"}, Alias: &IdentLiteral{Val: "test"}},
				},
				What: []Expr{&Table{Name: "person"}},
			}}},
		},
		{
			sql: "SELECT `email addresses` AS emails FROM person",
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{
					{Expr: &IdentLiteral{Val: "email addresses"}, Alias: &IdentLiteral{Val: "emails"}},
				},
				What: []Expr{&Table{Name: "person"}},
			}}},
		},
		{
			sql: "SELECT emails AS `email addresses` FROM person",
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{
					{Expr: &IdentLiteral{Val: "emails"}, Alias: &IdentLiteral{Val: "email addresses"}},
				},
				What: []Expr{&Table{Name: "person"}},
			}}},
		},
		{
			sql: "SELECT ALL FROM person WHERE id = '\x0A'",
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Table{Name: "person"}},
				Cond: []Expr{&BinaryExpression{LHS: &IdentLiteral{Val: "id"}, Op: "=", RHS: &StringLiteral{Val: "\n"}}},
			}}},
		},
		{
			sql: "SELECT ALL FROM person WHERE id = '\x0D'",
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Table{Name: "person"}},
				Cond: []Expr{&BinaryExpression{LHS: &IdentLiteral{Val: "id"}, Op: "=", RHS: &StringLiteral{Val: "\r"}}},
			}}},
		},
		{
			sql: `SELECT ALL FROM person WHERE id = "\b\n\r\t"`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Table{Name: "person"}},
				Cond: []Expr{&BinaryExpression{LHS: &IdentLiteral{Val: "id"}, Op: "=", RHS: &StringLiteral{Val: "\n\r\t"}}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE`,
			err: "Found `` but expected `field name`",
		},
		{
			sql: `SELECT * FROM person WHERE id`,
			err: "Found `` but expected `IN, =, !=, >, <, >=, <=, =~, !~, ∋, ∌`",
		},
		{
			sql: `SELECT * FROM person WHERE id =`,
			err: "Found `` but expected `field value`",
		},
		{
			sql: `SELECT * FROM person WHERE id = 1`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Table{Name: "person"}},
				Cond: []Expr{&BinaryExpression{LHS: &IdentLiteral{Val: "id"}, Op: "=", RHS: &NumberLiteral{Val: 1}}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE old != EMPTY AND old = true`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Table{Name: "person"}},
				Cond: []Expr{
					&BinaryExpression{LHS: &IdentLiteral{Val: "old"}, Op: "!=", RHS: &Empty{}},
					&BinaryExpression{LHS: &IdentLiteral{Val: "old"}, Op: "=", RHS: &BooleanLiteral{Val: true}},
				},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE old != EMPTY AND old = false`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Table{Name: "person"}},
				Cond: []Expr{
					&BinaryExpression{LHS: &IdentLiteral{Val: "old"}, Op: "!=", RHS: &Empty{}},
					&BinaryExpression{LHS: &IdentLiteral{Val: "old"}, Op: "=", RHS: &BooleanLiteral{Val: false}},
				},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE id != null AND id != EMPTY AND id > 13.9 AND id < 31 AND id >= 15 AND id <= 29.9`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Table{Name: "person"}},
				Cond: []Expr{
					&BinaryExpression{LHS: &IdentLiteral{Val: "id"}, Op: "!=", RHS: &Null{}},
					&BinaryExpression{LHS: &IdentLiteral{Val: "id"}, Op: "!=", RHS: &Empty{}},
					&BinaryExpression{LHS: &IdentLiteral{Val: "id"}, Op: ">", RHS: &DoubleLiteral{Val: 13.9}},
					&BinaryExpression{LHS: &IdentLiteral{Val: "id"}, Op: "<", RHS: &NumberLiteral{Val: 31}},
					&BinaryExpression{LHS: &IdentLiteral{Val: "id"}, Op: ">=", RHS: &NumberLiteral{Val: 15}},
					&BinaryExpression{LHS: &IdentLiteral{Val: "id"}, Op: "<=", RHS: &DoubleLiteral{Val: 29.9}},
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
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Table{Name: "person"}},
				Cond: []Expr{&BinaryExpression{LHS: &IdentLiteral{Val: "test"}, Op: "IN", RHS: &ArrayLiteral{Val: []interface{}{"London", "Paris"}}}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE test = {`,
			err: "Found `` but expected `field value`",
		},
		{
			sql: `SELECT * FROM person WHERE test = {"name","London"}`,
			err: `Invalid JSON: {"name","London"}`,
		},
		{
			sql: "SELECT * FROM person WHERE test = {\"name\":\"\x0A\"}",
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Table{Name: "person"}},
				Cond: []Expr{&BinaryExpression{LHS: &IdentLiteral{Val: "test"}, Op: "=", RHS: &JSONLiteral{Val: map[string]interface{}{"name": ""}}}},
			}}},
		},
		{
			sql: "SELECT * FROM person WHERE test = {\"name\":\"\x0D\"}",
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Table{Name: "person"}},
				Cond: []Expr{&BinaryExpression{LHS: &IdentLiteral{Val: "test"}, Op: "=", RHS: &JSONLiteral{Val: map[string]interface{}{"name": ""}}}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE test = {"name":"London"}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Table{Name: "person"}},
				Cond: []Expr{&BinaryExpression{LHS: &IdentLiteral{Val: "test"}, Op: "=", RHS: &JSONLiteral{Val: map[string]interface{}{"name": "London"}}}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE test = {"name":"\b\t\r\n\f\"\\"}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Table{Name: "person"}},
				Cond: []Expr{&BinaryExpression{LHS: &IdentLiteral{Val: "test"}, Op: "=", RHS: &JSONLiteral{Val: map[string]interface{}{"name": "\b\t\r\n\f\"\\"}}}},
			}}},
		},
		{
			sql: `SELECT * FROM person WHERE test = {"name":{"f":"first", "l":"last"}}`,
			res: &Query{Statements: []Statement{&SelectStatement{
				Expr: []*Field{{Expr: &Wildcard{}}},
				What: []Expr{&Table{Name: "person"}},
				Cond: []Expr{&BinaryExpression{LHS: &IdentLiteral{Val: "test"}, Op: "=", RHS: &JSONLiteral{Val: map[string]interface{}{"name": map[string]interface{}{"f": "first", "l": "last"}}}}},
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
			Expr: []*Field{{Expr: &Wildcard{}}},
			What: []Expr{&Table{Name: "person"}},
			Cond: []Expr{
				&BinaryExpression{LHS: &IdentLiteral{Val: "bday"}, Op: ">=", RHS: &DatetimeLiteral{Val: bday1}},
				&BinaryExpression{LHS: &IdentLiteral{Val: "bday"}, Op: ">=", RHS: &DatetimeLiteral{Val: bday2}},
				&BinaryExpression{LHS: &IdentLiteral{Val: "bday"}, Op: ">=", RHS: &DatetimeLiteral{Val: bday3}},
				&BinaryExpression{LHS: &IdentLiteral{Val: "bday"}, Op: "<=", RHS: &DatetimeLiteral{Val: bday4}},
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
			err: "Found `` but expected `table name`",
		},
		{
			sql: `INSERT INTO`,
			err: "Found `` but expected `table name`",
		},
		{
			sql: `CREATE person`,
			res: &Query{Statements: []Statement{&CreateStatement{
				What: []Expr{
					&Table{Name: "person"},
				},
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
			sql: `CREATE person SET firstname = EMPTY`,
			err: "Found `EMPTY` but expected `field value`",
		},
		{
			sql: `CREATE person SET firstname = VOID`,
			res: &Query{Statements: []Statement{&CreateStatement{
				What: []Expr{&Table{Name: "person"}},
				Data: []Expr{&BinaryExpression{LHS: &IdentLiteral{Val: "firstname"}, Op: "=", RHS: &Void{}}},
			}}},
		},
		{
			sql: `CREATE person SET firstname = "Tobie" something`,
			err: "Found `something` but expected `EOF, ;`",
		},
		{
			sql: `CREATE person SET firstname = "Tobie"`,
			res: &Query{Statements: []Statement{&CreateStatement{
				What: []Expr{&Table{Name: "person"}},
				Data: []Expr{&BinaryExpression{LHS: &IdentLiteral{Val: "firstname"}, Op: "=", RHS: &StringLiteral{Val: "Tobie"}}},
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
			err: "Found `something` but expected `EOF, ;`",
		},
		{
			sql: `CREATE person MERGE {"firstname":"Tobie"}`,
			res: &Query{Statements: []Statement{&CreateStatement{
				What: []Expr{&Table{Name: "person"}},
				Data: []Expr{&MergeExpression{JSON: &JSONLiteral{Val: map[string]interface{}{"firstname": "Tobie"}}}},
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
			err: "Found `something` but expected `EOF, ;`",
		},
		{
			sql: `CREATE person CONTENT {"firstname":"Tobie"}`,
			res: &Query{Statements: []Statement{&CreateStatement{
				What: []Expr{&Table{Name: "person"}},
				Data: []Expr{&ContentExpression{JSON: &JSONLiteral{Val: map[string]interface{}{"firstname": "Tobie"}}}},
			}}},
		},
		{
			sql: `CREATE person RETURN ID`,
			res: &Query{Statements: []Statement{&CreateStatement{
				What: []Expr{&Table{Name: "person"}},
				Echo: ID,
			}}},
		},
		{
			sql: `CREATE person RETURN NONE`,
			res: &Query{Statements: []Statement{&CreateStatement{
				What: []Expr{&Table{Name: "person"}},
				Echo: NONE,
			}}},
		},
		{
			sql: `CREATE person RETURN FULL`,
			res: &Query{Statements: []Statement{&CreateStatement{
				What: []Expr{&Table{Name: "person"}},
				Echo: FULL,
			}}},
		},
		{
			sql: `CREATE person RETURN BOTH`,
			res: &Query{Statements: []Statement{&CreateStatement{
				What: []Expr{&Table{Name: "person"}},
				Echo: BOTH,
			}}},
		},
		{
			sql: `CREATE person RETURN DIFF`,
			res: &Query{Statements: []Statement{&CreateStatement{
				What: []Expr{&Table{Name: "person"}},
				Echo: DIFF,
			}}},
		},
		{
			sql: `CREATE person RETURN BEFORE`,
			res: &Query{Statements: []Statement{&CreateStatement{
				What: []Expr{&Table{Name: "person"}},
				Echo: BEFORE,
			}}},
		},
		{
			sql: `CREATE person RETURN AFTER`,
			res: &Query{Statements: []Statement{&CreateStatement{
				What: []Expr{&Table{Name: "person"}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `CREATE person RETURN SOMETHING`,
			err: "Found `SOMETHING` but expected `ID, NONE, FULL, BOTH, DIFF, BEFORE, AFTER`",
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
			err: "Found `` but expected `table name`",
		},
		{
			sql: `UPSERT INTO`,
			err: "Found `` but expected `table name`",
		},
		{
			sql: `UPDATE person`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				What: []Expr{
					&Table{Name: "person"},
				},
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
			sql: `UPDATE person SET firstname = EMPTY`,
			err: "Found `EMPTY` but expected `field value`",
		},
		{
			sql: `UPDATE person SET firstname = VOID`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				What: []Expr{&Table{Name: "person"}},
				Data: []Expr{&BinaryExpression{LHS: &IdentLiteral{Val: "firstname"}, Op: "=", RHS: &Void{}}},
			}}},
		},
		{
			sql: `UPDATE person SET firstname = "Tobie" something`,
			err: "Found `something` but expected `EOF, ;`",
		},
		{
			sql: `UPDATE person SET firstname = "Tobie"`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				What: []Expr{&Table{Name: "person"}},
				Data: []Expr{&BinaryExpression{LHS: &IdentLiteral{Val: "firstname"}, Op: "=", RHS: &StringLiteral{Val: "Tobie"}}},
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
			err: "Found `something` but expected `EOF, ;`",
		},
		{
			sql: `UPDATE person MERGE {"firstname":"Tobie"}`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				What: []Expr{&Table{Name: "person"}},
				Data: []Expr{&MergeExpression{JSON: &JSONLiteral{Val: map[string]interface{}{"firstname": "Tobie"}}}},
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
			err: "Found `something` but expected `EOF, ;`",
		},
		{
			sql: `UPDATE person CONTENT {"firstname":"Tobie"}`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				What: []Expr{&Table{Name: "person"}},
				Data: []Expr{&ContentExpression{JSON: &JSONLiteral{Val: map[string]interface{}{"firstname": "Tobie"}}}},
			}}},
		},
		{
			sql: `UPDATE person RETURN ID`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				What: []Expr{&Table{Name: "person"}},
				Echo: ID,
			}}},
		},
		{
			sql: `UPDATE person RETURN NONE`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				What: []Expr{&Table{Name: "person"}},
				Echo: NONE,
			}}},
		},
		{
			sql: `UPDATE person RETURN FULL`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				What: []Expr{&Table{Name: "person"}},
				Echo: FULL,
			}}},
		},
		{
			sql: `UPDATE person RETURN BOTH`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				What: []Expr{&Table{Name: "person"}},
				Echo: BOTH,
			}}},
		},
		{
			sql: `UPDATE person RETURN DIFF`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				What: []Expr{&Table{Name: "person"}},
				Echo: DIFF,
			}}},
		},
		{
			sql: `UPDATE person RETURN BEFORE`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				What: []Expr{&Table{Name: "person"}},
				Echo: BEFORE,
			}}},
		},
		{
			sql: `UPDATE person RETURN AFTER`,
			res: &Query{Statements: []Statement{&UpdateStatement{
				What: []Expr{&Table{Name: "person"}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `UPDATE person RETURN SOMETHING`,
			err: "Found `SOMETHING` but expected `ID, NONE, FULL, BOTH, DIFF, BEFORE, AFTER`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Modify(t *testing.T) {

	var tests = []tester{
		{
			sql: `MODIFY`,
			err: "Found `` but expected `@`",
		},
		{
			sql: `MODIFY @person:test`,
			err: "Found `` but expected `DIFF`",
		},
		{
			sql: `MODIFY @person:test DIFF`,
			err: "Found `` but expected `json`",
		},
		{
			sql: `MODIFY @person:test DIFF {"diff": true}`,
			res: &Query{Statements: []Statement{&ModifyStatement{
				What: []Expr{&Thing{Table: "person", Thing: "test", ID: &IdentLiteral{Val: "test"}}},
				Diff: &DiffExpression{JSON: &JSONLiteral{Val: map[string]interface{}{"diff": true}}},
			}}},
		},
		{
			sql: `MODIFY @person:test DIFF {"diff": true} something`,
			err: "Found `something` but expected `EOF, ;`",
		},
		{
			sql: `MODIFY @person:test DIFF {"diff": true} RETURN ID`,
			res: &Query{Statements: []Statement{&ModifyStatement{
				What: []Expr{&Thing{Table: "person", Thing: "test", ID: &IdentLiteral{Val: "test"}}},
				Diff: &DiffExpression{JSON: &JSONLiteral{Val: map[string]interface{}{"diff": true}}},
				Echo: ID,
			}}},
		},
		{
			sql: `MODIFY @person:test DIFF {"diff": true} RETURN NONE`,
			res: &Query{Statements: []Statement{&ModifyStatement{
				What: []Expr{&Thing{Table: "person", Thing: "test", ID: &IdentLiteral{Val: "test"}}},
				Diff: &DiffExpression{JSON: &JSONLiteral{Val: map[string]interface{}{"diff": true}}},
				Echo: NONE,
			}}},
		},
		{
			sql: `MODIFY @person:test DIFF {"diff": true} RETURN FULL`,
			res: &Query{Statements: []Statement{&ModifyStatement{
				What: []Expr{&Thing{Table: "person", Thing: "test", ID: &IdentLiteral{Val: "test"}}},
				Diff: &DiffExpression{JSON: &JSONLiteral{Val: map[string]interface{}{"diff": true}}},
				Echo: FULL,
			}}},
		},
		{
			sql: `MODIFY @person:test DIFF {"diff": true} RETURN BOTH`,
			res: &Query{Statements: []Statement{&ModifyStatement{
				What: []Expr{&Thing{Table: "person", Thing: "test", ID: &IdentLiteral{Val: "test"}}},
				Diff: &DiffExpression{JSON: &JSONLiteral{Val: map[string]interface{}{"diff": true}}},
				Echo: BOTH,
			}}},
		},
		{
			sql: `MODIFY @person:test DIFF {"diff": true} RETURN DIFF`,
			res: &Query{Statements: []Statement{&ModifyStatement{
				What: []Expr{&Thing{Table: "person", Thing: "test", ID: &IdentLiteral{Val: "test"}}},
				Diff: &DiffExpression{JSON: &JSONLiteral{Val: map[string]interface{}{"diff": true}}},
				Echo: DIFF,
			}}},
		},
		{
			sql: `MODIFY @person:test DIFF {"diff": true} RETURN BEFORE`,
			res: &Query{Statements: []Statement{&ModifyStatement{
				What: []Expr{&Thing{Table: "person", Thing: "test", ID: &IdentLiteral{Val: "test"}}},
				Diff: &DiffExpression{JSON: &JSONLiteral{Val: map[string]interface{}{"diff": true}}},
				Echo: BEFORE,
			}}},
		},
		{
			sql: `MODIFY @person:test DIFF {"diff": true} RETURN AFTER`,
			res: &Query{Statements: []Statement{&ModifyStatement{
				What: []Expr{&Thing{Table: "person", Thing: "test", ID: &IdentLiteral{Val: "test"}}},
				Diff: &DiffExpression{JSON: &JSONLiteral{Val: map[string]interface{}{"diff": true}}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `MODIFY @person:test DIFF {"diff": true} RETURN SOMETHING`,
			err: "Found `SOMETHING` but expected `ID, NONE, FULL, BOTH, DIFF, BEFORE, AFTER`",
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
			err: "Found `` but expected `table name`",
		},
		{
			sql: `DELETE FROM`,
			err: "Found `` but expected `table name`",
		},
		{
			sql: `DELETE person`,
			res: &Query{Statements: []Statement{&DeleteStatement{
				What: []Expr{
					&Table{Name: "person"},
				},
			}}},
		},
		{
			sql: `DELETE person RETURN ID`,
			res: &Query{Statements: []Statement{&DeleteStatement{
				What: []Expr{&Table{Name: "person"}},
				Echo: ID,
			}}},
		},
		{
			sql: `DELETE person RETURN NONE`,
			res: &Query{Statements: []Statement{&DeleteStatement{
				What: []Expr{&Table{Name: "person"}},
				Echo: NONE,
			}}},
		},
		{
			sql: `DELETE person RETURN FULL`,
			res: &Query{Statements: []Statement{&DeleteStatement{
				What: []Expr{&Table{Name: "person"}},
				Echo: FULL,
			}}},
		},
		{
			sql: `DELETE person RETURN BOTH`,
			res: &Query{Statements: []Statement{&DeleteStatement{
				What: []Expr{&Table{Name: "person"}},
				Echo: BOTH,
			}}},
		},
		{
			sql: `DELETE person RETURN DIFF`,
			res: &Query{Statements: []Statement{&DeleteStatement{
				What: []Expr{&Table{Name: "person"}},
				Echo: DIFF,
			}}},
		},
		{
			sql: `DELETE person RETURN BEFORE`,
			res: &Query{Statements: []Statement{&DeleteStatement{
				What: []Expr{&Table{Name: "person"}},
				Echo: BEFORE,
			}}},
		},
		{
			sql: `DELETE person RETURN AFTER`,
			res: &Query{Statements: []Statement{&DeleteStatement{
				What: []Expr{&Table{Name: "person"}},
				Echo: AFTER,
			}}},
		},
		{
			sql: `DELETE person RETURN SOMETHING`,
			err: "Found `SOMETHING` but expected `ID, NONE, FULL, BOTH, DIFF, BEFORE, AFTER`",
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
			err: "Found `` but expected `TABLE, FIELD, INDEX`",
		},
		{
			sql: `DEFINE TABLE`,
			err: "Found `` but expected `table name`",
		},
		{
			sql: `DEFINE TABLE person`,
			res: &Query{Statements: []Statement{&DefineTableStatement{
				What: []Expr{&Table{Name: "person"}},
			}}},
		},
		{
			sql: `DEFINE TABLE person something`,
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
			err: "Found `` but expected `table name`",
		},
		{
			sql: `DEFINE FIELD temp ON person`,
			err: "Found `` but expected `TYPE`",
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE`,
			err: "Found `` but expected `IDENT, ARRAY`",
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name: &IdentLiteral{Val: "temp"},
				What: []Expr{&Table{Name: "person"}},
				Type: &IdentLiteral{Val: "any"},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE url`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name: &IdentLiteral{Val: "temp"},
				What: []Expr{&Table{Name: "person"}},
				Type: &IdentLiteral{Val: "url"},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE email`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name: &IdentLiteral{Val: "temp"},
				What: []Expr{&Table{Name: "person"}},
				Type: &IdentLiteral{Val: "email"},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE phone`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name: &IdentLiteral{Val: "temp"},
				What: []Expr{&Table{Name: "person"}},
				Type: &IdentLiteral{Val: "phone"},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE array`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name: &IdentLiteral{Val: "temp"},
				What: []Expr{&Table{Name: "person"}},
				Type: &IdentLiteral{Val: "array"},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE object`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name: &IdentLiteral{Val: "temp"},
				What: []Expr{&Table{Name: "person"}},
				Type: &IdentLiteral{Val: "object"},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE string`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name: &IdentLiteral{Val: "temp"},
				What: []Expr{&Table{Name: "person"}},
				Type: &IdentLiteral{Val: "string"},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE number`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name: &IdentLiteral{Val: "temp"},
				What: []Expr{&Table{Name: "person"}},
				Type: &IdentLiteral{Val: "number"},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE ["default","notdefault"]`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name: &IdentLiteral{Val: "temp"},
				What: []Expr{&Table{Name: "person"}},
				Type: &ArrayLiteral{Val: []interface{}{"default", "notdefault"}},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any DEFAULT true`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name:    &IdentLiteral{Val: "temp"},
				What:    []Expr{&Table{Name: "person"}},
				Type:    &IdentLiteral{Val: "any"},
				Default: &BooleanLiteral{Val: true},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any DEFAULT false`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name:    &IdentLiteral{Val: "temp"},
				What:    []Expr{&Table{Name: "person"}},
				Type:    &IdentLiteral{Val: "any"},
				Default: &BooleanLiteral{Val: false},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any DEFAULT 100`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name:    &IdentLiteral{Val: "temp"},
				What:    []Expr{&Table{Name: "person"}},
				Type:    &IdentLiteral{Val: "any"},
				Default: &NumberLiteral{Val: 100},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any DEFAULT "default"`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name:    &IdentLiteral{Val: "temp"},
				What:    []Expr{&Table{Name: "person"}},
				Type:    &IdentLiteral{Val: "any"},
				Default: &StringLiteral{Val: "default"},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any DEFAULT "this\nis\nsome\ntext"`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name:    &IdentLiteral{Val: "temp"},
				What:    []Expr{&Table{Name: "person"}},
				Type:    &IdentLiteral{Val: "any"},
				Default: &StringLiteral{Val: "this\nis\nsome\ntext"},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any DEFAULT {"default":true}`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name:    &IdentLiteral{Val: "temp"},
				What:    []Expr{&Table{Name: "person"}},
				Type:    &IdentLiteral{Val: "any"},
				Default: &JSONLiteral{Val: map[string]interface{}{"default": true}},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any DEFAULT something`,
			err: "Found `something` but expected `TRUE, FALSE, NUMBER, STRING, REGION, ARRAY, JSON`",
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any MIN`,
			err: "Found `` but expected `number`",
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any MIN 1`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name: &IdentLiteral{Val: "temp"},
				What: []Expr{&Table{Name: "person"}},
				Type: &IdentLiteral{Val: "any"},
				Min:  &NumberLiteral{Val: 1},
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
				Name: &IdentLiteral{Val: "temp"},
				What: []Expr{&Table{Name: "person"}},
				Type: &IdentLiteral{Val: "any"},
				Max:  &NumberLiteral{Val: 100},
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any MAX something`,
			err: "Found `something` but expected `number`",
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any NOTNULL`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name:    &IdentLiteral{Val: "temp"},
				What:    []Expr{&Table{Name: "person"}},
				Type:    &IdentLiteral{Val: "any"},
				Notnull: true,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any NOTNULL true`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name:    &IdentLiteral{Val: "temp"},
				What:    []Expr{&Table{Name: "person"}},
				Type:    &IdentLiteral{Val: "any"},
				Notnull: true,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any NOTNULL false`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name:    &IdentLiteral{Val: "temp"},
				What:    []Expr{&Table{Name: "person"}},
				Type:    &IdentLiteral{Val: "any"},
				Notnull: false,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any READONLY`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name:     &IdentLiteral{Val: "temp"},
				What:     []Expr{&Table{Name: "person"}},
				Type:     &IdentLiteral{Val: "any"},
				Readonly: true,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any READONLY true`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name:     &IdentLiteral{Val: "temp"},
				What:     []Expr{&Table{Name: "person"}},
				Type:     &IdentLiteral{Val: "any"},
				Readonly: true,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any READONLY false`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name:     &IdentLiteral{Val: "temp"},
				What:     []Expr{&Table{Name: "person"}},
				Type:     &IdentLiteral{Val: "any"},
				Readonly: false,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any MANDATORY`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name:      &IdentLiteral{Val: "temp"},
				What:      []Expr{&Table{Name: "person"}},
				Type:      &IdentLiteral{Val: "any"},
				Mandatory: true,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any MANDATORY true`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name:      &IdentLiteral{Val: "temp"},
				What:      []Expr{&Table{Name: "person"}},
				Type:      &IdentLiteral{Val: "any"},
				Mandatory: true,
			}}},
		},
		{
			sql: `DEFINE FIELD temp ON person TYPE any MANDATORY false`,
			res: &Query{Statements: []Statement{&DefineFieldStatement{
				Name:      &IdentLiteral{Val: "temp"},
				What:      []Expr{&Table{Name: "person"}},
				Type:      &IdentLiteral{Val: "any"},
				Mandatory: false,
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
			err: "Found `` but expected `table name`",
		},
		{
			sql: `DEFINE INDEX temp ON person`,
			err: "Found `` but expected `CODE, COLUMNS`",
		},
		{
			sql: `DEFINE INDEX temp ON person CODE`,
			err: "Found `` but expected `LUA script`",
		},
		{
			sql: `DEFINE INDEX temp ON person CODE ""`,
			res: &Query{Statements: []Statement{&DefineIndexStatement{
				Name: &IdentLiteral{Val: "temp"},
				What: []Expr{&Table{Name: "person"}},
				Code: &CodeExpression{CODE: &StringLiteral{Val: ""}},
			}}},
		},
		{
			sql: `DEFINE INDEX temp ON person CODE "\nemit()\n"`,
			res: &Query{Statements: []Statement{&DefineIndexStatement{
				Name: &IdentLiteral{Val: "temp"},
				What: []Expr{&Table{Name: "person"}},
				Code: &CodeExpression{CODE: &StringLiteral{Val: "\nemit()\n"}},
			}}},
		},
		{
			sql: `DEFINE INDEX temp ON person COLUMNS`,
			err: "Found `` but expected `field name`",
		},
		{
			sql: `DEFINE INDEX temp ON person COLUMNS firstname, lastname`,
			res: &Query{Statements: []Statement{&DefineIndexStatement{
				Name: &IdentLiteral{Val: "temp"},
				What: []Expr{&Table{Name: "person"}},
				Cols: []*Field{
					{Expr: &IdentLiteral{Val: "firstname"}},
					{Expr: &IdentLiteral{Val: "lastname"}},
				},
				Uniq: false,
			}}},
		},
		{
			sql: `DEFINE INDEX temp ON person COLUMNS firstname, lastname UNIQUE`,
			res: &Query{Statements: []Statement{&DefineIndexStatement{
				Name: &IdentLiteral{Val: "temp"},
				What: []Expr{&Table{Name: "person"}},
				Cols: []*Field{
					{Expr: &IdentLiteral{Val: "firstname"}},
					{Expr: &IdentLiteral{Val: "lastname"}},
				},
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
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Resync(t *testing.T) {

	var tests = []tester{
		{
			sql: `RESYNC`,
			err: "Found `` but expected `INDEX`",
		},

		{
			sql: `RESYNC INDEX`,
			err: "Found `` but expected `name`",
		},
		{
			sql: `RESYNC INDEX temp`,
			err: "Found `` but expected `ON`",
		},
		{
			sql: `RESYNC INDEX temp ON`,
			err: "Found `` but expected `table name`",
		},
		{
			sql: `RESYNC INDEX temp ON person`,
			res: &Query{Statements: []Statement{&ResyncIndexStatement{
				Name: &IdentLiteral{Val: "temp"},
				What: []Expr{&Table{Name: "person"}},
			}}},
		},
		{
			sql: `RESYNC INDEX temp ON person something`,
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
			err: "Found `` but expected `TABLE, FIELD, INDEX`",
		},
		{
			sql: `REMOVE TABLE`,
			err: "Found `` but expected `table name`",
		},
		{
			sql: `REMOVE TABLE person`,
			res: &Query{Statements: []Statement{&RemoveTableStatement{
				What: []Expr{&Table{Name: "person"}},
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
			err: "Found `` but expected `table name`",
		},
		{
			sql: `REMOVE FIELD temp ON person`,
			res: &Query{Statements: []Statement{&RemoveFieldStatement{
				Name: &IdentLiteral{Val: "temp"},
				What: []Expr{&Table{Name: "person"}},
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
			err: "Found `` but expected `table name`",
		},
		{
			sql: `REMOVE INDEX temp ON person`,
			res: &Query{Statements: []Statement{&RemoveIndexStatement{
				Name: &IdentLiteral{Val: "temp"},
				What: []Expr{&Table{Name: "person"}},
			}}},
		},
		{
			sql: `REMOVE INDEX temp ON person something`,
			err: "Found `something` but expected `EOF, ;`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}
