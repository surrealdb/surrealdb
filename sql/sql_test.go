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

package sql_test

import (
	"strings"
	"testing"

	"github.com/abcum/surreal/sql"
	. "github.com/smartystreets/goconvey/convey"
)

type tester struct {
	skip bool
	sql  string
	err  string
	res  sql.Statement
}

func testerr(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

func testsql(t *testing.T, test tester) {

	s, e := sql.NewParser(strings.NewReader(test.sql)).Parse()

	if test.skip {
		Convey(" ❗️ "+test.sql, t, nil)
		return
	}

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

// Ensure the parser can parse a multi-statement query.
func Test_Parse_General(t *testing.T) {

	s := `SELECT a FROM b`
	q, err := sql.Parse(s)

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(q.Statements) != 1 {
		t.Fatalf("unexpected statement count: %d", len(q.Statements))
	}

}

// Ensure the parser can parse a multi-statement query.
func Test_Parse_General_Single(t *testing.T) {

	s := `SELECT a FROM b`
	q, err := sql.NewParser(strings.NewReader(s)).Parse()

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(q.Statements) != 1 {
		t.Fatalf("unexpected statement count: %d", len(q.Statements))
	}

}

// Ensure the parser can parse a multi-statement query.
func Test_Parse_General_Multi(t *testing.T) {

	s := `SELECT a FROM b; SELECT c FROM d`
	q, err := sql.NewParser(strings.NewReader(s)).Parse()

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(q.Statements) != 2 {
		t.Fatalf("unexpected statement count: %d", len(q.Statements))
	}

}

func Test_Parse_Queries_Malformed(t *testing.T) {

	var tests = []tester{
		{
			sql: "SELECT ` FROM person",
			err: "found ` FROM person` but expected `field name`",
		},
		{
			sql: `SELECT ' FROM person`,
			err: "found ` FROM person` but expected `field name`",
		},
		{
			sql: `SELECT " FROM person`,
			err: "found ` FROM person` but expected `field name`",
		},
		{
			sql: `SELECT "\" FROM person`,
			err: "found `\" FROM person` but expected `field name`",
		},
		{
			sql: `SELECT "\q" FROM person`,
			err: "found `` but expected `field name`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Select(t *testing.T) {

	var tests = []tester{
		{
			sql: `!`,
			err: "found `!` but expected `SELECT, INSERT, UPSERT, UPDATE, MODIFY, DELETE, RELATE, RECORD, DEFINE, RESYNC, REMOVE`",
		},
		{
			sql: `SELECT`,
			err: "found `` but expected `field name`",
		},
		{
			sql: `SELECT FROM`,
			err: "found `FROM` but expected `field name`",
		},
		{
			sql: `SELECT *`,
			err: "found `` but expected `FROM`",
		},
		{
			sql: `SELECT * FROM`,
			err: "found `` but expected `table name`",
		},
		{
			sql: `SELECT * FROM per!son`,
			err: "found `!` but expected `EOF, ;`",
		},
		{
			sql: `SELECT * FROM person;`,
			res: &sql.Query{Statements: []sql.Statement{&sql.SelectStatement{
				Fields: []*sql.Field{{Expr: &sql.Wildcard{}}},
				Thing:  []sql.Expr{&sql.Table{Name: "person"}},
			}}},
		},
		{
			sql: `SELECT ALL FROM person;`,
			res: &sql.Query{Statements: []sql.Statement{&sql.SelectStatement{
				Fields: []*sql.Field{{Expr: &sql.Wildcard{}}},
				Thing:  []sql.Expr{&sql.Table{Name: "person"}},
			}}},
		},
		{
			sql: `SELECT * FROM person;;;`,
			err: "found `;` but expected `SELECT, INSERT, UPSERT, UPDATE, MODIFY, DELETE, RELATE, RECORD, DEFINE, RESYNC, REMOVE`",
		},
		{
			sql: `SELECT * FROM @`,
			err: "found `` but expected `table name`",
		},
		{
			sql: `SELECT * FROM person:uuid`,
			err: "found `:` but expected `EOF, ;`",
		},
		{
			sql: `SELECT * FROM @person`,
			err: "found `` but expected `:`",
		},
		{
			sql: `SELECT * FROM @person:`,
			err: "found `` but expected `table id`",
		},
		{
			sql: `SELECT * FROM person`,
			res: &sql.Query{Statements: []sql.Statement{&sql.SelectStatement{
				Fields: []*sql.Field{{Expr: &sql.Wildcard{}}},
				Thing:  []sql.Expr{&sql.Table{Name: "person"}},
			}}},
		},
		{
			sql: `SELECT * FROM person, tweet`,
			res: &sql.Query{Statements: []sql.Statement{&sql.SelectStatement{
				Fields: []*sql.Field{{Expr: &sql.Wildcard{}}},
				Thing:  []sql.Expr{&sql.Table{Name: "person"}, &sql.Table{Name: "tweet"}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:123456`,
			res: &sql.Query{Statements: []sql.Statement{&sql.SelectStatement{
				Fields: []*sql.Field{{Expr: &sql.Wildcard{}}},
				Thing:  []sql.Expr{&sql.Thing{Table: "person", ID: "123456"}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:123.456`,
			err: "found `123.456` but expected `table id`",
		},
		{
			sql: `SELECT * FROM @person:123.456.789.012`,
			res: &sql.Query{Statements: []sql.Statement{&sql.SelectStatement{
				Fields: []*sql.Field{{Expr: &sql.Wildcard{}}},
				Thing:  []sql.Expr{&sql.Thing{Table: "person", ID: "123.456.789.012"}},
			}}},
		},
		{
			skip: true,
			sql:  `SELECT * FROM @person:A250C5A3-948F-4657-88AD-FF5F27B5B24E`,
			res: &sql.Query{Statements: []sql.Statement{&sql.SelectStatement{
				Fields: []*sql.Field{{Expr: &sql.Wildcard{}}},
				Thing:  []sql.Expr{&sql.Thing{Table: "person", ID: "A250C5A3-948F-4657-88AD-FF5F27B5B24E"}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:8250C5A3-948F-4657-88AD-FF5F27B5B24E`,
			res: &sql.Query{Statements: []sql.Statement{&sql.SelectStatement{
				Fields: []*sql.Field{{Expr: &sql.Wildcard{}}},
				Thing:  []sql.Expr{&sql.Thing{Table: "person", ID: "8250C5A3-948F-4657-88AD-FF5F27B5B24E"}},
			}}},
		},
		{
			sql: `SELECT * FROM @person:{Tobie Morgan Hitchcock}`,
			res: &sql.Query{Statements: []sql.Statement{&sql.SelectStatement{
				Fields: []*sql.Field{{Expr: &sql.Wildcard{}}},
				Thing:  []sql.Expr{&sql.Thing{Table: "person", ID: "Tobie Morgan Hitchcock"}},
			}}},
		},
		{
			sql: `SELECT * FROM @{email addresses}:{tobie@abcum.com}`,
			res: &sql.Query{Statements: []sql.Statement{&sql.SelectStatement{
				Fields: []*sql.Field{{Expr: &sql.Wildcard{}}},
				Thing:  []sql.Expr{&sql.Thing{Table: "email addresses", ID: "tobie@abcum.com"}},
			}}},
		},
		{
			sql: `SELECT * FROM @{email addresses}:{tobie+spam@abcum.com}`,
			res: &sql.Query{Statements: []sql.Statement{&sql.SelectStatement{
				Fields: []*sql.Field{{Expr: &sql.Wildcard{}}},
				Thing:  []sql.Expr{&sql.Thing{Table: "email addresses", ID: "tobie+spam@abcum.com"}},
			}}},
		},
		{
			sql: `SELECT * FROM @{email addresses}:{this\nis\nodd}`,
			err: "found `this\nis\nodd` but expected `table id`",
		},
		{
			sql: `SELECT * FROM @{email addresses}:{this\qis\nodd}`,
			err: "found `is` but expected `EOF, ;`",
		},
		{
			sql: `SELECT *, temp AS test FROM person`,
			res: &sql.Query{Statements: []sql.Statement{&sql.SelectStatement{
				Fields: []*sql.Field{
					{Expr: &sql.Wildcard{}},
					{Expr: &sql.IdentLiteral{Val: "temp"}, Alias: &sql.IdentLiteral{Val: "test"}},
				},
				Thing: []sql.Expr{&sql.Table{Name: "person"}},
			}}},
		},
		{
			sql: "SELECT `email addresses` AS emails FROM person",
			res: &sql.Query{Statements: []sql.Statement{&sql.SelectStatement{
				Fields: []*sql.Field{
					{Expr: &sql.IdentLiteral{Val: "email addresses"}, Alias: &sql.IdentLiteral{Val: "emails"}},
				},
				Thing: []sql.Expr{&sql.Table{Name: "person"}},
			}}},
		},
		{
			sql: "SELECT emails AS `email addresses` FROM person",
			res: &sql.Query{Statements: []sql.Statement{&sql.SelectStatement{
				Fields: []*sql.Field{
					{Expr: &sql.IdentLiteral{Val: "emails"}, Alias: &sql.IdentLiteral{Val: "email addresses"}},
				},
				Thing: []sql.Expr{&sql.Table{Name: "person"}},
			}}},
		},
		{
			sql: "SELECT * FROM person WHERE",
			err: "found `` but expected `field name`",
		},
		{
			sql: "SELECT * FROM person WHERE id",
			err: "found `` but expected `IN, =, !=, >, <, >=, <=, =~, !~, ∋, ∌`",
		},
		{
			sql: "SELECT * FROM person WHERE id =",
			err: "found `` but expected `field value`",
		},
		{
			sql: "SELECT * FROM person WHERE id = 1",
			res: &sql.Query{Statements: []sql.Statement{&sql.SelectStatement{
				Fields: []*sql.Field{{Expr: &sql.Wildcard{}}},
				Thing:  []sql.Expr{&sql.Table{Name: "person"}},
				Where:  []sql.Expr{&sql.BinaryExpression{LHS: &sql.IdentLiteral{Val: "id"}, Op: "=", RHS: &sql.NumberLiteral{Val: 1}}},
			}}},
		},
		{
			sql: "SELECT * FROM person WHERE id != 1 AND id > 14 AND id < 31 AND id >= 15 AND id <= 30",
			res: &sql.Query{Statements: []sql.Statement{&sql.SelectStatement{
				Fields: []*sql.Field{{Expr: &sql.Wildcard{}}},
				Thing:  []sql.Expr{&sql.Table{Name: "person"}},
				Where: []sql.Expr{
					&sql.BinaryExpression{LHS: &sql.IdentLiteral{Val: "id"}, Op: "!=", RHS: &sql.NumberLiteral{Val: 1}},
					&sql.BinaryExpression{LHS: &sql.IdentLiteral{Val: "id"}, Op: ">", RHS: &sql.NumberLiteral{Val: 14}},
					&sql.BinaryExpression{LHS: &sql.IdentLiteral{Val: "id"}, Op: "<", RHS: &sql.NumberLiteral{Val: 31}},
					&sql.BinaryExpression{LHS: &sql.IdentLiteral{Val: "id"}, Op: ">=", RHS: &sql.NumberLiteral{Val: 15}},
					&sql.BinaryExpression{LHS: &sql.IdentLiteral{Val: "id"}, Op: "<=", RHS: &sql.NumberLiteral{Val: 30}},
				},
			}}},
		},
		{
			skip: true,
			sql: `SELECT ALL,
	1a,
	12 AS int,
	13.90831 AS mean,
	{some thing} AS something,
	"some string" AS string
FROM
	@person:a1,
	@person:1a,
	@person:{Tobie Morgan Hitchcock},
	@{some table}:{Tobie Morgan Hitchcock},
	@{email addresses}:{tobie+spam@abcum.com}
WHERE
	id=true
	OR 30 > test
	OR firstname = "Tobie"
	OR firstname = lastname
	OR "London" IN tags
	OR account IN ["@account:abcum","@account:gibboo","@account:acreon"];`,
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Create(t *testing.T) {

	var tests = []tester{
		{
			sql: `CREATE`,
			err: "found `` but expected `table name`",
		},
		{
			sql: `CREATE INTO`,
			err: "found `` but expected `table name`",
		},
		{
			sql: `CREATE person`,
			err: "found `` but expected `SET`",
		},
		{
			sql: `CREATE person SET firstname`,
			err: "found `` but expected `=, +=, -=`",
		},
		{
			sql: `CREATE person SET firstname = "Tobie"`,
			res: &sql.Query{Statements: []sql.Statement{&sql.CreateStatement{
				What: &sql.Table{Name: "person"},
				Data: []sql.Expr{
					&sql.BinaryExpression{LHS: &sql.IdentLiteral{Val: "firstname"}, Op: "=", RHS: &sql.StringLiteral{Val: "Tobie"}},
				},
			}}},
		},
		{
			sql: `CREATE person SET firstname = "Tobie" something`,
			err: "found `something` but expected `EOF, ;`",
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
			err: "found `` but expected `table name`",
		},
		{
			sql: `INSERT INTO`,
			err: "found `` but expected `table name`",
		},
		{
			sql: `INSERT INTO person`,
			err: "found `` but expected `SET`",
		},
		{
			sql: `INSERT INTO person SET firstname`,
			err: "found `` but expected `=, +=, -=`",
		},
		{
			sql: `INSERT INTO person SET firstname = "Tobie"`,
			res: &sql.Query{Statements: []sql.Statement{&sql.InsertStatement{
				What: &sql.Table{Name: "person"},
				Data: []sql.Expr{
					&sql.BinaryExpression{LHS: &sql.IdentLiteral{Val: "firstname"}, Op: "=", RHS: &sql.StringLiteral{Val: "Tobie"}},
				},
			}}},
		},
		{
			sql: `INSERT INTO person SET firstname = "Tobie" something`,
			err: "found `something` but expected `EOF, ;`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Upsert(t *testing.T) {

	var tests = []tester{}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Update(t *testing.T) {

	var tests = []tester{}

	for _, test := range tests {
		testsql(t, test)
	}

}

func Test_Parse_Queries_Delete(t *testing.T) {

	var tests = []tester{}

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
			err: "found `` but expected `INDEX, VIEW`",
		},

		// VIEW

		{
			sql: `DEFINE VIEW`,
			err: "found `` but expected `name`",
		},
		{
			sql: `DEFINE VIEW temp`,
			err: "found `` but expected `MAP`",
		},
		{
			sql: `DEFINE VIEW temp MAP`,
			err: "found `` but expected `string`",
		},
		{
			sql: "DEFINE VIEW temp MAP ``",
			err: "found `` but expected `REDUCE`",
		},
		{
			sql: "DEFINE VIEW temp MAP `` REDUCE",
			err: "found `` but expected `string`",
		},
		{
			sql: "DEFINE VIEW temp MAP `` REDUCE ``",
			res: &sql.Query{Statements: []sql.Statement{&sql.DefineViewStatement{
				View:   &sql.IdentLiteral{Val: "temp"},
				Map:    &sql.StringLiteral{Val: ""},
				Reduce: &sql.StringLiteral{Val: ""},
			}}},
		},
		{
			sql: "DEFINE VIEW temp MAP `\nemit()\n` REDUCE `\nreturn sum()\n`",
			res: &sql.Query{Statements: []sql.Statement{&sql.DefineViewStatement{
				View:   &sql.IdentLiteral{Val: "temp"},
				Map:    &sql.StringLiteral{Val: "\nemit()\n"},
				Reduce: &sql.StringLiteral{Val: "\nreturn sum()\n"},
			}}},
		},
		{
			sql: `DEFINE VIEW temp
MAP "
if (meta.table == 'person') {
    if (doc.firstname && doc.lastname) {
        emit([doc.lastname, doc.firstname, meta.id], null)
    }
}
"
REDUCE "
return sum()
"`,
			res: &sql.Query{Statements: []sql.Statement{&sql.DefineViewStatement{
				View:   &sql.IdentLiteral{Val: "temp"},
				Map:    &sql.StringLiteral{Val: "\nif (meta.table == 'person') {\n    if (doc.firstname && doc.lastname) {\n        emit([doc.lastname, doc.firstname, meta.id], null)\n    }\n}\n"},
				Reduce: &sql.StringLiteral{Val: "\nreturn sum()\n"},
			}}},
		},
		{
			sql: "DEFINE VIEW temp MAP `` REDUCE `` something",
			err: "found `something` but expected `EOF, ;`",
		},

		// INDEX

		{
			sql: `DEFINE INDEX`,
			err: "found `` but expected `name`",
		},
		{
			sql: `DEFINE INDEX temp`,
			err: "found `` but expected `ON`",
		},
		{
			sql: `DEFINE INDEX temp ON`,
			err: "found `` but expected `table name`",
		},
		{
			sql: `DEFINE INDEX temp ON person`,
			err: "found `` but expected `COLUMNS`",
		},
		{
			sql: `DEFINE INDEX temp ON person COLUMNS`,
			err: "found `` but expected `field name`",
		},
		{
			sql: `DEFINE INDEX temp ON person COLUMNS firstname, lastname`,
			res: &sql.Query{Statements: []sql.Statement{&sql.DefineIndexStatement{
				Index: &sql.IdentLiteral{Val: "temp"},
				Table: &sql.Table{Name: "person"},
				Fields: []*sql.Field{
					{Expr: &sql.IdentLiteral{Val: "firstname"}},
					{Expr: &sql.IdentLiteral{Val: "lastname"}},
				},
				Unique: false,
			}}},
		},
		{
			sql: `DEFINE INDEX temp ON person COLUMNS firstname, lastname UNIQUE`,
			res: &sql.Query{Statements: []sql.Statement{&sql.DefineIndexStatement{
				Index: &sql.IdentLiteral{Val: "temp"},
				Table: &sql.Table{Name: "person"},
				Fields: []*sql.Field{
					{Expr: &sql.IdentLiteral{Val: "firstname"}},
					{Expr: &sql.IdentLiteral{Val: "lastname"}},
				},
				Unique: true,
			}}},
		},
		{
			sql: `DEFINE INDEX temp ON person COLUMNS firstname, lastname something UNIQUE`,
			err: "found `something` but expected `EOF, ;`",
		},
		{
			sql: `DEFINE INDEX temp ON person COLUMNS firstname, lastname UNIQUE something`,
			err: "found `something` but expected `EOF, ;`",
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
			err: "found `` but expected `INDEX, VIEW`",
		},

		// VIEW

		{
			sql: `RESYNC VIEW`,
			err: "found `` but expected `name`",
		},
		{
			sql: `RESYNC VIEW temp`,
			res: &sql.Query{Statements: []sql.Statement{&sql.ResyncViewStatement{
				View: &sql.IdentLiteral{Val: "temp"},
			}}},
		},
		{
			sql: `RESYNC VIEW temp something`,
			err: "found `something` but expected `EOF, ;`",
		},

		// INDEX

		{
			sql: `RESYNC INDEX`,
			err: "found `` but expected `name`",
		},
		{
			sql: `RESYNC INDEX temp`,
			err: "found `` but expected `ON`",
		},
		{
			sql: `RESYNC INDEX temp ON`,
			err: "found `` but expected `table name`",
		},
		{
			sql: `RESYNC INDEX temp ON person`,
			res: &sql.Query{Statements: []sql.Statement{&sql.ResyncIndexStatement{
				Index: &sql.IdentLiteral{Val: "temp"},
				Table: &sql.Table{Name: "person"},
			}}},
		},
		{
			sql: `RESYNC INDEX temp ON person something`,
			err: "found `something` but expected `EOF, ;`",
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
			err: "found `` but expected `INDEX, VIEW`",
		},

		// VIEW

		{
			sql: `REMOVE VIEW`,
			err: "found `` but expected `name`",
		},
		{
			sql: `REMOVE VIEW temp`,
			res: &sql.Query{Statements: []sql.Statement{&sql.RemoveViewStatement{
				View: &sql.IdentLiteral{Val: "temp"},
			}}},
		},
		{
			sql: `REMOVE VIEW temp something`,
			err: "found `something` but expected `EOF, ;`",
		},

		// INDEX

		{
			sql: `REMOVE INDEX`,
			err: "found `` but expected `name`",
		},
		{
			sql: `REMOVE INDEX temp`,
			err: "found `` but expected `ON`",
		},
		{
			sql: `REMOVE INDEX temp ON`,
			err: "found `` but expected `table name`",
		},
		{
			sql: `REMOVE INDEX temp ON person`,
			res: &sql.Query{Statements: []sql.Statement{&sql.RemoveIndexStatement{
				Index: &sql.IdentLiteral{Val: "temp"},
				Table: &sql.Table{Name: "person"},
			}}},
		},
		{
			sql: `RESYNC INDEX temp ON person something`,
			err: "found `something` but expected `EOF, ;`",
		},
	}

	for _, test := range tests {
		testsql(t, test)
	}

}
