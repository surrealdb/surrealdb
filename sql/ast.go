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
	"strconv"
	"time"
)

// --------------------------------------------------
// Queries
// --------------------------------------------------

// Query represents a multi statement SQL query
type Query struct {
	Statements Statements
}

// Statement represents a single SQL AST
type Statement interface{}

// Statements represents multiple SQL ASTs
type Statements []Statement

// --------------------------------------------------
// Trans
// --------------------------------------------------

// UseStatement represents a SQL BEGIN TRANSACTION statement.
type BeginStatement struct{}

// UseStatement represents a SQL CANCEL TRANSACTION statement.
type CancelStatement struct{}

// UseStatement represents a SQL COMMIT TRANSACTION statement.
type CommitStatement struct{}

// --------------------------------------------------
// Use
// --------------------------------------------------

// UseStatement represents a SQL USE statement.
type UseStatement struct {
	NS string `cork:"-" codec:"-"`
	DB string `cork:"-" codec:"-"`
}

// --------------------------------------------------
// Info
// --------------------------------------------------

// InfoStatement represents an SQL INFO statement.
type InfoStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	What string `cork:"-" codec:"-"`
}

// --------------------------------------------------
// LET
// --------------------------------------------------

// LetStatement represents a SQL LET statement.
type LetStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Name string `cork:"-" codec:"-"`
	What Expr   `cork:"-" codec:"-"`
}

// ReturnStatement represents a SQL RETURN statement.
type ReturnStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	What Expr   `cork:"-" codec:"-"`
}

// --------------------------------------------------
// Normal
// --------------------------------------------------

// LiveStatement represents a SQL LIVE statement.
type LiveStatement struct {
	KV   string   `cork:"-" codec:"-"`
	NS   string   `cork:"-" codec:"-"`
	DB   string   `cork:"-" codec:"-"`
	Expr []*Field `cork:"expr" codec:"expr"`
	What []Expr   `cork:"what" codec:"what"`
	Cond Expr     `cork:"cond" codec:"cond"`
	Echo Token    `cork:"echo" codec:"echo"`
}

// SelectStatement represents a SQL SELECT statement.
type SelectStatement struct {
	KV      string   `cork:"-" codec:"-"`
	NS      string   `cork:"-" codec:"-"`
	DB      string   `cork:"-" codec:"-"`
	Expr    []*Field `cork:"expr" codec:"expr"`
	What    []Expr   `cork:"what" codec:"what"`
	Cond    Expr     `cork:"cond" codec:"cond"`
	Group   []*Group `cork:"group" codec:"group"`
	Order   []*Order `cork:"order" codec:"order"`
	Limit   Expr     `cork:"limit" codec:"limit"`
	Start   Expr     `cork:"start" codec:"start"`
	Version Expr     `cork:"version" codec:"version"`
	Echo    Token    `cork:"echo" codec:"echo"`
}

// CreateStatement represents a SQL CREATE statement.
type CreateStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	What []Expr `cork:"what" codec:"what"`
	Data []Expr `cork:"data" codec:"data"`
	Echo Token  `cork:"echo" codec:"echo"`
}

// UpdateStatement represents a SQL UPDATE statement.
type UpdateStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Hard bool   `cork:"hard" codec:"hard"`
	What []Expr `cork:"what" codec:"what"`
	Data []Expr `cork:"data" codec:"data"`
	Cond Expr   `cork:"cond" codec:"cond"`
	Echo Token  `cork:"echo" codec:"echo"`
}

// DeleteStatement represents a SQL DELETE statement.
type DeleteStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Hard bool   `cork:"hard" codec:"hard"`
	What []Expr `cork:"what" codec:"what"`
	Cond Expr   `cork:"cond" codec:"cond"`
	Echo Token  `cork:"echo" codec:"echo"`
}

// RelateStatement represents a SQL RELATE statement.
type RelateStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Type Expr   `cork:"type" codec:"type"`
	From []Expr `cork:"from" codec:"from"`
	With []Expr `cork:"with" codec:"with"`
	Data []Expr `cork:"data" codec:"data"`
	Uniq bool   `cork:"uniq" codec:"uniq"`
	Echo Token  `cork:"echo" codec:"echo"`
}

// --------------------------------------------------
// Namespace
// --------------------------------------------------

type DefineNamespaceStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Name string `cork:"name" codec:"name"`
}

type RemoveNamespaceStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Name string `cork:"name" codec:"name"`
}

// --------------------------------------------------
// Database
// --------------------------------------------------

type DefineDatabaseStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Name string `cork:"name" codec:"name"`
}

type RemoveDatabaseStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Name string `cork:"name" codec:"name"`
}

// --------------------------------------------------
// Login
// --------------------------------------------------

// DefineLoginStatement represents an SQL DEFINE LOGIN statement.
type DefineLoginStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Kind Token  `cork:"kind" codec:"kind"`
	User string `cork:"user" codec:"user"`
	Pass string `cork:"pass" codec:"pass"`
}

// RemoveLoginStatement represents an SQL REMOVE LOGIN statement.
type RemoveLoginStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Kind Token  `cork:"kind" codec:"kind"`
	User string `cork:"user" codec:"user"`
}

// --------------------------------------------------
// Token
// --------------------------------------------------

// DefineTokenStatement represents an SQL DEFINE TOKEN statement.
type DefineTokenStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Kind Token  `cork:"kind" codec:"kind"`
	Name string `cork:"name" codec:"name"`
	Type string `cork:"type" codec:"type"`
	Text string `cork:"text" codec:"text"`
}

// RemoveTokenStatement represents an SQL REMOVE TOKEN statement.
type RemoveTokenStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Kind Token  `cork:"kind" codec:"kind"`
	Name string `cork:"name" codec:"name"`
}

// --------------------------------------------------
// Scope
// --------------------------------------------------

// DefineScopeStatement represents an SQL DEFINE SCOPE statement.
type DefineScopeStatement struct {
	KV     string        `cork:"-" codec:"-"`
	NS     string        `cork:"-" codec:"-"`
	DB     string        `cork:"-" codec:"-"`
	Name   string        `cork:"name" codec:"name"`
	Time   time.Duration `cork:"time" codec:"time"`
	Signup Expr          `cork:"signup" codec:"signup"`
	Signin Expr          `cork:"signin" codec:"signin"`
}

// RemoveScopeStatement represents an SQL REMOVE SCOPE statement.
type RemoveScopeStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Name string `cork:"name" codec:"name"`
}

// --------------------------------------------------
// Table
// --------------------------------------------------

// DefineTableStatement represents an SQL DEFINE TABLE statement.
type DefineTableStatement struct {
	KV   string   `cork:"-" codec:"-"`
	NS   string   `cork:"-" codec:"-"`
	DB   string   `cork:"-" codec:"-"`
	What []string `cork:"-" codec:"-"`
	Full bool     `cork:"full" codec:"full"`
}

// RemoveTableStatement represents an SQL REMOVE TABLE statement.
type RemoveTableStatement struct {
	KV   string   `cork:"-" codec:"-"`
	NS   string   `cork:"-" codec:"-"`
	DB   string   `cork:"-" codec:"-"`
	What []string `cork:"-" codec:"-"`
}

// --------------------------------------------------
// Rules
// --------------------------------------------------

// DefineRulesStatement represents an SQL DEFINE RULES statement.
type DefineRulesStatement struct {
	KV   string   `cork:"-" codec:"-"`
	NS   string   `cork:"-" codec:"-"`
	DB   string   `cork:"-" codec:"-"`
	What []string `cork:"-" codec:"-"`
	When []string `cork:"-" codec:"-"`
	Rule string   `cork:"rule" codec:"rule"`
	Cond Expr     `cork:"cond" codec:"cond"`
}

// RemoveRulesStatement represents an SQL REMOVE RULES statement.
type RemoveRulesStatement struct {
	KV   string   `cork:"-" codec:"-"`
	NS   string   `cork:"-" codec:"-"`
	DB   string   `cork:"-" codec:"-"`
	What []string `cork:"-" codec:"-"`
	When []string `cork:"-" codec:"-"`
}

// --------------------------------------------------
// Field
// --------------------------------------------------

// DefineFieldStatement represents an SQL DEFINE FIELD statement.
type DefineFieldStatement struct {
	KV        string        `cork:"-" codec:"-"`
	NS        string        `cork:"-" codec:"-"`
	DB        string        `cork:"-" codec:"-"`
	Name      string        `cork:"name" codec:"name"`
	What      []string      `cork:"-" codec:"-"`
	Type      string        `cork:"type" codec:"type"`
	Enum      []interface{} `cork:"enum" codec:"enum"`
	Code      string        `cork:"code" codec:"code"`
	Min       float64       `cork:"min" codec:"min"`
	Max       float64       `cork:"max" codec:"max"`
	Match     string        `cork:"match" codec:"match"`
	Default   interface{}   `cork:"default" codec:"default"`
	Notnull   bool          `cork:"notnull" codec:"notnull"`
	Readonly  bool          `cork:"readonly" codec:"readonly"`
	Mandatory bool          `cork:"mandatory" codec:"mandatory"`
	Validate  bool          `cork:"validate" codec:"validate"`
}

// RemoveFieldStatement represents an SQL REMOVE FIELD statement.
type RemoveFieldStatement struct {
	KV   string   `cork:"-" codec:"-"`
	NS   string   `cork:"-" codec:"-"`
	DB   string   `cork:"-" codec:"-"`
	Name string   `cork:"-" codec:"-"`
	What []string `cork:"-" codec:"-"`
}

// --------------------------------------------------
// Index
// --------------------------------------------------

// DefineIndexStatement represents an SQL DEFINE INDEX statement.
type DefineIndexStatement struct {
	KV   string   `cork:"-" codec:"-"`
	NS   string   `cork:"-" codec:"-"`
	DB   string   `cork:"-" codec:"-"`
	Name string   `cork:"name" codec:"name"`
	What []string `cork:"-" codec:"-"`
	Cols []string `cork:"cols" codec:"cols"`
	Uniq bool     `cork:"uniq" codec:"uniq"`
}

// RemoveIndexStatement represents an SQL REMOVE INDEX statement.
type RemoveIndexStatement struct {
	KV   string   `cork:"-" codec:"-"`
	NS   string   `cork:"-" codec:"-"`
	DB   string   `cork:"-" codec:"-"`
	Name string   `cork:"-" codec:"-"`
	What []string `cork:"-" codec:"-"`
}

// --------------------------------------------------
// View
// --------------------------------------------------

// DefineViewStatement represents an SQL DEFINE VIEW statement.
type DefineViewStatement struct {
	KV    string   `cork:"-" codec:"-"`
	NS    string   `cork:"-" codec:"-"`
	DB    string   `cork:"-" codec:"-"`
	Name  string   `cork:"name" codec:"name"`
	Expr  []*Field `cork:"expr" codec:"expr"`
	What  []Expr   `cork:"what" codec:"what"`
	Cond  Expr     `cork:"cond" codec:"cond"`
	Group []*Group `cork:"group" codec:"group"`
}

// RemoveViewStatement represents an SQL REMOVE VIEW statement.
type RemoveViewStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Name string `cork:"-" codec:"-"`
}

// --------------------------------------------------
// Literals
// --------------------------------------------------

// Expr represents a sql expression
type Expr interface{}

// All represents a * expression.
type All struct{}

// Any represents a ? expression.
type Any struct{}

// Asc represents the ASC expression.
type Asc struct{}

// Desc represents the DESC expression.
type Desc struct{}

// Null represents a null expression.
type Null struct{}

// Void represents an expression which is not set.
type Void struct{}

// Empty represents an expression which is null or "".
type Empty struct{}

// Field represents a SELECT AS clause.
type Field struct {
	Expr  Expr
	Alias string
}

// Group represents a GROUP BY clause.
type Group struct {
	Expr Expr
}

// Order represents a ORDER BY clause.
type Order struct {
	Expr Expr
	Dir  Expr
}

// --------------------------------------------------
// Expressions
// --------------------------------------------------

// SubExpression represents a subquery.
type SubExpression struct {
	Expr Expr
}

// FuncExpression represents a function call.
type FuncExpression struct {
	Name string
	Args []Expr
}

// DataExpression represents a SET expression.
type DataExpression struct {
	LHS Expr
	Op  Token
	RHS Expr
}

// BinaryExpression represents a WHERE expression.
type BinaryExpression struct {
	LHS Expr
	Op  Token
	RHS Expr
}

// PathExpression represents a path expression.
type PathExpression struct {
	Expr []Expr
}

// PartExpression represents a path part expression.
type PartExpression struct {
	Part Expr
}

// PartExpression represents a path join expression.
type JoinExpression struct {
	Join Token
}

// SubpExpression represents a sub path expression.
type SubpExpression struct {
	What []Expr
	Name string
	Cond Expr
}

// DiffExpression represents a JSON to DIFF
type DiffExpression struct {
	JSON interface{}
}

// MergeExpression represents JSON to MERGE
type MergeExpression struct {
	JSON interface{}
}

// ContentExpression represents JSON to REPLACE
type ContentExpression struct {
	JSON interface{}
}

// --------------------------------------------------
// Parts
// --------------------------------------------------

// Ident comment
type Ident struct {
	ID string
}

func NewIdent(ID string) *Ident {
	return &Ident{ID}
}

// --------------------------------------------------
// Parts
// --------------------------------------------------

// Param comment
type Param struct {
	ID string
}

func NewParam(ID string) *Param {
	return &Param{ID}
}

// --------------------------------------------------
// Parts
// --------------------------------------------------

// Table comment
type Table struct {
	TB string
}

func NewTable(TB string) *Table {
	return &Table{TB}
}

// --------------------------------------------------
// Parts
// --------------------------------------------------

// Thing comment
type Thing struct {
	TB interface{}
	ID interface{}
}

func NewThing(TB interface{}, ID interface{}) *Thing {
	if str, ok := ID.(string); ok {
		if cnv, err := strconv.ParseFloat(str, 64); err == nil {
			if cnv == float64(int64(cnv)) {
				return &Thing{TB: TB, ID: int64(cnv)}
			}
			return &Thing{TB: TB, ID: cnv}
		} else if cnv, err := time.Parse(RFCDate, str); err == nil {
			return &Thing{TB: TB, ID: cnv.UTC()}
		} else if cnv, err := time.Parse(RFCTime, str); err == nil {
			return &Thing{TB: TB, ID: cnv.UTC()}
		} else if cnv, err := time.Parse(RFCNorm, str); err == nil {
			return &Thing{TB: TB, ID: cnv.UTC()}
		} else if cnv, err := time.Parse(RFCText, str); err == nil {
			return &Thing{TB: TB, ID: cnv.UTC()}
		}
	}
	return &Thing{TB: TB, ID: ID}
}
