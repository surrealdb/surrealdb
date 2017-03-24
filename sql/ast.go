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
// Other
// --------------------------------------------------

type AuthableStatement interface {
	Auth() (string, string)
}

type KillableStatement interface {
	Begin()
	Cease()
	Timedout() <-chan struct{}
}

type killable struct {
	ticker *time.Timer
	closer chan struct{}
}

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
	Kind Token  `cork:"-" codec:"-"`
	What *Table `cork:"-" codec:"-"`
}

// --------------------------------------------------
// LET
// --------------------------------------------------

// LetStatement represents a SQL LET statement.
type LetStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Name *Ident `cork:"-" codec:"-"`
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
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Expr Fields `cork:"expr" codec:"expr"`
	What Exprs  `cork:"what" codec:"what"`
	Cond Expr   `cork:"cond" codec:"cond"`
	Echo Token  `cork:"echo" codec:"echo"`
}

// SelectStatement represents a SQL SELECT statement.
type SelectStatement struct {
	killable
	KV      string        `cork:"-" codec:"-"`
	NS      string        `cork:"-" codec:"-"`
	DB      string        `cork:"-" codec:"-"`
	Expr    Fields        `cork:"expr" codec:"expr"`
	What    Exprs         `cork:"what" codec:"what"`
	Cond    Expr          `cork:"cond" codec:"cond"`
	Group   Groups        `cork:"group" codec:"group"`
	Order   Orders        `cork:"order" codec:"order"`
	Limit   Expr          `cork:"limit" codec:"limit"`
	Start   Expr          `cork:"start" codec:"start"`
	Version Expr          `cork:"version" codec:"version"`
	Timeout time.Duration `cork:"timeout" codec:"timeout"`
}

// CreateStatement represents a SQL CREATE statement.
type CreateStatement struct {
	killable
	KV      string        `cork:"-" codec:"-"`
	NS      string        `cork:"-" codec:"-"`
	DB      string        `cork:"-" codec:"-"`
	What    Exprs         `cork:"what" codec:"what"`
	Data    Expr          `cork:"data" codec:"data"`
	Echo    Token         `cork:"echo" codec:"echo"`
	Timeout time.Duration `cork:"timeout" codec:"timeout"`
}

// UpdateStatement represents a SQL UPDATE statement.
type UpdateStatement struct {
	killable
	KV      string        `cork:"-" codec:"-"`
	NS      string        `cork:"-" codec:"-"`
	DB      string        `cork:"-" codec:"-"`
	Hard    bool          `cork:"hard" codec:"hard"`
	What    Exprs         `cork:"what" codec:"what"`
	Data    Expr          `cork:"data" codec:"data"`
	Cond    Expr          `cork:"cond" codec:"cond"`
	Echo    Token         `cork:"echo" codec:"echo"`
	Timeout time.Duration `cork:"timeout" codec:"timeout"`
}

// DeleteStatement represents a SQL DELETE statement.
type DeleteStatement struct {
	killable
	KV      string        `cork:"-" codec:"-"`
	NS      string        `cork:"-" codec:"-"`
	DB      string        `cork:"-" codec:"-"`
	Hard    bool          `cork:"hard" codec:"hard"`
	What    Exprs         `cork:"what" codec:"what"`
	Cond    Expr          `cork:"cond" codec:"cond"`
	Echo    Token         `cork:"echo" codec:"echo"`
	Timeout time.Duration `cork:"timeout" codec:"timeout"`
}

// RelateStatement represents a SQL RELATE statement.
type RelateStatement struct {
	killable
	KV      string        `cork:"-" codec:"-"`
	NS      string        `cork:"-" codec:"-"`
	DB      string        `cork:"-" codec:"-"`
	Type    Expr          `cork:"type" codec:"type"`
	From    Exprs         `cork:"from" codec:"from"`
	With    Exprs         `cork:"with" codec:"with"`
	Data    Expr          `cork:"data" codec:"data"`
	Uniq    bool          `cork:"uniq" codec:"uniq"`
	Echo    Token         `cork:"echo" codec:"echo"`
	Timeout time.Duration `cork:"timeout" codec:"timeout"`
}

// --------------------------------------------------
// Namespace
// --------------------------------------------------

type DefineNamespaceStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Name *Ident `cork:"name" codec:"name"`
}

type RemoveNamespaceStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Name *Ident `cork:"name" codec:"name"`
}

// --------------------------------------------------
// Database
// --------------------------------------------------

type DefineDatabaseStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Name *Ident `cork:"name" codec:"name"`
}

type RemoveDatabaseStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Name *Ident `cork:"name" codec:"name"`
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
	User *Ident `cork:"user" codec:"user"`
	Pass []byte `cork:"pass" codec:"pass"`
	Code []byte `cork:"code" codec:"code"`
}

// RemoveLoginStatement represents an SQL REMOVE LOGIN statement.
type RemoveLoginStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Kind Token  `cork:"kind" codec:"kind"`
	User *Ident `cork:"user" codec:"user"`
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
	Name *Ident `cork:"name" codec:"name"`
	Type string `cork:"type" codec:"type"`
	Code []byte `cork:"code" codec:"code"`
}

// RemoveTokenStatement represents an SQL REMOVE TOKEN statement.
type RemoveTokenStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Kind Token  `cork:"kind" codec:"kind"`
	Name *Ident `cork:"name" codec:"name"`
}

// --------------------------------------------------
// Scope
// --------------------------------------------------

// DefineScopeStatement represents an SQL DEFINE SCOPE statement.
type DefineScopeStatement struct {
	KV     string        `cork:"-" codec:"-"`
	NS     string        `cork:"-" codec:"-"`
	DB     string        `cork:"-" codec:"-"`
	Name   *Ident        `cork:"name" codec:"name"`
	Time   time.Duration `cork:"time" codec:"time"`
	Code   []byte        `cork:"code" codec:"code"`
	Signup Expr          `cork:"signup" codec:"signup"`
	Signin Expr          `cork:"signin" codec:"signin"`
}

// RemoveScopeStatement represents an SQL REMOVE SCOPE statement.
type RemoveScopeStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Name *Ident `cork:"name" codec:"name"`
}

// --------------------------------------------------
// Table
// --------------------------------------------------

// DefineTableStatement represents an SQL DEFINE TABLE statement.
type DefineTableStatement struct {
	KV   string          `cork:"-" codec:"-"`
	NS   string          `cork:"-" codec:"-"`
	DB   string          `cork:"-" codec:"-"`
	What Tables          `cork:"-" codec:"-"`
	Full bool            `cork:"full" codec:"full"`
	Perm *PermExpression `cork:"perm" codec:"perm"`
}

// RemoveTableStatement represents an SQL REMOVE TABLE statement.
type RemoveTableStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	What Tables `cork:"-" codec:"-"`
}

// --------------------------------------------------
// Field
// --------------------------------------------------

// DefineFieldStatement represents an SQL DEFINE FIELD statement.
type DefineFieldStatement struct {
	KV        string          `cork:"-" codec:"-"`
	NS        string          `cork:"-" codec:"-"`
	DB        string          `cork:"-" codec:"-"`
	Name      *Ident          `cork:"name" codec:"name"`
	What      Tables          `cork:"-" codec:"-"`
	Type      string          `cork:"type" codec:"type"`
	Perm      *PermExpression `cork:"perm" codec:"perm"`
	Enum      Array           `cork:"enum" codec:"enum"`
	Code      string          `cork:"code" codec:"code"`
	Min       float64         `cork:"min" codec:"min"`
	Max       float64         `cork:"max" codec:"max"`
	Match     string          `cork:"match" codec:"match"`
	Default   interface{}     `cork:"default" codec:"default"`
	Notnull   bool            `cork:"notnull" codec:"notnull"`
	Readonly  bool            `cork:"readonly" codec:"readonly"`
	Mandatory bool            `cork:"mandatory" codec:"mandatory"`
	Validate  bool            `cork:"validate" codec:"validate"`
}

// RemoveFieldStatement represents an SQL REMOVE FIELD statement.
type RemoveFieldStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Name *Ident `cork:"-" codec:"-"`
	What Tables `cork:"-" codec:"-"`
}

// --------------------------------------------------
// Index
// --------------------------------------------------

// DefineIndexStatement represents an SQL DEFINE INDEX statement.
type DefineIndexStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Name *Ident `cork:"name" codec:"name"`
	What Tables `cork:"-" codec:"-"`
	Cols Idents `cork:"cols" codec:"cols"`
	Uniq bool   `cork:"uniq" codec:"uniq"`
}

// RemoveIndexStatement represents an SQL REMOVE INDEX statement.
type RemoveIndexStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Name *Ident `cork:"-" codec:"-"`
	What Tables `cork:"-" codec:"-"`
}

// --------------------------------------------------
// View
// --------------------------------------------------

// DefineViewStatement represents an SQL DEFINE VIEW statement.
type DefineViewStatement struct {
	KV    string `cork:"-" codec:"-"`
	NS    string `cork:"-" codec:"-"`
	DB    string `cork:"-" codec:"-"`
	Name  *Ident `cork:"name" codec:"name"`
	Expr  Fields `cork:"expr" codec:"expr"`
	What  Exprs  `cork:"what" codec:"what"`
	Cond  Expr   `cork:"cond" codec:"cond"`
	Group Groups `cork:"group" codec:"group"`
}

// RemoveViewStatement represents an SQL REMOVE VIEW statement.
type RemoveViewStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Name *Ident `cork:"-" codec:"-"`
}

// --------------------------------------------------
// Literals
// --------------------------------------------------

// Expr represents a sql expression.
type Expr interface{}

// Exprs represents multiple sql expressions.
type Exprs []Expr

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

// Array represents a parsed json array.
type Array []interface{}

// Object represents a parsed json object.
type Object map[string]interface{}

// Field represents a SELECT AS clause.
type Field struct {
	Expr  Expr
	Alias *Ident
}

// Fields represents multiple SELECT AS clauses.
type Fields []*Field

// Group represents a GROUP BY clause.
type Group struct {
	Expr Expr
}

// Groups represents multiple GROUP BY clauses.
type Groups []*Group

// Order represents a ORDER BY clause.
type Order struct {
	Expr Expr
	Dir  Expr
}

// Orders represents multiple ORDER BY clauses.
type Orders []*Order

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
	Args Exprs
}

// ItemExpression represents a part of a SET expression.
type ItemExpression struct {
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
	Expr Exprs
}

// PartExpression represents a path part expression.
type PartExpression struct {
	Part Expr
}

// JoinExpression represents a path join expression.
type JoinExpression struct {
	Join Token
}

// SubpExpression represents a sub path expression.
type SubpExpression struct {
	What Exprs
	Name *Ident
	Cond Expr
}

// PermExpression represents a permissions expression.
type PermExpression struct {
	Select Expr
	Create Expr
	Update Expr
	Delete Expr
	Relate Expr
}

// DataExpression represents a SET expression.
type DataExpression struct {
	Data []*ItemExpression
}

// DiffExpression represents a JSON to DIFF
type DiffExpression struct {
	Data Expr
}

// MergeExpression represents JSON to MERGE
type MergeExpression struct {
	Data Expr
}

// ContentExpression represents JSON to REPLACE
type ContentExpression struct {
	Data Expr
}

// --------------------------------------------------
// Param
// --------------------------------------------------

// Params represents multiple Param clauses.
type Params []*Param

// Param comment
type Param struct {
	ID string
}

func NewParam(ID string) *Param {
	return &Param{ID}
}

// --------------------------------------------------
// Value
// --------------------------------------------------

// Values represents multiple Value clauses.
type Values []*Value

// Value comment
type Value struct {
	ID string
}

func NewValue(ID string) *Value {
	return &Value{ID}
}

// --------------------------------------------------
// Ident
// --------------------------------------------------

// Idents represents multiple Ident clauses.
type Idents []*Ident

// Ident comment
type Ident struct {
	ID string
}

func NewIdent(ID string) *Ident {
	return &Ident{ID}
}

// --------------------------------------------------
// Table
// --------------------------------------------------

// Tables represents multiple Table clauses.
type Tables []*Table

// Table comment
type Table struct {
	TB string
}

func NewTable(TB string) *Table {
	return &Table{TB}
}

// --------------------------------------------------
// Thing
// --------------------------------------------------

// Things represents multiple Thing clauses.
type Things []*Thing

// Thing comment
type Thing struct {
	TB string
	ID interface{}
}

func NewThing(TB string, ID interface{}) *Thing {
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
