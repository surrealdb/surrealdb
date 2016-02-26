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
	"regexp"
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
// Select
// --------------------------------------------------

// SelectStatement represents a SQL SELECT statement.
type SelectStatement struct {
	Fields  []*Field
	Thing   Expr
	Where   Expr
	Group   []*Group
	Order   []*Order
	Limit   Expr
	Start   Expr
	Version Expr
}

// --------------------------------------------------
// Items
// --------------------------------------------------

// CreateStatement represents a SQL CREATE statement.
// CREATE person SET column = 'value'
type CreateStatement struct {
	What Expr
	Data Expr
}

// InsertStatement represents a SQL INSERT statement.
// INSERT person SET column = 'value'
type InsertStatement struct {
	What Expr
	Data Expr
}

// UpsertStatement represents a SQL UPSERT statement.
// UPSERT @person:123 SET column = 'value'
type UpsertStatement struct {
	What Expr
	Data Expr
}

// UpdateStatement represents a SQL UPDATE statement.
// UPDATE person SET column = 'value'
type UpdateStatement struct {
	Thing Expr
	Data  Expr
	Where Expr
}

// DeleteStatement represents a SQL DELETE statement.
// DELETE FROMn person
type DeleteStatement struct {
	Thing Expr
	Where Expr
}

// RelateStatement represents a SQL RELATE statement.
// RELATE friend FROM @person:123 TO @person:456 SET column = 'value'
type RelateStatement struct {
	Kind Expr
	From *Thing
	To   *Thing
	Data Expr
}

// RecordStatement represents a SQL CREATE EVENT statement.
// RECORD login ON @person:123 AT 2016-01-29T22:42:56.478Z SET column = true
type RecordStatement struct {
	Name Expr
	ON   *Thing
	At   Expr
	Data Expr
}

// --------------------------------------------------
// Index
// --------------------------------------------------

// DefineIndexStatement represents an SQL DEFINE INDEX statement.
// DEFINE INDEX name ON person COLUMNS (account, age) UNIQUE
type DefineIndexStatement struct {
	Index  Expr
	Table  Expr
	Fields []*Field
	Unique Expr
}

// ResyncIndexStatement represents an SQL RESYNC INDEX statement.
// RESYNC INDEX name ON person
type ResyncIndexStatement struct {
	Index Expr
	Table Expr
}

// RemoveIndexStatement represents an SQL REMOVE INDEX statement.
// REMOVE INDEX name ON person
type RemoveIndexStatement struct {
	Index Expr
	Table Expr
}

// --------------------------------------------------
// Views
// --------------------------------------------------

// DefineViewStatement represents an SQL DEFINE VIEW statement.
// DEFINE VIEW name MAP `` REDUCE ``
type DefineViewStatement struct {
	View   Expr
	Map    Expr
	Reduce Expr
}

// ResyncViewStatement represents an SQL RESYNC VIEW statement.
// RESYNC VIEW name
type ResyncViewStatement struct {
	View Expr
}

// RemoveViewStatement represents an SQL REMOVE VIEW statement.
// REMOVE VIEW name
type RemoveViewStatement struct {
	View Expr
}

// --------------------------------------------------
// Literals
// --------------------------------------------------

// Expr represents a sql expression
type Expr interface{}

// Null represents a null expression.
type Null struct{}

// Wildcard represents a wildcard expression.
type Wildcard struct{}

// IdentLiteral represents a variable.
type IdentLiteral struct {
	Val string `json:"Ident"`
}

// JSONLiteral represents a regular expression.
type JSONLiteral struct {
	Val interface{} `json:"Json"`
}

// RegexLiteral represents a regular expression.
type RegexLiteral struct {
	Val *regexp.Regexp `json:"Regex"`
}

// NumberLiteral represents a integer literal.
type NumberLiteral struct {
	Val int64 `json:"Number"`
}

// DoubleLiteral represents a float literal.
type DoubleLiteral struct {
	Val float64 `json:"Double"`
}

// StringLiteral represents a string literal.
type StringLiteral struct {
	Val string `json:"String"`
}

// BooleanLiteral represents a boolean literal.
type BooleanLiteral struct {
	Val bool `json:"Boolean"`
}

// DatetimeLiteral represents a point-in-time literal.
type DatetimeLiteral struct {
	Val time.Time `json:"Datetime"`
}

// DurationLiteral represents a duration literal.
type DurationLiteral struct {
	Val time.Duration `json:"Duration"`
}

// DirectionLiteral represents a duration literal.
type DirectionLiteral struct {
	Val bool `json:"Direction"`
}

// ClosedExpression represents a parenthesized expression.
type ClosedExpression struct {
	Expr Expr
}

// BinaryExpression represents a binary expression tree,
type BinaryExpression struct {
	LHS Expr
	Op  string
	RHS Expr
}

// --------------------------------------------------
// Parts
// --------------------------------------------------

// Table comment
type Table struct {
	Name string `json:"Name"`
}

// Thing comment
type Thing struct {
	Table string `json:"Table"`
	ID    string `json:"ID"`
}

// Field comment
type Field struct {
	Expr  Expr
	Alias Expr
}

// Group comment
type Group struct {
	Expr Expr
}

// Order comment
type Order struct {
	Expr Expr
	Dir  Expr
}
