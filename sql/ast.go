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

// UseStatement represents a SQL USE statement.
type UseStatement struct {
	NS string // Namespace
	DB string // Database
	CK string // Cipherkey
}

// --------------------------------------------------
// Select
// --------------------------------------------------

// SelectStatement represents a SQL SELECT statement.
type SelectStatement struct {
	EX      bool     // Explain
	KV      string   // Bucket
	NS      string   // Namespace
	DB      string   // Database
	Expr    []*Field // Which fields
	What    []Expr   // What to select
	Cond    []Expr   // Select conditions
	Group   []*Group // Group by
	Order   []*Order // Order by
	Limit   Expr     // Limit by
	Start   Expr     // Start at
	Version Expr     // Version
}

// --------------------------------------------------
// Items
// --------------------------------------------------

// CreateStatement represents a SQL CREATE statement.
//
// CREATE person SET column = 'value' RETURN ID
type CreateStatement struct {
	EX   bool   // Explain
	KV   string // Bucket
	NS   string // Namespace
	DB   string // Database
	What []Expr // What to create
	Data []Expr // Create data
	Echo Token  // What to return
}

// UpdateStatement represents a SQL UPDATE statement.
//
// UPDATE person SET column = 'value' WHERE age < 18 RETURN ID
type UpdateStatement struct {
	EX   bool   // Explain
	KV   string // Bucket
	NS   string // Namespace
	DB   string // Database
	What []Expr // What to update
	Data []Expr // Update data
	Cond []Expr // Update conditions
	Echo Token  // What to return
}

// ModifyStatement represents a SQL UPDATE statement.
//
// MODIFY @person:123 WITH {} RETURN ID
type ModifyStatement struct {
	EX   bool   // Explain
	KV   string // Bucket
	NS   string // Namespace
	DB   string // Database
	What []Expr // What to modify
	Diff Expr   // Diff object
	Echo Token  // What to return
}

// DeleteStatement represents a SQL DELETE statement.
//
// DELETE FROM person WHERE age < 18 RETURN ID
type DeleteStatement struct {
	EX   bool   // Explain
	KV   string // Bucket
	NS   string // Namespace
	DB   string // Database
	What []Expr // What to delete
	Cond []Expr // Delete conditions
	Echo Token  // What to return
}

// RelateStatement represents a SQL RELATE statement.
//
// RELATE friend FROM @person:123 TO @person:456 SET column = 'value' RETURN ID
type RelateStatement struct {
	EX   bool   // Explain
	KV   string // Bucket
	NS   string // Namespace
	DB   string // Database
	Type []Expr
	From []Expr
	To   []Expr
	Data []Expr
	Echo Token // What to return
}

// RecordStatement represents a SQL CREATE EVENT statement.
//
// RECORD login ON @person:123 AT 2016-01-29T22:42:56.478Z SET column = true
type RecordStatement struct {
	EX   bool   // Explain
	KV   string // Bucket
	NS   string // Namespace
	DB   string // Database
	Type []Expr
	On   []Expr
	At   Expr
	Data []Expr
	Echo Token // What to return
}

// --------------------------------------------------
// Table
// --------------------------------------------------

// DefineTableStatement represents an SQL DEFINE TABLE statement.
//
// DEFINE TABLE person
type DefineTableStatement struct {
	EX   bool   // Explain
	KV   string // Bucket
	NS   string // Namespace
	DB   string // Database
	What []Expr // Table names
}

// RemoveTableStatement represents an SQL REMOVE TABLE statement.
//
// REMOVE TABLE person
type RemoveTableStatement struct {
	EX   bool   // Explain
	KV   string // Bucket
	NS   string // Namespace
	DB   string // Database
	What []Expr // Table names
}

// --------------------------------------------------
// Field
// --------------------------------------------------

// DefineFieldStatement represents an SQL DEFINE INDEX statement.
//
// DEFINE FIELD name ON person TYPE string CODE {}
// DEFINE FIELD name ON person TYPE [0,1,2,3,4,5] DEFAULT 0
// DEFINE FIELD name ON person TYPE [0...100]number MIN 0 MAX 3 DEFAULT 0
type DefineFieldStatement struct {
	EX        bool   // Explain
	KV        string // Bucket
	NS        string // Namespace
	DB        string // Database
	Name      Expr   // Field name
	What      []Expr // Table names
	Type      Expr   // Field type
	Code      Expr   // Field code
	Min       *NumberLiteral
	Max       *NumberLiteral
	Default   Expr
	Notnull   bool
	Readonly  bool
	Mandatory bool
}

// RemoveFieldStatement represents an SQL REMOVE INDEX statement.
//
// REMOVE FIELD name ON person
type RemoveFieldStatement struct {
	EX   bool   // Explain
	KV   string // Bucket
	NS   string // Namespace
	DB   string // Database
	Name Expr   // Field name
	What []Expr // Table names
}

// --------------------------------------------------
// Index
// --------------------------------------------------

// DefineIndexStatement represents an SQL DEFINE INDEX statement.
//
// DEFINE INDEX name ON person COLUMNS (account, age) UNIQUE
type DefineIndexStatement struct {
	EX   bool     // Explain
	KV   string   // Bucket
	NS   string   // Namespace
	DB   string   // Database
	Name Expr     // Index name
	What []Expr   // Table names
	Code Expr     // Index code
	Cols []*Field // Index cols
	Uniq bool     // Unique index
}

// RemoveIndexStatement represents an SQL REMOVE INDEX statement.
//
// REMOVE INDEX name ON person
type RemoveIndexStatement struct {
	EX   bool   // Explain
	KV   string // Bucket
	NS   string // Namespace
	DB   string // Database
	Name Expr   // Index name
	What []Expr // Table names
}

// ResyncIndexStatement represents an SQL RESYNC INDEX statement.
//
// RESYNC INDEX name ON person
type ResyncIndexStatement struct {
	EX   bool   // Explain
	KV   string // Bucket
	NS   string // Namespace
	DB   string // Database
	Name Expr   // Index name
	What []Expr // Table names
}

// --------------------------------------------------
// Literals
// --------------------------------------------------

// Expr represents a sql expression
type Expr interface{}

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

// Wildcard represents a wildcard expression.
type Wildcard struct{}

// JSONLiteral represents a json object.
type JSONLiteral struct {
	Val interface{}
}

// ArrayLiteral represents a json array.
type ArrayLiteral struct {
	Val []interface{}
}

// IdentLiteral represents a variable.
type IdentLiteral struct {
	Val string `json:"Ident"`
}

// BytesLiteral represents a null expression.
type BytesLiteral struct {
	Val []byte `json:"Bytes"`
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

// CodeExpression represents js/lua CODE
type CodeExpression struct {
	CODE *StringLiteral
}

// DiffExpression represents a JSON DIFF PATCH
type DiffExpression struct {
	JSON *JSONLiteral
}

// MergeExpression represents JSON to MERGE
type MergeExpression struct {
	JSON *JSONLiteral
}

// ContentExpression represents JSON to REPLACE
type ContentExpression struct {
	JSON *JSONLiteral
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
	Table string      `json:"Table"`
	Thing string      `json:"Thing"`
	ID    interface{} `json:"ID"`
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
