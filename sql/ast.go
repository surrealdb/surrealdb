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
// Use
// --------------------------------------------------

// UseStatement represents a SQL USE statement.
type UseStatement struct {
	NS string // Namespace
	DB string // Database
}

// --------------------------------------------------
// Trans
// --------------------------------------------------

type BeginStatement struct{}

type CancelStatement struct{}

type CommitStatement struct{}

// --------------------------------------------------
// Normal
// --------------------------------------------------

// ActionStatement represents a SQL ACTION statement.
type ActionStatement struct {
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
	Echo    Token    // What to return
}

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
	Diff []Expr // Diff object
	Cond []Expr // Update conditions
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
	Hard bool   // Expunge
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
// Rules
// --------------------------------------------------

// DefineRulesStatement represents an SQL DEFINE RULES statement.
//
// DEFINE RULES person
type DefineRulesStatement struct {
	EX   bool     `json:"-" msgpack:"-"`       // Explain
	KV   string   `json:"-" msgpack:"-"`       // Bucket
	NS   string   `json:"-" msgpack:"-"`       // Namespace
	DB   string   `json:"-" msgpack:"-"`       // Database
	What []string `json:"-" msgpack:"-"`       // Table names
	When []string `json:"-" msgpack:"-"`       // Action names
	Rule string   `json:"rule" msgpack:"rule"` // Rule behaviour
	Code string   `json:"code" msgpack:"code"` // Rule custom code
}

// RemoveRulesStatement represents an SQL REMOVE RULES statement.
//
// REMOVE RULES person
type RemoveRulesStatement struct {
	EX   bool     `json:"-" msgpack:"-"` // Explain
	KV   string   `json:"-" msgpack:"-"` // Bucket
	NS   string   `json:"-" msgpack:"-"` // Namespace
	DB   string   `json:"-" msgpack:"-"` // Database
	What []string `json:"-" msgpack:"-"` // Table names
	When []string `json:"-" msgpack:"-"` // Action names
}

// --------------------------------------------------
// Table
// --------------------------------------------------

// DefineTableStatement represents an SQL DEFINE TABLE statement.
//
// DEFINE TABLE person
type DefineTableStatement struct {
	EX   bool     `json:"-" msgpack:"-"` // Explain
	KV   string   `json:"-" msgpack:"-"` // Bucket
	NS   string   `json:"-" msgpack:"-"` // Namespace
	DB   string   `json:"-" msgpack:"-"` // Database
	What []string `json:"-" msgpack:"-"` // Table names
}

// RemoveTableStatement represents an SQL REMOVE TABLE statement.
//
// REMOVE TABLE person
type RemoveTableStatement struct {
	EX   bool     `json:"-" msgpack:"-"` // Explain
	KV   string   `json:"-" msgpack:"-"` // Bucket
	NS   string   `json:"-" msgpack:"-"` // Namespace
	DB   string   `json:"-" msgpack:"-"` // Database
	What []string `json:"-" msgpack:"-"` // Table names
}

// --------------------------------------------------
// Field
// --------------------------------------------------

// DefineFieldStatement represents an SQL DEFINE INDEX statement.
//
// DEFINE FIELD name ON person TYPE string CODE {}
// DEFINE FIELD name ON person TYPE number MIN 0 MAX 5 DEFAULT 0
// DEFINE FIELD name ON person TYPE custom ENUM [0,1,2,3,4,5] DEFAULT 0
type DefineFieldStatement struct {
	EX        bool          `json:"-" msgpack:"-"`                 // Explain
	KV        string        `json:"-" msgpack:"-"`                 // Bucket
	NS        string        `json:"-" msgpack:"-"`                 // Namespace
	DB        string        `json:"-" msgpack:"-"`                 // Database
	Name      string        `json:"name" msgpack:"name"`           // Field name
	What      []string      `json:"-" msgpack:"-"`                 // Table names
	Type      string        `json:"type" msgpack:"type"`           // Field type
	Enum      []interface{} `json:"enum" msgpack:"enum"`           // Custom options
	Code      string        `json:"code" msgpack:"code"`           // Field code
	Min       float64       `json:"min" msgpack:"min"`             // Minimum value / length
	Max       float64       `json:"max" msgpack:"max"`             // Maximum value / length
	Match     string        `json:"match" msgpack:"match"`         // Regex value
	Default   interface{}   `json:"default" msgpack:"default"`     // Default value
	Notnull   bool          `json:"notnull" msgpack:"notnull"`     // Notnull - can not be NULL?
	Readonly  bool          `json:"readonly" msgpack:"readonly"`   // Readonly - can not be changed?
	Mandatory bool          `json:"mandatory" msgpack:"mandatory"` // Mandatory - can not be VOID?
	Validate  bool          `json:"validate" msgpack:"validate"`   // Validate - can not be INCORRECT?
}

// RemoveFieldStatement represents an SQL REMOVE INDEX statement.
//
// REMOVE FIELD name ON person
type RemoveFieldStatement struct {
	EX   bool     `json:"-" msgpack:"-"` // Explain
	KV   string   `json:"-" msgpack:"-"` // Bucket
	NS   string   `json:"-" msgpack:"-"` // Namespace
	DB   string   `json:"-" msgpack:"-"` // Database
	Name string   `json:"-" msgpack:"-"` // Field name
	What []string `json:"-" msgpack:"-"` // Table names
}

// --------------------------------------------------
// Index
// --------------------------------------------------

// DefineIndexStatement represents an SQL DEFINE INDEX statement.
//
// DEFINE INDEX name ON person COLUMNS (account, age) UNIQUE
type DefineIndexStatement struct {
	EX   bool     `json:"-" msgpack:"-"`           // Explain
	KV   string   `json:"-" msgpack:"-"`           // Bucket
	NS   string   `json:"-" msgpack:"-"`           // Namespace
	DB   string   `json:"-" msgpack:"-"`           // Database
	Name string   `json:"name" msgpack:"name"`     // Index name
	What []string `json:"-" msgpack:"-"`           // Table names
	Cols []string `json:"cols" msgpack:"cols"`     // Index cols
	Uniq bool     `json:"unique" msgpack:"unique"` // Unique index
	CI   bool
	CS   bool
}

// RemoveIndexStatement represents an SQL REMOVE INDEX statement.
//
// REMOVE INDEX name ON person
type RemoveIndexStatement struct {
	EX   bool     `json:"-" msgpack:"-"` // Explain
	KV   string   `json:"-" msgpack:"-"` // Bucket
	NS   string   `json:"-" msgpack:"-"` // Namespace
	DB   string   `json:"-" msgpack:"-"` // Database
	Name string   `json:"-" msgpack:"-"` // Index name
	What []string `json:"-" msgpack:"-"` // Table names
}

// ResyncIndexStatement represents an SQL RESYNC INDEX statement.
//
// RESYNC INDEX name ON person
type ResyncIndexStatement struct {
	EX   bool     `json:"-" msgpack:"-"` // Explain
	KV   string   `json:"-" msgpack:"-"` // Bucket
	NS   string   `json:"-" msgpack:"-"` // Namespace
	DB   string   `json:"-" msgpack:"-"` // Database
	What []string `json:"-" msgpack:"-"` // Table names
}

// --------------------------------------------------
// Literals
// --------------------------------------------------

// Expr represents a sql expression
type Expr interface{}

// All represents a wildcard expression.
type All struct{}

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

// ClosedExpression represents a parenthesized expression.
type ClosedExpression struct {
	Expr Expr
}

// BinaryExpression represents a binary expression tree,
type BinaryExpression struct {
	LHS Expr
	Op  Token
	RHS Expr
}

// DiffExpression represents a JSON DIFF PATCH
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

// Table comment
type Table struct {
	TB string
}

// Thing comment
type Thing struct {
	TB string
	ID interface{}
}

// Field comment
type Field struct {
	Expr  Expr
	Alias string
}

// Group represents an sql GROUP BY clause
type Group struct {
	Expr Expr
}

// Order represents an sql ORDER BY clause
type Order struct {
	Expr Expr
	Dir  Expr
}
