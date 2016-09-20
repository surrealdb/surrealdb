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
	NS string `codec:"-"`
	DB string `codec:"-"`
}

// --------------------------------------------------
// Info
// --------------------------------------------------

// InfoStatement represents an SQL INFO statement.
type InfoStatement struct {
	EX   bool   `codec:"-"`
	KV   string `codec:"-"`
	NS   string `codec:"-"`
	DB   string `codec:"-"`
	What string `codec:"-"`
}

// --------------------------------------------------
// Normal
// --------------------------------------------------

// SelectStatement represents a SQL SELECT statement.
type SelectStatement struct {
	EX      bool     `codec:"-"`
	KV      string   `codec:"-"`
	NS      string   `codec:"-"`
	DB      string   `codec:"-"`
	Expr    []*Field `codec:"expr"`
	What    []Expr   `codec:"what"`
	Cond    []Expr   `codec:"cond"`
	Group   []*Group `codec:"group"`
	Order   []*Order `codec:"order"`
	Limit   Expr     `codec:"limit"`
	Start   Expr     `codec:"start"`
	Version Expr     `codec:"version"`
	Echo    Token    `codec:"echo"`
}

// CreateStatement represents a SQL CREATE statement.
type CreateStatement struct {
	EX   bool   `codec:"-"`
	KV   string `codec:"-"`
	NS   string `codec:"-"`
	DB   string `codec:"-"`
	What []Expr `codec:"what"`
	Data []Expr `codec:"data"`
	Echo Token  `codec:"echo"`
}

// UpdateStatement represents a SQL UPDATE statement.
type UpdateStatement struct {
	EX   bool   `codec:"-"`
	KV   string `codec:"-"`
	NS   string `codec:"-"`
	DB   string `codec:"-"`
	What []Expr `codec:"what"`
	Data []Expr `codec:"data"`
	Cond []Expr `codec:"cond"`
	Echo Token  `codec:"echo"`
}

// ModifyStatement represents a SQL MODIFY statement.
type ModifyStatement struct {
	EX   bool   `codec:"-"`
	KV   string `codec:"-"`
	NS   string `codec:"-"`
	DB   string `codec:"-"`
	What []Expr `codec:"what"`
	Diff []Expr `codec:"diff"`
	Cond []Expr `codec:"cond"`
	Echo Token  `codec:"echo"`
}

// DeleteStatement represents a SQL DELETE statement.
type DeleteStatement struct {
	EX   bool   `codec:"-"`
	KV   string `codec:"-"`
	NS   string `codec:"-"`
	DB   string `codec:"-"`
	Hard bool   `codec:"hard"`
	What []Expr `codec:"what"`
	Cond []Expr `codec:"cond"`
	Echo Token  `codec:"echo"`
}

// RelateStatement represents a SQL RELATE statement.
type RelateStatement struct {
	EX   bool   `codec:"-"`
	KV   string `codec:"-"`
	NS   string `codec:"-"`
	DB   string `codec:"-"`
	Type []Expr `codec:"type"`
	From []Expr `codec:"from"`
	To   []Expr `codec:"to"`
	Data []Expr `codec:"data"`
	Echo Token  `codec:"echo"`
}

// RecordStatement represents a SQL RECORD statement.
type RecordStatement struct {
	EX   bool   `codec:"-"`
	KV   string `codec:"-"`
	NS   string `codec:"-"`
	DB   string `codec:"-"`
	Type []Expr `codec:"type"`
	When Expr   `codec:"when"`
	Data []Expr `codec:"data"`
	Echo Token  `codec:"echo"`
}

// --------------------------------------------------
// Table
// --------------------------------------------------

// DefineTableStatement represents an SQL DEFINE TABLE statement.
type DefineTableStatement struct {
	EX   bool     `codec:"-"`
	KV   string   `codec:"-"`
	NS   string   `codec:"-"`
	DB   string   `codec:"-"`
	What []string `codec:"-"`
}

// RemoveTableStatement represents an SQL REMOVE TABLE statement.
type RemoveTableStatement struct {
	EX   bool     `codec:"-"`
	KV   string   `codec:"-"`
	NS   string   `codec:"-"`
	DB   string   `codec:"-"`
	What []string `codec:"-"`
}

// --------------------------------------------------
// Rules
// --------------------------------------------------

// DefineRulesStatement represents an SQL DEFINE RULES statement.
type DefineRulesStatement struct {
	EX   bool     `codec:"-"`
	KV   string   `codec:"-"`
	NS   string   `codec:"-"`
	DB   string   `codec:"-"`
	What []string `codec:"-"`
	When []string `codec:"-"`
	Rule string   `codec:"rule"`
	Code string   `codec:"code"`
}

// RemoveRulesStatement represents an SQL REMOVE RULES statement.
type RemoveRulesStatement struct {
	EX   bool     `codec:"-"`
	KV   string   `codec:"-"`
	NS   string   `codec:"-"`
	DB   string   `codec:"-"`
	What []string `codec:"-"`
	When []string `codec:"-"`
}

// --------------------------------------------------
// Field
// --------------------------------------------------

// DefineFieldStatement represents an SQL DEFINE FIELD statement.
type DefineFieldStatement struct {
	EX        bool          `codec:"-"`
	KV        string        `codec:"-"`
	NS        string        `codec:"-"`
	DB        string        `codec:"-"`
	Name      string        `codec:"name"`
	What      []string      `codec:"-"`
	Type      string        `codec:"type"`
	Enum      []interface{} `codec:"enum"`
	Code      string        `codec:"code"`
	Min       float64       `codec:"min"`
	Max       float64       `codec:"max"`
	Match     string        `codec:"match"`
	Default   interface{}   `codec:"default"`
	Notnull   bool          `codec:"notnull"`
	Readonly  bool          `codec:"readonly"`
	Mandatory bool          `codec:"mandatory"`
	Validate  bool          `codec:"validate"`
}

// RemoveFieldStatement represents an SQL REMOVE FIELD statement.
type RemoveFieldStatement struct {
	EX   bool     `codec:"-"`
	KV   string   `codec:"-"`
	NS   string   `codec:"-"`
	DB   string   `codec:"-"`
	Name string   `codec:"-"`
	What []string `codec:"-"`
}

// --------------------------------------------------
// Index
// --------------------------------------------------

// DefineIndexStatement represents an SQL DEFINE INDEX statement.
type DefineIndexStatement struct {
	EX   bool     `codec:"-"`
	KV   string   `codec:"-"`
	NS   string   `codec:"-"`
	DB   string   `codec:"-"`
	Name string   `codec:"name"`
	What []string `codec:"-"`
	Cols []string `codec:"cols"`
	Uniq bool     `codec:"unique"`
}

// RemoveIndexStatement represents an SQL REMOVE INDEX statement.
type RemoveIndexStatement struct {
	EX   bool     `codec:"-"`
	KV   string   `codec:"-"`
	NS   string   `codec:"-"`
	DB   string   `codec:"-"`
	Name string   `codec:"-"`
	What []string `codec:"-"`
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

func (this Ident) String() string {
	return this.ID
}

func NewIdent(ID string) *Ident {
	return &Ident{ID}
}

// --------------------------------------------------
// Parts
// --------------------------------------------------

// Table comment
type Table struct {
	TB string
}

func (this Table) String() string {
	return this.TB
}

func NewTable(TB string) *Table {
	return &Table{TB}
}

// --------------------------------------------------
// Parts
// --------------------------------------------------

// Thing comment
type Thing struct {
	TB string
	ID interface{}
}

func (this Thing) String() string {
	return fmt.Sprintf("@%s:%v", this.TB, this.ID)
}

func NewThing(TB string, ID interface{}) *Thing {
	if str, ok := ID.(string); ok {
		if cnv, err := strconv.ParseFloat(str, 64); err == nil {
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
