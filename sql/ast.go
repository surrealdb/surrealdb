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
// Normal
// --------------------------------------------------

// SelectStatement represents a SQL SELECT statement.
type SelectStatement struct {
	KV      string   `cork:"-" codec:"-"`
	NS      string   `cork:"-" codec:"-"`
	DB      string   `cork:"-" codec:"-"`
	Expr    []*Field `cork:"expr" codec:"expr"`
	What    []Expr   `cork:"what" codec:"what"`
	Cond    []Expr   `cork:"cond" codec:"cond"`
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
	What []Expr `cork:"what" codec:"what"`
	Data []Expr `cork:"data" codec:"data"`
	Cond []Expr `cork:"cond" codec:"cond"`
	Echo Token  `cork:"echo" codec:"echo"`
}

// ModifyStatement represents a SQL MODIFY statement.
type ModifyStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	What []Expr `cork:"what" codec:"what"`
	Diff []Expr `cork:"diff" codec:"diff"`
	Cond []Expr `cork:"cond" codec:"cond"`
	Echo Token  `cork:"echo" codec:"echo"`
}

// DeleteStatement represents a SQL DELETE statement.
type DeleteStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Hard bool   `cork:"hard" codec:"hard"`
	What []Expr `cork:"what" codec:"what"`
	Cond []Expr `cork:"cond" codec:"cond"`
	Echo Token  `cork:"echo" codec:"echo"`
}

// RelateStatement represents a SQL RELATE statement.
type RelateStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Type []Expr `cork:"type" codec:"type"`
	From []Expr `cork:"from" codec:"from"`
	To   []Expr `cork:"to" codec:"to"`
	Data []Expr `cork:"data" codec:"data"`
	Echo Token  `cork:"echo" codec:"echo"`
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
	Code string   `cork:"code" codec:"code"`
	Cond []Expr   `cork:"cond" codec:"cond"`
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
	Uniq bool     `cork:"unique" codec:"unique"`
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
	Cond  []Expr   `cork:"cond" codec:"cond"`
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
		if cnv, err := strconv.ParseInt(str, 10, 64); err == nil {
			return &Thing{TB: TB, ID: cnv}
		} else if cnv, err := strconv.ParseFloat(str, 64); err == nil {
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
