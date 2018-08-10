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
	"strings"
	"time"

	"golang.org/x/text/language"
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
	Duration() time.Duration
}

type WriteableStatement interface {
	Writeable() bool
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
	NS string
	DB string
}

// --------------------------------------------------
// Info
// --------------------------------------------------

// InfoStatement represents an SQL INFO statement.
type InfoStatement struct {
	KV   string
	NS   string
	DB   string
	Kind Token
	What *Table
}

// --------------------------------------------------
// If
// --------------------------------------------------

// IfStatement represents an if else clause.
type IfStatement struct {
	RW   bool
	Cond Exprs
	Then Exprs
	Else Expr
}

// --------------------------------------------------
// Run
// --------------------------------------------------

// RunStatement represents a run clause.
type RunStatement struct {
	RW   bool
	Expr Expr
}

// --------------------------------------------------
// LET
// --------------------------------------------------

// LetStatement represents a SQL LET statement.
type LetStatement struct {
	RW   bool
	KV   string
	NS   string
	DB   string
	Name *Ident
	What Expr
}

// ReturnStatement represents a SQL RETURN statement.
type ReturnStatement struct {
	RW   bool
	KV   string
	NS   string
	DB   string
	What Exprs
}

// --------------------------------------------------
// Normal
// --------------------------------------------------

// LiveStatement represents a SQL LIVE statement.
type LiveStatement struct {
	ID    string
	FB    string
	KV    string
	NS    string
	DB    string
	Diff  bool
	Expr  Fields
	What  Exprs
	Cond  Expr
	Fetch Fetchs
}

// KillStatement represents a SQL KILL statement.
type KillStatement struct {
	FB   string
	KV   string
	NS   string
	DB   string
	What Exprs
}

// SelectStatement represents a SQL SELECT statement.
type SelectStatement struct {
	RW       bool
	KV       string
	NS       string
	DB       string
	Expr     Fields
	What     Exprs
	Cond     Expr
	Group    Groups
	Order    Orders
	Limit    Expr
	Start    Expr
	Fetch    Fetchs
	Version  Expr
	Timeout  time.Duration
	Parallel int
}

// CreateStatement represents a SQL CREATE statement.
type CreateStatement struct {
	KV       string
	NS       string
	DB       string
	What     Exprs
	Data     Expr
	Echo     Token
	Timeout  time.Duration
	Parallel int
}

// UpdateStatement represents a SQL UPDATE statement.
type UpdateStatement struct {
	KV       string
	NS       string
	DB       string
	What     Exprs
	Data     Expr
	Cond     Expr
	Echo     Token
	Timeout  time.Duration
	Parallel int
}

// DeleteStatement represents a SQL DELETE statement.
type DeleteStatement struct {
	KV       string
	NS       string
	DB       string
	Hard     bool
	What     Exprs
	Cond     Expr
	Echo     Token
	Timeout  time.Duration
	Parallel int
}

// RelateStatement represents a SQL RELATE statement.
type RelateStatement struct {
	KV       string
	NS       string
	DB       string
	Type     Expr
	From     Exprs
	With     Exprs
	Data     Expr
	Uniq     bool
	Echo     Token
	Timeout  time.Duration
	Parallel int
}

// InsertStatement represents a SQL INSERT statement.
type InsertStatement struct {
	KV       string
	NS       string
	DB       string
	Data     Expr
	Into     *Table
	Echo     Token
	Timeout  time.Duration
	Parallel int
}

// UpsertStatement represents a SQL UPSERT statement.
type UpsertStatement struct {
	KV       string
	NS       string
	DB       string
	Data     Expr
	Into     *Table
	Echo     Token
	Timeout  time.Duration
	Parallel int
}

// --------------------------------------------------
// Namespace
// --------------------------------------------------

type DefineNamespaceStatement struct {
	KV   string
	NS   string
	DB   string
	Name *Ident
}

type RemoveNamespaceStatement struct {
	KV   string
	NS   string
	DB   string
	Name *Ident
}

// --------------------------------------------------
// Database
// --------------------------------------------------

type DefineDatabaseStatement struct {
	KV   string
	NS   string
	DB   string
	Name *Ident
}

type RemoveDatabaseStatement struct {
	KV   string
	NS   string
	DB   string
	Name *Ident
}

// --------------------------------------------------
// Login
// --------------------------------------------------

// DefineLoginStatement represents an SQL DEFINE LOGIN statement.
type DefineLoginStatement struct {
	KV   string
	NS   string
	DB   string
	Kind Token
	User *Ident
	Pass []byte
	Code []byte
}

// RemoveLoginStatement represents an SQL REMOVE LOGIN statement.
type RemoveLoginStatement struct {
	KV   string
	NS   string
	DB   string
	Kind Token
	User *Ident
}

// --------------------------------------------------
// Token
// --------------------------------------------------

// DefineTokenStatement represents an SQL DEFINE TOKEN statement.
type DefineTokenStatement struct {
	KV   string
	NS   string
	DB   string
	Kind Token
	Name *Ident
	Type string
	Code []byte
}

// RemoveTokenStatement represents an SQL REMOVE TOKEN statement.
type RemoveTokenStatement struct {
	KV   string
	NS   string
	DB   string
	Kind Token
	Name *Ident
}

// --------------------------------------------------
// Scope
// --------------------------------------------------

// DefineScopeStatement represents an SQL DEFINE SCOPE statement.
type DefineScopeStatement struct {
	KV       string
	NS       string
	DB       string
	Name     *Ident
	Time     time.Duration
	Code     []byte
	Signup   Expr
	Signin   Expr
	Connect  Expr
	OnSignup Expr
	OnSignin Expr
}

// RemoveScopeStatement represents an SQL REMOVE SCOPE statement.
type RemoveScopeStatement struct {
	KV   string
	NS   string
	DB   string
	Name *Ident
}

// --------------------------------------------------
// Table
// --------------------------------------------------

// DefineTableStatement represents an SQL DEFINE TABLE statement.
type DefineTableStatement struct {
	KV    string
	NS    string
	DB    string
	Name  *Ident
	What  Tables
	Full  bool
	Vers  bool
	Drop  bool
	Lock  bool
	Expr  Fields
	From  Tables
	Cond  Expr
	Group Groups
	Perms Expr
}

// RemoveTableStatement represents an SQL REMOVE TABLE statement.
type RemoveTableStatement struct {
	KV   string
	NS   string
	DB   string
	What Tables
}

// --------------------------------------------------
// Event
// --------------------------------------------------

// DefineEventStatement represents an SQL DEFINE EVENT statement.
type DefineEventStatement struct {
	KV   string
	NS   string
	DB   string
	Name *Ident
	What Tables
	When Expr
	Then Expr
}

// RemoveEventStatement represents an SQL REMOVE EVENT statement.
type RemoveEventStatement struct {
	KV   string
	NS   string
	DB   string
	Name *Ident
	What Tables
}

// --------------------------------------------------
// Field
// --------------------------------------------------

// DefineFieldStatement represents an SQL DEFINE FIELD statement.
type DefineFieldStatement struct {
	KV       string
	NS       string
	DB       string
	Name     *Ident
	What     Tables
	Perms    Expr
	Type     string
	Kind     string
	Value    Expr
	Assert   Expr
	Priority float64
}

// RemoveFieldStatement represents an SQL REMOVE FIELD statement.
type RemoveFieldStatement struct {
	KV   string
	NS   string
	DB   string
	Name *Ident
	What Tables
}

// --------------------------------------------------
// Index
// --------------------------------------------------

// DefineIndexStatement represents an SQL DEFINE INDEX statement.
type DefineIndexStatement struct {
	KV   string
	NS   string
	DB   string
	Name *Ident
	What Tables
	Cols Idents
	Uniq bool
}

// RemoveIndexStatement represents an SQL REMOVE INDEX statement.
type RemoveIndexStatement struct {
	KV   string
	NS   string
	DB   string
	Name *Ident
	What Tables
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

// Null represents an expression which is null.
type Null struct{}

// Void represents an expression which is not set.
type Void struct{}

// Empty represents an expression which is null or "".
type Empty struct{}

// Field represents a SELECT AS clause.
type Field struct {
	Expr  Expr
	Field string
	Alias string
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
	Dir  bool
	Tag  language.Tag
}

// Orders represents multiple ORDER BY clauses.
type Orders []*Order

// Fetch represents a FETCH AS clause.
type Fetch struct {
	Expr Expr
}

// Fetchs represents multiple FETCH AS clauses.
type Fetchs []*Fetch

// --------------------------------------------------
// Expressions
// --------------------------------------------------

// SubExpression represents a subquery.
type SubExpression struct {
	Expr Expr
}

// MultExpression represents multiple queries.
type MultExpression struct {
	Expr []Expr
}

// IfelExpression represents an if else clause.
type IfelExpression struct {
	Cond Exprs
	Then Exprs
	Else Expr
}

// FuncExpression represents a function call.
type FuncExpression struct {
	Name string
	Args Exprs
	Aggr bool
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
	VA string
}

func NewParam(VA string) *Param {
	return &Param{VA}
}

// --------------------------------------------------
// Ident
// --------------------------------------------------

// Idents represents multiple Ident clauses.
type Idents []*Ident

// Ident comment
type Ident struct {
	VA string
}

func NewIdent(VA string) *Ident {
	return &Ident{VA}
}

// --------------------------------------------------
// Value
// --------------------------------------------------

// Values represents multiple Value clauses.
type Values []*Value

// Value comment
type Value struct {
	VA string
}

func NewValue(VA string) *Value {
	return &Value{VA}
}

// --------------------------------------------------
// Regex
// --------------------------------------------------

// Regexs represents multiple Regex clauses.
type Regexs []*Regex

// Regex comment
type Regex struct {
	VA string
}

func NewRegex(VA string) *Regex {
	return &Regex{VA}
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
// Batch
// --------------------------------------------------

// Batchs represents multiple Batch clauses.
type Batchs []*Batch

// Batch comment
type Batch struct {
	TB string
	BA []*Thing
}

func NewBatch(TB string, BA []interface{}) *Batch {
	var b = &Batch{TB: TB}
	for _, ID := range BA {
		b.BA = append(b.BA, NewThing(TB, ID))
	}
	return b
}

// --------------------------------------------------
// Model
// --------------------------------------------------

// Models represents multiple Model clauses.
type Models []*Model

// Model comment
type Model struct {
	TB  string
	MIN float64
	INC float64
	MAX float64
}

func NewModel(TB string, MIN, INC, MAX float64) *Model {
	return &Model{TB: TB, MIN: MIN, INC: INC, MAX: MAX}
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

func ParseThing(val string) *Thing {
	r := strings.NewReader(val)
	s := newScanner(r)
	if t, _, v := s.scanIdiom(); t == THING {
		return v.(*Thing)
	}
	return nil
}

func NewThing(TB string, ID interface{}) *Thing {
	switch val := ID.(type) {
	default:
		return &Thing{TB: TB, ID: ID}
	case int:
		return &Thing{TB: TB, ID: float64(val)}
	case int64:
		return &Thing{TB: TB, ID: float64(val)}
	case string:
		val = strings.Replace(val, TB+":", "", -1)
		if cnv, err := strconv.ParseFloat(val, 64); err == nil {
			return &Thing{TB: TB, ID: cnv}
		} else if cnv, err := strconv.ParseBool(val); err == nil {
			return &Thing{TB: TB, ID: cnv}
		} else if cnv, err := time.Parse(RFCDate, val); err == nil {
			return &Thing{TB: TB, ID: cnv.UTC()}
		} else if cnv, err := time.Parse(RFCTime, val); err == nil {
			return &Thing{TB: TB, ID: cnv.UTC()}
		}
		return &Thing{TB: TB, ID: val}
	}
}

// --------------------------------------------------
// Point
// --------------------------------------------------

// Points represents multiple Point clauses.
type Points []*Point

// Point comment
type Point struct {
	LA float64
	LO float64
}

func NewPoint(LA, LO float64) *Point {
	return &Point{LA: LA, LO: LO}
}

// --------------------------------------------------
// Circle
// --------------------------------------------------

// Circles represents multiple Circle clauses.
type Circles []*Circle

// Circle comment
type Circle struct {
	CE *Point
	RA float64
}

func NewCircle(CE *Point, RA float64) *Circle {
	return &Circle{CE: CE, RA: RA}
}

// --------------------------------------------------
// Polygon
// --------------------------------------------------

// Polygons represents multiple Polygon clauses.
type Polygons []*Polygon

// Polygon comment
type Polygon struct {
	PS []*Point
}

func NewPolygon(PS ...*Point) *Polygon {
	return &Polygon{PS: PS}
}
