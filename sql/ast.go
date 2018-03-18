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
// If
// --------------------------------------------------

// IfStatement represents an if else clause.
type IfStatement struct {
	RW   bool  `cork:"-" codec:"-"`
	Cond Exprs `cork:"cond" codec:"cond"`
	Then Exprs `cork:"then" codec:"then"`
	Else Expr  `cork:"else" codec:"else"`
}

// --------------------------------------------------
// LET
// --------------------------------------------------

// LetStatement represents a SQL LET statement.
type LetStatement struct {
	RW   bool   `cork:"-" codec:"-"`
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Name *Ident `cork:"-" codec:"-"`
	What Expr   `cork:"-" codec:"-"`
}

// ReturnStatement represents a SQL RETURN statement.
type ReturnStatement struct {
	RW   bool   `cork:"-" codec:"-"`
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	What Exprs  `cork:"-" codec:"-"`
}

// --------------------------------------------------
// Normal
// --------------------------------------------------

// LiveStatement represents a SQL LIVE statement.
type LiveStatement struct {
	ID   string `cork:"ID" codec:"ID"`
	FB   string `cork:"FB" codec:"FB"`
	KV   string `cork:"KV" codec:"KV"`
	NS   string `cork:"NS" codec:"NS"`
	DB   string `cork:"DB" codec:"DB"`
	Diff bool   `cork:"diff" codec:"diff"`
	Expr Fields `cork:"expr" codec:"expr"`
	What Exprs  `cork:"what" codec:"what"`
	Cond Expr   `cork:"cond" codec:"cond"`
}

// KillStatement represents a SQL KILL statement.
type KillStatement struct {
	FB   string `cork:"FB" codec:"FB"`
	KV   string `cork:"KV" codec:"KV"`
	NS   string `cork:"NS" codec:"NS"`
	DB   string `cork:"DB" codec:"DB"`
	What Exprs  `cork:"what" codec:"what"`
}

// SelectStatement represents a SQL SELECT statement.
type SelectStatement struct {
	RW       bool          `cork:"-" codec:"-"`
	KV       string        `cork:"KV" codec:"KV"`
	NS       string        `cork:"NS" codec:"NS"`
	DB       string        `cork:"DB" codec:"DB"`
	Expr     Fields        `cork:"expr" codec:"expr"`
	What     Exprs         `cork:"what" codec:"what"`
	Cond     Expr          `cork:"cond" codec:"cond"`
	Group    Groups        `cork:"group" codec:"group"`
	Order    Orders        `cork:"order" codec:"order"`
	Limit    Expr          `cork:"limit" codec:"limit"`
	Start    Expr          `cork:"start" codec:"start"`
	Version  Expr          `cork:"version" codec:"version"`
	Timeout  time.Duration `cork:"timeout" codec:"timeout"`
	Parallel int           `cork:"parallel" codec:"parallel"`
}

// CreateStatement represents a SQL CREATE statement.
type CreateStatement struct {
	KV       string        `cork:"KV" codec:"KV"`
	NS       string        `cork:"NS" codec:"NS"`
	DB       string        `cork:"DB" codec:"DB"`
	What     Exprs         `cork:"what" codec:"what"`
	Data     Expr          `cork:"data" codec:"data"`
	Echo     Token         `cork:"echo" codec:"echo"`
	Timeout  time.Duration `cork:"timeout" codec:"timeout"`
	Parallel int           `cork:"parallel" codec:"parallel"`
}

// UpdateStatement represents a SQL UPDATE statement.
type UpdateStatement struct {
	KV       string        `cork:"KV" codec:"KV"`
	NS       string        `cork:"NS" codec:"NS"`
	DB       string        `cork:"DB" codec:"DB"`
	What     Exprs         `cork:"what" codec:"what"`
	Data     Expr          `cork:"data" codec:"data"`
	Cond     Expr          `cork:"cond" codec:"cond"`
	Echo     Token         `cork:"echo" codec:"echo"`
	Timeout  time.Duration `cork:"timeout" codec:"timeout"`
	Parallel int           `cork:"parallel" codec:"parallel"`
}

// DeleteStatement represents a SQL DELETE statement.
type DeleteStatement struct {
	KV       string        `cork:"KV" codec:"KV"`
	NS       string        `cork:"NS" codec:"NS"`
	DB       string        `cork:"DB" codec:"DB"`
	Hard     bool          `cork:"hard" codec:"hard"`
	What     Exprs         `cork:"what" codec:"what"`
	Cond     Expr          `cork:"cond" codec:"cond"`
	Echo     Token         `cork:"echo" codec:"echo"`
	Timeout  time.Duration `cork:"timeout" codec:"timeout"`
	Parallel int           `cork:"parallel" codec:"parallel"`
}

// RelateStatement represents a SQL RELATE statement.
type RelateStatement struct {
	KV       string        `cork:"KV" codec:"KV"`
	NS       string        `cork:"NS" codec:"NS"`
	DB       string        `cork:"DB" codec:"DB"`
	Type     Expr          `cork:"type" codec:"type"`
	From     Exprs         `cork:"from" codec:"from"`
	With     Exprs         `cork:"with" codec:"with"`
	Data     Expr          `cork:"data" codec:"data"`
	Uniq     bool          `cork:"uniq" codec:"uniq"`
	Echo     Token         `cork:"echo" codec:"echo"`
	Timeout  time.Duration `cork:"timeout" codec:"timeout"`
	Parallel int           `cork:"parallel" codec:"parallel"`
}

// InsertStatement represents a SQL INSERT statement.
type InsertStatement struct {
	KV       string        `cork:"KV" codec:"KV"`
	NS       string        `cork:"NS" codec:"NS"`
	DB       string        `cork:"DB" codec:"DB"`
	Data     Expr          `cork:"data" codec:"data"`
	Into     *Table        `cork:"into" codec:"into"`
	Echo     Token         `cork:"echo" codec:"echo"`
	Timeout  time.Duration `cork:"timeout" codec:"timeout"`
	Parallel int           `cork:"parallel" codec:"parallel"`
}

// UpsertStatement represents a SQL UPSERT statement.
type UpsertStatement struct {
	KV       string        `cork:"KV" codec:"KV"`
	NS       string        `cork:"NS" codec:"NS"`
	DB       string        `cork:"DB" codec:"DB"`
	Data     Expr          `cork:"data" codec:"data"`
	Into     *Table        `cork:"into" codec:"into"`
	Echo     Token         `cork:"echo" codec:"echo"`
	Timeout  time.Duration `cork:"timeout" codec:"timeout"`
	Parallel int           `cork:"parallel" codec:"parallel"`
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
	KV      string        `cork:"-" codec:"-"`
	NS      string        `cork:"-" codec:"-"`
	DB      string        `cork:"-" codec:"-"`
	Name    *Ident        `cork:"name" codec:"name"`
	Time    time.Duration `cork:"time" codec:"time"`
	Code    []byte        `cork:"code" codec:"code"`
	Signup  Expr          `cork:"signup" codec:"signup"`
	Signin  Expr          `cork:"signin" codec:"signin"`
	Connect Expr          `cork:"connect" codec:"connect"`
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
	KV    string `cork:"-" codec:"-"`
	NS    string `cork:"-" codec:"-"`
	DB    string `cork:"-" codec:"-"`
	Name  *Ident `cork:"name" codec:"name"`
	What  Tables `cork:"-" codec:"-"`
	Full  bool   `cork:"full" codec:"full"`
	Drop  bool   `cork:"drop" codec:"drop"`
	Lock  bool   `cork:"lock" codec:"lock"`
	Expr  Fields `cork:"expr" codec:"expr"`
	From  Tables `cork:"from" codec:"from"`
	Cond  Expr   `cork:"cond" codec:"cond"`
	Group Groups `cork:"group" codec:"group"`
	Perms Expr   `cork:"perms" codec:"perms"`
}

// RemoveTableStatement represents an SQL REMOVE TABLE statement.
type RemoveTableStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	What Tables `cork:"-" codec:"-"`
}

// --------------------------------------------------
// Event
// --------------------------------------------------

// DefineEventStatement represents an SQL DEFINE EVENT statement.
type DefineEventStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Name *Ident `cork:"name" codec:"name"`
	What Tables `cork:"-" codec:"-"`
	When Expr   `cork:"when" codec:"when"`
	Then Expr   `cork:"then" codec:"then"`
}

// RemoveEventStatement represents an SQL REMOVE EVENT statement.
type RemoveEventStatement struct {
	KV   string `cork:"-" codec:"-"`
	NS   string `cork:"-" codec:"-"`
	DB   string `cork:"-" codec:"-"`
	Name *Ident `cork:"-" codec:"-"`
	What Tables `cork:"-" codec:"-"`
}

// --------------------------------------------------
// Field
// --------------------------------------------------

// DefineFieldStatement represents an SQL DEFINE FIELD statement.
type DefineFieldStatement struct {
	KV       string  `cork:"-" codec:"-"`
	NS       string  `cork:"-" codec:"-"`
	DB       string  `cork:"-" codec:"-"`
	Name     *Ident  `cork:"name" codec:"name"`
	What     Tables  `cork:"-" codec:"-"`
	Perms    Expr    `cork:"perms" codec:"perms"`
	Type     string  `cork:"type" codec:"type"`
	Kind     string  `cork:"kind" codec:"kind"`
	Value    Expr    `cork:"value" codec:"value"`
	Assert   Expr    `cork:"assert" codec:"assert"`
	Priority float64 `cork:"priority" codec:"priority"`
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
// Model
// --------------------------------------------------

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
	if t, _, v := s.scanThing(); t == THING {
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
	PS Points
}

func NewPolygon(PS ...*Point) *Polygon {
	return &Polygon{PS: PS}
}
