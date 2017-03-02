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
	"encoding/json"
	"fmt"
	"strings"
)

func orNil(v interface{}) string {
	switch v.(type) {
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}

func stringIf(b bool, v interface{}) string {
	switch b {
	case false:
		return ""
	default:
		return fmt.Sprint(v)
	}
}

func padToken(t Token) string {
	switch t {
	case OR, AND:
		fallthrough
	case IN, IS, CONTAINS:
		fallthrough
	case CONTAINSALL, CONTAINSNONE, CONTAINSSOME:
		fallthrough
	case ALLCONTAINEDIN, NONECONTAINEDIN, SOMECONTAINEDIN:
		return fmt.Sprintf(" %v ", t)
	default:
		return fmt.Sprintf("%v", t)
	}
}

func toQuote(s string) bool {
	for _, c := range s {
		switch {
		case c >= 'a' && c <= 'z':
			continue
		case c >= 'A' && c <= 'Z':
			continue
		case c >= '0' && c <= '9':
			continue
		case c == '[', c == ']':
			continue
		case c == '-', c == '.':
			continue
		case c == '*':
			continue
		default:
			return true
		}
	}
	return false
}

func stringFromBool(v bool, y, n string) string {
	switch v {
	case false:
		return n
	default:
		return y
	}
}

func stringFromString(v string, y, n string) string {
	switch v {
	case "":
		return n
	default:
		return y
	}
}

func stringFromInt(v int64, y, n string) string {
	switch v {
	case 0:
		return n
	default:
		return y
	}
}

func stringFromFloat(v float64, y, n string) string {
	switch v {
	case 0:
		return n
	default:
		return y
	}
}

func stringFromArray(v Array, y, n string) string {
	switch len(v) {
	case 0:
		return n
	default:
		return y
	}
}

func stringFromSlice(v []interface{}, y, n string) string {
	switch len(v) {
	case 0:
		return n
	default:
		return y
	}
}

func stringFromIdent(v *Ident, y, n string) string {
	switch v {
	case nil:
		return n
	default:
		return y
	}
}

func stringFromInterface(v interface{}, y, n string) string {
	switch v {
	case nil:
		return n
	default:
		return y
	}
}

// ---------------------------------------------
// Statements
// ---------------------------------------------

func (this BeginStatement) String() string {
	return "BEGIN TRANSACTION"
}

func (this CancelStatement) String() string {
	return "CANCEL TRANSACTION"
}

func (this CommitStatement) String() string {
	return "COMMIT TRANSACTION"
}

func (this UseStatement) String() string {
	switch {
	case len(this.NS) == 0:
		return fmt.Sprintf("USE DB %v", this.DB)
	case len(this.DB) == 0:
		return fmt.Sprintf("USE NS %v", this.NS)
	default:
		return fmt.Sprintf("USE NS %v DB %v", this.NS, this.DB)
	}
}

func (this InfoStatement) String() string {
	switch this.Kind {
	case NAMESPACE:
		return fmt.Sprintf("INFO FOR NAMESPACE")
	case DATABASE:
		return fmt.Sprintf("INFO FOR DATABASE")
	default:
		return fmt.Sprintf("INFO FOR TABLE %v", this.What)
	}
}

func (this LetStatement) String() string {
	return fmt.Sprintf("LET %v = %v",
		this.Name,
		this.What,
	)
}

func (this ReturnStatement) String() string {
	return fmt.Sprintf("RETURN %v",
		this.What,
	)
}

func (this SelectStatement) String() string {
	return fmt.Sprintf("SELECT %v FROM %v%v%v%v%v%v%v",
		this.Expr,
		this.What,
		stringFromInterface(this.Cond, fmt.Sprintf(" WHERE %v", this.Cond), ""),
		this.Group,
		this.Order,
		stringFromInterface(this.Limit, fmt.Sprintf(" LIMIT %v", this.Limit), ""),
		stringFromInterface(this.Start, fmt.Sprintf(" START %v", this.Start), ""),
		stringFromInterface(this.Version, fmt.Sprintf(" VERSION %v", this.Version), ""),
	)
}

func (this CreateStatement) String() string {
	return fmt.Sprintf("CREATE %v%v RETURN %v",
		this.What,
		this.Data,
		this.Echo,
	)
}

func (this UpdateStatement) String() string {
	return fmt.Sprintf("CREATE %v%v%v RETURN %v",
		this.What,
		this.Data,
		this.Cond,
		this.Echo,
	)
}

func (this DeleteStatement) String() string {
	return fmt.Sprintf("DELETE %v%v%v RETURN %v",
		stringFromBool(this.Hard, "AND EXPUNGE ", ""),
		this.What,
		this.Cond,
		this.Echo,
	)
}

func (this RelateStatement) String() string {
	return fmt.Sprintf("RELATE %v FROM %v WITH %v%v%v RETURN %v",
		this.Type,
		this.From,
		this.With,
		this.Data,
		stringFromBool(this.Uniq, " UNIQUE", ""),
		this.Echo,
	)
}

func (this DefineNamespaceStatement) String() string {
	return fmt.Sprintf("DEFINE NAMESPACE %v",
		this.Name,
	)
}

func (this RemoveNamespaceStatement) String() string {
	return fmt.Sprintf("REMOVE NAMESPACE %v",
		this.Name,
	)
}

func (this DefineDatabaseStatement) String() string {
	return fmt.Sprintf("DEFINE DATABASE %v",
		this.Name,
	)
}

func (this RemoveDatabaseStatement) String() string {
	return fmt.Sprintf("REMOVE DATABASE %v",
		this.Name,
	)
}

func (this DefineLoginStatement) String() string {
	return fmt.Sprintf("DEFINE LOGIN %v ON %v PASSWORD ********",
		this.User,
		this.Kind,
	)
}

func (this RemoveLoginStatement) String() string {
	return fmt.Sprintf("REMOVE LOGIN %v ON %v",
		this.User,
		this.Kind,
	)
}

func (this DefineTokenStatement) String() string {
	return fmt.Sprintf("DEFINE TOKEN %v ON %v TYPE %v VALUE ********",
		this.Name,
		this.Kind,
		this.Type,
	)
}

func (this RemoveTokenStatement) String() string {
	return fmt.Sprintf("REMOVE TOKEN %v ON %v",
		this.Name,
		this.Kind,
	)
}

func (this DefineScopeStatement) String() string {
	return fmt.Sprintf("DEFINE SCOPE %v SESSION %v SIGNUP AS (%v) SIGNIN AS (%v)",
		this.Name,
		this.Time,
		this.Signup,
		this.Signin,
	)
}

func (this RemoveScopeStatement) String() string {
	return fmt.Sprintf("REMOVE SCOPE %v",
		this.Name,
	)
}

func (this DefineTableStatement) String() string {
	return fmt.Sprintf("DEFINE TABLE %v%v%v",
		this.What,
		stringFromBool(this.Full, " SCHEMAFULL", " SCHEMALESS"),
		stringIf(this.Perm != nil, this.Perm),
	)
}

func (this RemoveTableStatement) String() string {
	return fmt.Sprintf("REMOVE TABLE %v",
		this.What,
	)
}

func (this DefineFieldStatement) String() string {
	return fmt.Sprintf("DEFINE FIELD %v ON %v TYPE %v%v%v%v%v%v%v%v%v%v%v%v",
		this.Name,
		this.What,
		this.Type,
		stringFromFloat(this.Min, fmt.Sprintf(" MIN %v", this.Min), ""),
		stringFromFloat(this.Max, fmt.Sprintf(" MAX %v", this.Max), ""),
		stringFromArray(this.Enum, fmt.Sprintf(" ENUM %v", this.Enum), ""),
		stringFromString(this.Code, fmt.Sprintf(" CODE \"%v\"", this.Code), ""),
		stringFromString(this.Match, fmt.Sprintf(" MATCH /%v/", this.Match), ""),
		stringFromInterface(this.Default, fmt.Sprintf(" DEFAULT %v", this.Default), ""),
		stringFromBool(this.Notnull, " NOTNULL", ""),
		stringFromBool(this.Readonly, " READONLY", ""),
		stringFromBool(this.Validate, " VALIDATE", ""),
		stringFromBool(this.Mandatory, " MANDATORY", ""),
		stringIf(this.Perm != nil, this.Perm),
	)
}

func (this RemoveFieldStatement) String() string {
	return fmt.Sprintf("REMOVE FIELD %v ON %v",
		this.Name,
		this.What,
	)
}

func (this DefineIndexStatement) String() string {
	return fmt.Sprintf("DEFINE INDEX %v ON %v COLUMNS %v%v",
		this.Name,
		this.What,
		this.Cols,
		stringFromBool(this.Uniq, " UNIQUE", ""),
	)
}

func (this RemoveIndexStatement) String() string {
	return fmt.Sprintf("REMOVE INDEX %v",
		this.Name,
	)
}

func (this DefineViewStatement) String() string {
	return fmt.Sprintf("DEFINE VIEW %v",
		this.Name,
	)
}

func (this RemoveViewStatement) String() string {
	return fmt.Sprintf("REMOVE VIEW %v",
		this.Name,
	)
}

// ---------------------------------------------
// Literals
// ---------------------------------------------

func (this Exprs) String() string {
	var m []string
	for _, v := range this {
		m = append(m, fmt.Sprintf("%v", v))
	}
	return strings.Join(m, ", ")
}

func (this All) String() string {
	return "*"
}

func (this Any) String() string {
	return "?"
}

func (this Asc) String() string {
	return "ASC"
}

func (this Desc) String() string {
	return "DESC"
}

func (this Null) String() string {
	return "NULL"
}

func (this Void) String() string {
	return "VOID"
}

func (this Empty) String() string {
	return "EMPTY"
}

func (this Array) String() string {
	out, _ := json.Marshal(this)
	return string(out)
}

func (this Object) String() string {
	out, _ := json.Marshal(this)
	return string(out)
}

// ---------------------------------------------
// Field
// ---------------------------------------------

func (this Fields) String() string {
	var m []string
	for _, v := range this {
		m = append(m, v.String())
	}
	return fmt.Sprintf("%v",
		strings.Join(m, ", "),
	)
}

func (this Field) String() string {
	return fmt.Sprintf("%v%v",
		this.Expr,
		stringFromIdent(this.Alias, fmt.Sprintf(" AS %v", this.Alias), ""),
	)
}

// ---------------------------------------------
// Group
// ---------------------------------------------

func (this Groups) String() string {
	if len(this) == 0 {
		return ""
	}
	var m []string
	for _, v := range this {
		m = append(m, v.String())
	}
	return fmt.Sprintf(" GROUP BY %v",
		strings.Join(m, ", "),
	)
}

func (this Group) String() string {
	return fmt.Sprintf("%v",
		this.Expr,
	)
}

// ---------------------------------------------
// Order
// ---------------------------------------------

func (this Orders) String() string {
	if len(this) == 0 {
		return ""
	}
	var m []string
	for _, v := range this {
		m = append(m, v.String())
	}
	return fmt.Sprintf(" ORDER BY %v",
		strings.Join(m, ", "),
	)
}

func (this Order) String() string {
	return fmt.Sprintf("%v %v",
		this.Expr,
		this.Dir,
	)
}

// ---------------------------------------------
// Param
// ---------------------------------------------

func (this Params) String() string {
	var m []string
	for _, v := range this {
		m = append(m, v.String())
	}
	return strings.Join(m, ", ")
}

func (this Param) String() string {
	return fmt.Sprintf("$%v", this.ID)
}

// ---------------------------------------------
// Value
// ---------------------------------------------

func (this Values) String() string {
	var m []string
	for _, v := range this {
		m = append(m, v.String())
	}
	return strings.Join(m, ", ")
}

func (this Value) String() string {
	return fmt.Sprintf("\"%v\"", this.ID)
}

// ---------------------------------------------
// Ident
// ---------------------------------------------

func (this Idents) String() string {
	var m []string
	for _, v := range this {
		m = append(m, v.String())
	}
	return strings.Join(m, ", ")
}

func (this Ident) String() string {
	switch newToken(this.ID) {
	case ILLEGAL:
		if toQuote(this.ID) {
			return fmt.Sprintf("`%v`", this.ID)
		}
		return fmt.Sprintf("%v", this.ID)
	default:
		return fmt.Sprintf("`%v`", this.ID)
	}
}

// ---------------------------------------------
// Table
// ---------------------------------------------

func (this Tables) String() string {
	var m []string
	for _, v := range this {
		m = append(m, v.String())
	}
	return strings.Join(m, ", ")
}

func (this Table) String() string {
	switch newToken(this.TB) {
	case ILLEGAL:
		if toQuote(this.TB) {
			return fmt.Sprintf("`%v`", this.TB)
		}
		return fmt.Sprintf("%v", this.TB)
	default:
		return fmt.Sprintf("`%v`", this.TB)
	}
}

// ---------------------------------------------
// Thing
// ---------------------------------------------

func (this Things) String() string {
	var m []string
	for _, v := range this {
		m = append(m, v.String())
	}
	return strings.Join(m, ", ")
}

func (this Thing) String() string {
	tb := this.TB
	if toQuote(fmt.Sprint(this.TB)) {
		tb = fmt.Sprintf("{%v}", this.TB)
	}
	id := this.ID
	if toQuote(fmt.Sprint(this.ID)) {
		id = fmt.Sprintf("{%v}", this.ID)
	}
	return fmt.Sprintf("@%v:%v", tb, id)
}

// ---------------------------------------------
// Expressions
// ---------------------------------------------

func (this SubExpression) String() string {
	return fmt.Sprintf("(%v)",
		this.Expr,
	)
}

func (this FuncExpression) String() string {
	return fmt.Sprintf("%v()",
		this.Name,
	)
}

func (this ItemExpression) String() string {
	return fmt.Sprintf("%v %v %v",
		this.LHS,
		this.Op,
		this.RHS,
	)
}

func (this BinaryExpression) String() string {
	return fmt.Sprintf("%v%v%v",
		this.LHS,
		padToken(this.Op),
		this.RHS,
	)
}

func (this PathExpression) String() string {
	var m []string
	for _, v := range this.Expr {
		m = append(m, fmt.Sprintf("%v", v))
	}
	return strings.Join(m, "")
}

func (this PartExpression) String() string {
	return fmt.Sprintf("%v",
		this.Part,
	)
}

func (this JoinExpression) String() string {
	return fmt.Sprintf("%v",
		this.Join,
	)
}

func (this SubpExpression) String() string {
	return fmt.Sprintf("(%v%v%v)",
		this.What,
		stringFromIdent(this.Name, fmt.Sprintf(" AS %v", this.Name), ""),
		stringFromInterface(this.Cond, fmt.Sprintf(" WHERE %v", this.Cond), ""),
	)
}

func (this DataExpression) String() string {
	var m []string
	for _, v := range this.Data {
		m = append(m, v.String())
	}
	return fmt.Sprintf(" SET %v",
		strings.Join(m, ", "),
	)
}

func (this DiffExpression) String() string {
	return fmt.Sprintf(" DIFF %v",
		this.Data,
	)
}

func (this MergeExpression) String() string {
	return fmt.Sprintf(" MERGE %v",
		this.Data,
	)
}

func (this ContentExpression) String() string {
	return fmt.Sprintf(" CONTENT %v",
		this.Data,
	)
}

func (this PermExpression) String() string {

	var s, c, u, d, r string

	p := map[bool][]string{}

	if v, ok := this.Select.(bool); ok {
		s = stringFromBool(v, "FULL", "NONE")
		p[v] = append(p[v], fmt.Sprintf("select"))
	} else {
		s = fmt.Sprintf("WHERE %v", this.Select)
	}

	if v, ok := this.Create.(bool); ok {
		c = stringFromBool(v, "FULL", "NONE")
		p[v] = append(p[v], fmt.Sprintf("create"))
	} else {
		c = fmt.Sprintf("WHERE %v", this.Create)
	}

	if v, ok := this.Update.(bool); ok {
		u = stringFromBool(v, "FULL", "NONE")
		p[v] = append(p[v], fmt.Sprintf("update"))
	} else {
		u = fmt.Sprintf("WHERE %v", this.Update)
	}

	if v, ok := this.Delete.(bool); ok {
		d = stringFromBool(v, "FULL", "NONE")
		p[v] = append(p[v], fmt.Sprintf("delete"))
	} else {
		d = fmt.Sprintf("WHERE %v", this.Delete)
	}

	if v, ok := this.Relate.(bool); ok {
		r = stringFromBool(v, "FULL", "NONE")
		p[v] = append(p[v], fmt.Sprintf("relate"))
	} else {
		r = fmt.Sprintf("WHERE %v", this.Relate)
	}

	if len(p[true]) == 5 {
		return fmt.Sprintf(" PERMISSIONS FULL")
	}

	if len(p[false]) == 5 {
		return fmt.Sprintf(" PERMISSIONS NONE")
	}

	if len(p[true])+len(p[false]) == 5 {
		return fmt.Sprintf(" PERMISSIONS FOR %v FULL FOR %v NONE",
			strings.Join(p[true], ", "),
			strings.Join(p[false], ", "),
		)
	}

	return fmt.Sprintf(" PERMISSIONS%v%v%v%v%v",
		fmt.Sprintf(" FOR select %v", s),
		fmt.Sprintf(" FOR create %v", c),
		fmt.Sprintf(" FOR update %v", u),
		fmt.Sprintf(" FOR delete %v", d),
		fmt.Sprintf(" FOR relate %v", r),
	)

}
