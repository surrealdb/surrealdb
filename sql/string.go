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
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	_select method = iota
	_create
	_update
	_delete
)

type method int

type methods []method

func (this method) String() string {
	switch this {
	case _select:
		return "select"
	case _create:
		return "create"
	case _update:
		return "update"
	case _delete:
		return "delete"
	}
	return ""
}

func (this methods) String() string {
	m := make([]string, len(this))
	for k, v := range this {
		m[k] = v.String()
	}
	return strings.Join(m, ", ")
}

// ---------------------------------------------
// Helpers
// ---------------------------------------------

func print(s string, a ...interface{}) string {
	for k, v := range a {
		switch v.(type) {
		default:
		case nil:
			a[k] = "NULL"
		case []interface{}:
			out, _ := json.Marshal(v)
			a[k] = string(out)
		case map[string]interface{}:
			out, _ := json.Marshal(v)
			a[k] = string(out)
		}
	}
	return fmt.Sprintf(s, a...)
}

func maybe(b bool, v ...interface{}) string {
	switch b {
	case false:
		if len(v) >= 2 {
			return fmt.Sprint(v[1])
		}
	case true:
		if len(v) >= 1 {
			return fmt.Sprint(v[0])
		}
	}
	return ""
}

func quote(s string) string {
	t := newToken(s)
	switch t {
	case ILLEGAL:
		if toQuote(s) {
			return "`" + s + "`"
		}
		return s
	default:
		switch {
		case t.isKeyword():
			return "`" + s + "`"
		case t.isOperator():
			return "`" + s + "`"
		}
		return s
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
		case c == '.', c == '*':
			continue
		default:
			return true
		}
	}
	return false
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
		return print("USE DATABASE %v", quote(this.DB))
	case len(this.DB) == 0:
		return print("USE NAMESPACE %v", quote(this.NS))
	default:
		return print("USE NAMESPACE %v DATABASE %v", quote(this.NS), quote(this.DB))
	}
}

func (this InfoStatement) String() string {
	switch this.Kind {
	case NAMESPACE:
		return "INFO FOR NAMESPACE"
	case DATABASE:
		return "INFO FOR DATABASE"
	default:
		return print("INFO FOR TABLE %s", this.What)
	}
}

func (this IfStatement) String() string {
	m := make([]string, len(this.Cond))
	for k := range this.Cond {
		m[k] = print("%v THEN %v", this.Cond[k], this.Then[k])
	}
	return print("IF %v%v END",
		strings.Join(m, " ELSE IF "),
		maybe(this.Else != nil, print(" ELSE %v", this.Else)),
	)
}

func (this LetStatement) String() string {
	return print("LET %v = %v",
		this.Name,
		this.What,
	)
}

func (this ReturnStatement) String() string {
	return print("RETURN %v",
		this.What,
	)
}

func (this LiveStatement) String() string {
	return print("LIVE SELECT %v%v FROM %v%v",
		maybe(this.Diff, "DIFF"),
		this.Expr,
		this.What,
		maybe(this.Cond != nil, print(" WHERE %v", this.Cond)),
	)
}

func (this KillStatement) String() string {
	return print("KILL %v",
		this.What,
	)
}

func (this SelectStatement) String() string {
	return print("SELECT %v FROM %v%v%v%v%v%v%v%v",
		this.Expr,
		this.What,
		maybe(this.Cond != nil, print(" WHERE %v", this.Cond)),
		this.Group,
		this.Order,
		maybe(this.Limit != nil, print(" LIMIT %v", this.Limit)),
		maybe(this.Start != nil, print(" START %v", this.Start)),
		maybe(this.Version != nil, print(" VERSION %v", this.Version)),
		maybe(this.Timeout > 0, print(" TIMEOUT %v", this.Timeout.String())),
	)
}

func (this CreateStatement) String() string {
	return print("CREATE %v%v%v%v",
		this.What,
		maybe(this.Data != nil, print("%v", this.Data)),
		maybe(this.Echo != AFTER, print(" RETURN %v", this.Echo)),
		maybe(this.Timeout > 0, print(" TIMEOUT %v", this.Timeout.String())),
	)
}

func (this UpdateStatement) String() string {
	return print("UPDATE %v%v%v%v%v",
		this.What,
		maybe(this.Data != nil, print("%v", this.Data)),
		maybe(this.Cond != nil, print(" WHERE %v", this.Cond)),
		maybe(this.Echo != AFTER, print(" RETURN %v", this.Echo)),
		maybe(this.Timeout > 0, print(" TIMEOUT %v", this.Timeout.String())),
	)
}

func (this DeleteStatement) String() string {
	return print("DELETE %v%v%v%v%v",
		maybe(this.Hard, "AND EXPUNGE "),
		this.What,
		maybe(this.Cond != nil, print(" WHERE %v", this.Cond)),
		maybe(this.Echo != NONE, print(" RETURN %v", this.Echo)),
		maybe(this.Timeout > 0, print(" TIMEOUT %v", this.Timeout.String())),
	)
}

func (this RelateStatement) String() string {
	return print("RELATE %v FROM %v WITH %v%v%v%v%v",
		this.Type,
		this.From,
		this.With,
		maybe(this.Data != nil, print("%v", this.Data)),
		maybe(this.Uniq, " UNIQUE"),
		maybe(this.Echo != AFTER, print(" RETURN %v", this.Echo)),
		maybe(this.Timeout > 0, print(" TIMEOUT %v", this.Timeout.String())),
	)
}

func (this InsertStatement) String() string {
	return print("INSERT %v INTO %v%v%v",
		this.Data,
		this.Into,
		maybe(this.Echo != AFTER, print(" RETURN %v", this.Echo)),
		maybe(this.Timeout > 0, print(" TIMEOUT %v", this.Timeout.String())),
	)
}

func (this UpsertStatement) String() string {
	return print("UPSERT %v INTO %v%v%v",
		this.Data,
		this.Into,
		maybe(this.Echo != AFTER, print(" RETURN %v", this.Echo)),
		maybe(this.Timeout > 0, print(" TIMEOUT %v", this.Timeout.String())),
	)
}

func (this DefineNamespaceStatement) String() string {
	return print("DEFINE NAMESPACE %v",
		this.Name,
	)
}

func (this RemoveNamespaceStatement) String() string {
	return print("REMOVE NAMESPACE %v",
		this.Name,
	)
}

func (this DefineDatabaseStatement) String() string {
	return print("DEFINE DATABASE %v",
		this.Name,
	)
}

func (this RemoveDatabaseStatement) String() string {
	return print("REMOVE DATABASE %v",
		this.Name,
	)
}

func (this DefineLoginStatement) String() string {
	return print("DEFINE LOGIN %v ON %v PASSWORD ********",
		this.User,
		this.Kind,
	)
}

func (this RemoveLoginStatement) String() string {
	return print("REMOVE LOGIN %v ON %v",
		this.User,
		this.Kind,
	)
}

func (this DefineTokenStatement) String() string {
	return print("DEFINE TOKEN %v ON %v TYPE %v VALUE ********",
		this.Name,
		this.Kind,
		this.Type,
	)
}

func (this RemoveTokenStatement) String() string {
	return print("REMOVE TOKEN %v ON %v",
		this.Name,
		this.Kind,
	)
}

func (this DefineScopeStatement) String() string {
	return print("DEFINE SCOPE %v%v%v%v%v",
		this.Name,
		maybe(this.Time > 0, print(" SESSION %v", this.Time)),
		maybe(this.Signup != nil, print(" SIGNUP AS %v", this.Signup)),
		maybe(this.Signin != nil, print(" SIGNIN AS %v", this.Signin)),
		maybe(this.Connect != nil, print(" CONNECT AS %v", this.Connect)),
	)
}

func (this RemoveScopeStatement) String() string {
	return print("REMOVE SCOPE %v",
		this.Name,
	)
}

func (this DefineTableStatement) String() (s string) {
	w := maybe(this.Cond != nil, print(" WHERE %v", this.Cond))
	return print("DEFINE TABLE %v%v%v%v%v",
		maybe(this.Name != nil, print("%s", this.Name), print("%s", this.What)),
		maybe(this.Full, " SCHEMAFULL"),
		maybe(this.Drop, " DROP"),
		maybe(this.Lock, print(" AS SELECT %v FROM %v%v%v", this.Expr, this.From, w, this.Group)),
		maybe(this.Perms != nil, this.Perms),
	)
}

func (this RemoveTableStatement) String() string {
	return print("REMOVE TABLE %v",
		this.What,
	)
}

func (this DefineEventStatement) String() string {
	return print("DEFINE EVENT %v ON %v WHEN %v THEN %v",
		this.Name,
		this.What,
		this.When,
		this.Then,
	)
}

func (this RemoveEventStatement) String() string {
	return print("REMOVE EVENT %v ON %v",
		this.Name,
		this.What,
	)
}

func (this DefineFieldStatement) String() string {
	return print("DEFINE FIELD %v ON %v%v%v%v%v%v%v",
		this.Name,
		this.What,
		maybe(this.Type != "", print(" TYPE %v", this.Type)),
		maybe(this.Kind != "", print(" (%v)", this.Kind)),
		maybe(this.Value != nil, print(" VALUE %v", this.Value)),
		maybe(this.Assert != nil, print(" ASSERT %v", this.Assert)),
		maybe(this.Priority != 0, print(" PRIORITY %v", this.Priority)),
		maybe(this.Perms != nil, this.Perms),
	)
}

func (this RemoveFieldStatement) String() string {
	return print("REMOVE FIELD %v ON %v",
		this.Name,
		this.What,
	)
}

func (this DefineIndexStatement) String() string {
	return print("DEFINE INDEX %v ON %v COLUMNS %v%v",
		this.Name,
		this.What,
		this.Cols,
		maybe(this.Uniq, " UNIQUE"),
	)
}

func (this RemoveIndexStatement) String() string {
	return print("REMOVE INDEX %v ON %v",
		this.Name,
		this.What,
	)
}

// ---------------------------------------------
// Literals
// ---------------------------------------------

func (this Exprs) String() string {
	m := make([]string, len(this))
	for k, v := range this {
		m[k] = print("%v", v)
	}
	return strings.Join(m, ", ")
}

func (this All) String() string {
	return "*"
}

func (this Any) String() string {
	return "?"
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

// ---------------------------------------------
// Field
// ---------------------------------------------

func (this Fields) String() string {
	m := make([]string, len(this))
	for k, v := range this {
		m[k] = v.String()
	}
	return print("%v",
		strings.Join(m, ", "),
	)
}

func (this Field) String() string {
	return print("%v%v",
		this.Expr,
		maybe(this.Alias != "", print(" AS %s", quote(this.Alias))),
	)
}

// ---------------------------------------------
// Group
// ---------------------------------------------

func (this Groups) String() string {
	if len(this) == 0 {
		return ""
	}
	m := make([]string, len(this))
	for k, v := range this {
		m[k] = v.String()
	}
	return print(" GROUP BY %v",
		strings.Join(m, ", "),
	)
}

func (this Group) String() string {
	return print("%v",
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
	m := make([]string, len(this))
	for k, v := range this {
		m[k] = v.String()
	}
	return print(" ORDER BY %v",
		strings.Join(m, ", "),
	)
}

func (this Order) String() string {
	return print("%v %v",
		this.Expr,
		maybe(this.Dir, "ASC", "DESC"),
	)
}

// ---------------------------------------------
// Model
// ---------------------------------------------

func (this Model) String() string {
	switch {
	case this.INC == 0:
		max := strconv.FormatFloat(this.MAX, 'f', -1, 64)
		return print("|%s:%s|", quote(this.TB), max)
	case this.INC == 1:
		min := strconv.FormatFloat(this.MIN, 'f', -1, 64)
		max := strconv.FormatFloat(this.MAX, 'f', -1, 64)
		return print("|%s:%s..%s|", quote(this.TB), min, max)
	default:
		inc := strconv.FormatFloat(this.INC, 'f', -1, 64)
		min := strconv.FormatFloat(this.MIN, 'f', -1, 64)
		max := strconv.FormatFloat(this.MAX, 'f', -1, 64)
		return print("|%s:%s,%s..%s|", quote(this.TB), min, inc, max)
	}
}

// ---------------------------------------------
// Param
// ---------------------------------------------

func (this Params) String() string {
	m := make([]string, len(this))
	for k, v := range this {
		m[k] = v.String()
	}
	return strings.Join(m, ", ")
}

func (this Param) String() string {
	return print("$%v", this.ID)
}

// ---------------------------------------------
// Value
// ---------------------------------------------

func (this Values) String() string {
	m := make([]string, len(this))
	for k, v := range this {
		m[k] = v.String()
	}
	return strings.Join(m, ", ")
}

func (this Value) String() string {
	return print("\"%v\"", this.ID)
}

// ---------------------------------------------
// Ident
// ---------------------------------------------

func (this Idents) String() string {
	m := make([]string, len(this))
	for k, v := range this {
		m[k] = v.String()
	}
	return strings.Join(m, ", ")
}

func (this Ident) String() string {
	return quote(this.ID)
}

// ---------------------------------------------
// Table
// ---------------------------------------------

func (this Tables) String() string {
	m := make([]string, len(this))
	for k, v := range this {
		m[k] = v.String()
	}
	return strings.Join(m, ", ")
}

func (this Table) String() string {
	return quote(this.TB)
}

// ---------------------------------------------
// Batch
// ---------------------------------------------

func (this Batchs) String() string {
	m := make([]string, len(this))
	for k, v := range this {
		m[k] = v.String()
	}
	return strings.Join(m, ", ")
}

func (this Batch) String() string {
	return print("batch(%v, [%v]", this.TB, this.BA)
}

// ---------------------------------------------
// Thing
// ---------------------------------------------

func (this Things) String() string {
	m := make([]string, len(this))
	for k, v := range this {
		m[k] = v.String()
	}
	return strings.Join(m, ", ")
}

func (this Thing) String() string {
	tb := this.TB
	if toQuote(this.TB) {
		tb = print("⟨%v⟩", this.TB)
	}
	id := this.ID
	switch v := this.ID.(type) {
	case int64:
		id = strconv.FormatInt(v, 10)
	case float64:
		id = strconv.FormatFloat(v, 'f', -1, 64)
	case time.Time:
		id = print("⟨%v⟩", v.Format(RFCNano))
	case string:
		if toQuote(v) {
			id = print("⟨%v⟩", v)
		}
	default:
		if toQuote(fmt.Sprint(v)) {
			id = print("⟨%v⟩", v)
		}
	}
	return print("%v:%v", tb, id)
}

// ---------------------------------------------
// Point
// ---------------------------------------------

func (this Points) String() string {
	m := make([]string, len(this))
	for k, v := range this {
		m[k] = v.String()
	}
	return strings.Join(m, ", ")
}

func (this Point) String() string {
	return print("geo.point(%v,%v)", this.LA, this.LO)
}

func (this Point) JSON() string {
	return fmt.Sprintf(`[%v,%v]`, this.LA, this.LO)
}

// ---------------------------------------------
// Circle
// ---------------------------------------------

func (this Circles) String() string {
	m := make([]string, len(this))
	for k, v := range this {
		m[k] = v.String()
	}
	return strings.Join(m, ", ")
}

func (this Circle) String() string {
	return print("geo.circle(%v,%v)", this.CE, this.RA)
}

func (this Circle) JSON() string {
	return fmt.Sprintf(`{"type":"circle","center":%v,"radius":%v}`, this.CE.JSON(), this.RA)
}

// ---------------------------------------------
// Polygon
// ---------------------------------------------

func (this Polygons) String() string {
	m := make([]string, len(this))
	for k, v := range this {
		m[k] = v.String()
	}
	return strings.Join(m, ", ")
}

func (this Polygon) String() string {
	var m []string
	for _, v := range this.PS {
		m = append(m, v.String())
	}
	return print("geo.polygon(%v)",
		strings.Join(m, ", "),
	)
}

func (this Polygon) JSON() string {
	var m []string
	for _, p := range this.PS {
		m = append(m, p.JSON())
	}
	return fmt.Sprintf(`{"type":"polygon","points":[%v]}`, strings.Join(m, ","))
}

// ---------------------------------------------
// Expressions
// ---------------------------------------------

func (this SubExpression) String() string {
	return print("(%v)",
		this.Expr,
	)
}

func (this IfelExpression) String() string {
	m := make([]string, len(this.Cond))
	for k := range this.Cond {
		m[k] = print("%v THEN %v", this.Cond[k], this.Then[k])
	}
	return print("IF %v%v END",
		strings.Join(m, " ELSE IF "),
		maybe(this.Else != nil, print(" ELSE %v", this.Else)),
	)
}

func (this FuncExpression) String() string {
	return print("%v(%v)",
		this.Name,
		this.Args,
	)
}

func (this ItemExpression) String() string {
	return print("%v %v %v",
		this.LHS,
		this.Op,
		this.RHS,
	)
}

func (this BinaryExpression) String() string {
	return print("%v %v %v",
		this.LHS,
		this.Op,
		this.RHS,
	)
}

func (this PathExpression) String() string {
	var m []string
	for _, v := range this.Expr {
		m = append(m, print("%v", v))
	}
	return strings.Join(m, "")
}

func (this PartExpression) String() string {
	return print("%v",
		this.Part,
	)
}

func (this JoinExpression) String() string {
	return print("%v",
		this.Join,
	)
}

func (this SubpExpression) String() string {
	return print("(%v%v%v)",
		this.What,
		maybe(this.Name != nil, print(" AS %v", this.Name)),
		maybe(this.Cond != nil, print(" WHERE %v", this.Cond)),
	)
}

func (this DataExpression) String() string {
	var m []string
	for _, v := range this.Data {
		m = append(m, v.String())
	}
	return print(" SET %v",
		strings.Join(m, ", "),
	)
}

func (this DiffExpression) String() string {
	return print(" DIFF %v",
		this.Data,
	)
}

func (this MergeExpression) String() string {
	return print(" MERGE %v",
		this.Data,
	)
}

func (this ContentExpression) String() string {
	return print(" CONTENT %v",
		this.Data,
	)
}

func (this PermExpression) String() string {

	var k, o string

	a := []string{}
	m := map[string]methods{}

	if v, ok := this.Select.(bool); ok {
		k = maybe(v, "FULL", "NONE")
		m[k] = append(m[k], _select)
	} else {
		k = print("WHERE %v", this.Select)
		m[k] = append(m[k], _select)
	}

	if v, ok := this.Create.(bool); ok {
		k = maybe(v, "FULL", "NONE")
		m[k] = append(m[k], _create)
	} else {
		k = print("WHERE %v", this.Create)
		m[k] = append(m[k], _create)
	}

	if v, ok := this.Update.(bool); ok {
		k = maybe(v, "FULL", "NONE")
		m[k] = append(m[k], _update)
	} else {
		k = print("WHERE %v", this.Update)
		m[k] = append(m[k], _update)
	}

	if v, ok := this.Delete.(bool); ok {
		k = maybe(v, "FULL", "NONE")
		m[k] = append(m[k], _delete)
	} else {
		k = print("WHERE %v", this.Delete)
		m[k] = append(m[k], _delete)
	}

	if len(m) == 1 {
		for k := range m {
			return print(" PERMISSIONS %v", k)
		}
	}

	for k := range m {
		a = append(a, k)
	}

	sort.Slice(a, func(i, j int) bool {
		return m[a[i]][0] < m[a[j]][0]
	})

	for _, v := range a {
		o += print(" FOR %v %v", m[v], v)
	}

	return print(" PERMISSIONS%v", o)

}
