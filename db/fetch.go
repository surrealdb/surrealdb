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

package db

import (
	"context"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"golang.org/x/text/language"
	"golang.org/x/text/search"

	"github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
	"github.com/abcum/surreal/util/deep"
	"github.com/abcum/surreal/util/fncs"
)

var ign = data.New()

func (e *executor) fetch(ctx context.Context, val interface{}, doc *data.Doc) (out interface{}, err error) {

	switch val := val.(type) {
	default:
		return val, nil
	case *sql.Null:
		return nil, nil
	case *sql.Value:
		return val.VA, nil
	case []byte:
		return string(val), nil
	case int:
		return float64(val), nil
	case int8:
		return float64(val), nil
	case int16:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case uint:
		return float64(val), nil
	case uint16:
		return float64(val), nil
	case uint32:
		return float64(val), nil
	case uint64:
		return float64(val), nil
	case float32:
		return float64(val), nil
	case float64:
		return float64(val), nil
	case []interface{}:
		return deep.Copy(val), nil
	case map[string]interface{}:
		return deep.Copy(val), nil

	case *sql.Regex:

		return regexp.Compile(val.VA)

	case *sql.Ident:

		switch {
		default:
			return val, nil
		case doc == ign:
			return val, queryIdentFailed
		case doc != nil:

			fnc := func(key string, val interface{}, path []string) interface{} {
				if len(path) > 0 {
					switch res := val.(type) {
					case []interface{}:
						val, _ = e.fetchArray(ctx, res, doc)
						return val
					case *sql.Thing:
						val, _ = e.fetchThing(ctx, res, doc)
						return val
					}
				}
				return val
			}

			res := doc.Fetch(fnc, val.VA).Data()

			return e.fetch(ctx, res, doc)

		}

	case *sql.Param:

		if len(val.VA) > 0 {

			for _, s := range paramSearchKeys {

				if obj, ok := ctx.Value(s).(*data.Doc); ok {

					fnc := func(key string, val interface{}, path []string) interface{} {
						if len(path) > 0 {
							switch res := val.(type) {
							case []interface{}:
								val, _ = e.fetchArray(ctx, res, doc)
								return val
							case *sql.Thing:
								val, _ = e.fetchThing(ctx, res, doc)
								return val
							}
						}
						return val
					}

					res := obj.Fetch(fnc, val.VA).Data()

					if res != nil {
						return e.fetch(ctx, res, doc)
					}

				}

			}

		}

		return nil, nil

	case *sql.RunStatement:

		return e.fetch(ctx, val.Expr, doc)

	case *sql.IfStatement:

		for k, v := range val.Cond {
			ife, err := e.fetch(ctx, v, doc)
			if err != nil {
				return nil, err
			}
			if chk, ok := ife.(bool); ok && chk {
				return e.fetch(ctx, val.Then[k], doc)
			}
		}
		return e.fetch(ctx, val.Else, doc)

	case *sql.IfelExpression:

		for k, v := range val.Cond {
			ife, err := e.fetch(ctx, v, doc)
			if err != nil {
				return nil, err
			}
			if chk, ok := ife.(bool); ok && chk {
				return e.fetch(ctx, val.Then[k], doc)
			}
		}
		return e.fetch(ctx, val.Else, doc)

	case *sql.FuncExpression:

		var args []interface{}
		for _, v := range val.Args {
			val, err := e.fetch(ctx, v, doc)
			if err != nil {
				return nil, err
			}
			args = append(args, val)
		}
		res, err := fncs.Run(ctx, val.Name, args...)
		if err != nil {
			return nil, err
		}
		return e.fetch(ctx, res, doc)

	case *sql.SubExpression:

		switch exp := val.Expr.(type) {
		default:
			return e.fetch(ctx, exp, doc)
		case *sql.SelectStatement:
			return e.fetchSelect(ctx, exp, doc)
		case *sql.CreateStatement:
			return e.fetchCreate(ctx, exp, doc)
		case *sql.UpdateStatement:
			return e.fetchUpdate(ctx, exp, doc)
		case *sql.DeleteStatement:
			return e.fetchDelete(ctx, exp, doc)
		case *sql.RelateStatement:
			return e.fetchRelate(ctx, exp, doc)
		case *sql.InsertStatement:
			return e.fetchInsert(ctx, exp, doc)
		case *sql.UpsertStatement:
			return e.fetchUpsert(ctx, exp, doc)
		}

	case *sql.MultExpression:

		for _, exp := range val.Expr {

			switch exp := exp.(type) {
			default:
				out, err = e.fetch(ctx, exp, doc)
			case *sql.SelectStatement:
				out, err = e.fetchSelect(ctx, exp, doc)
			case *sql.CreateStatement:
				out, err = e.fetchCreate(ctx, exp, doc)
			case *sql.UpdateStatement:
				out, err = e.fetchUpdate(ctx, exp, doc)
			case *sql.DeleteStatement:
				out, err = e.fetchDelete(ctx, exp, doc)
			case *sql.RelateStatement:
				out, err = e.fetchRelate(ctx, exp, doc)
			case *sql.InsertStatement:
				out, err = e.fetchInsert(ctx, exp, doc)
			case *sql.UpsertStatement:
				out, err = e.fetchUpsert(ctx, exp, doc)
			}

			if err != nil {
				return out, err
			}

		}

		return nil, nil

	case *sql.PathExpression:

		return e.fetchPaths(ctx, doc, val.Expr...)

	case *sql.BinaryExpression:

		l, err := e.fetch(ctx, val.LHS, doc)
		if err != nil {
			return nil, err
		}

		switch val.Op {
		case sql.OR:
			if calcAsBool(l) {
				return true, nil
			}
		case sql.AND:
			if !calcAsBool(l) {
				return false, nil
			}
		}

		r, err := e.fetch(ctx, val.RHS, doc)
		if err != nil {
			return nil, err
		}

		switch val.Op {
		case sql.EEQ:
			return l == r, nil
		case sql.NEE:
			return l != r, nil
		case sql.AND, sql.OR:
			return binaryBool(val.Op, l, r), nil
		case sql.ADD, sql.SUB, sql.MUL, sql.DIV, sql.INC, sql.DEC:
			return binaryMath(val.Op, l, r), nil
		case sql.EQ, sql.NEQ, sql.ANY, sql.LT, sql.LTE, sql.GT, sql.GTE:
			return binaryCheck(val.Op, l, r, val.LHS, val.RHS, doc), nil
		case sql.SIN, sql.SNI, sql.INS, sql.NIS, sql.MAT, sql.NAT, sql.MAY:
			return binaryCheck(val.Op, l, r, val.LHS, val.RHS, doc), nil
		case sql.CONTAINSALL, sql.CONTAINSSOME, sql.CONTAINSNONE:
			return binaryCheck(val.Op, l, r, val.LHS, val.RHS, doc), nil
		case sql.ALLCONTAINEDIN, sql.SOMECONTAINEDIN, sql.NONECONTAINEDIN:
			return binaryCheck(val.Op, l, r, val.LHS, val.RHS, doc), nil
		}

	}

	return nil, nil

}

func (e *executor) fetchPaths(ctx context.Context, doc *data.Doc, exprs ...sql.Expr) (interface{}, error) {

	var expr sql.Expr

	if len(exprs) == 0 {
		return doc.Data(), nil
	}

	expr, exprs = exprs[0], exprs[1:]

	switch val := expr.(type) {
	case *sql.JoinExpression:
		switch val.Join {
		case sql.DOT:
			return e.fetchPaths(ctx, doc, exprs...)
		case sql.OEDGE:
			return nil, errFeatureNotImplemented
		case sql.IEDGE:
			return nil, errFeatureNotImplemented
		case sql.BEDGE:
			return nil, errFeatureNotImplemented
		}
	case *sql.PartExpression:
		switch val := val.Part.(type) {
		case *sql.All:
			return e.fetchPaths(ctx, doc, exprs...)
		case *sql.Param:
			res, err := e.fetch(ctx, val, doc)
			if err != nil {
				return nil, err
			}
			return e.fetchPaths(ctx, data.Consume(res), exprs...)
		case *sql.Ident:
			res, err := e.fetch(ctx, val, doc)
			if err != nil {
				return nil, err
			}
			return e.fetchPaths(ctx, data.Consume(res), exprs...)
		case *sql.Thing:
			res, err := e.fetchThing(ctx, val, doc)
			if err != nil {
				return nil, err
			}
			return e.fetchPaths(ctx, data.Consume(res), exprs...)
		}
	}

	return nil, nil

}

func (e *executor) fetchThing(ctx context.Context, val *sql.Thing, doc *data.Doc) (interface{}, error) {

	ver, err := e.fetchVersion(ctx, ctx.Value(ctxKeyVersion))
	if err != nil {
		return nil, err
	}

	res, err := e.executeSelect(ctx, &sql.SelectStatement{
		KV:       cnf.Settings.DB.Base,
		NS:       ctx.Value(ctxKeyNs).(string),
		DB:       ctx.Value(ctxKeyDb).(string),
		Expr:     []*sql.Field{{Expr: &sql.All{}}},
		What:     []sql.Expr{val},
		Version:  sql.Expr(ver),
		Parallel: 1,
	})

	if err != nil {
		return nil, err
	}

	if len(res) > 0 {
		return res[0], nil
	}

	return nil, nil

}

func (e *executor) fetchArray(ctx context.Context, val []interface{}, doc *data.Doc) (interface{}, error) {

	ver, err := e.fetchVersion(ctx, ctx.Value(ctxKeyVersion))
	if err != nil {
		return nil, err
	}

	res, err := e.executeSelect(ctx, &sql.SelectStatement{
		KV:       cnf.Settings.DB.Base,
		NS:       ctx.Value(ctxKeyNs).(string),
		DB:       ctx.Value(ctxKeyDb).(string),
		Expr:     []*sql.Field{{Expr: &sql.All{}}},
		What:     []sql.Expr{val},
		Version:  sql.Expr(ver),
		Parallel: 1,
	})

	if err != nil {
		return nil, err
	}

	return res, nil

}

func (e *executor) fetchPerms(ctx context.Context, val sql.Expr, tb *sql.Ident) error {

	// If the table does exist we reset the
	// context to DB level so that no other
	// embedded permissions are checked on
	// records within these permissions.

	ctx = context.WithValue(ctx, ctxKeyKind, cnf.AuthDB)

	// We then try to process the relevant
	// permissions expression, but only if
	// the specified expression doesn't
	// reference any document fields.

	res, err := e.fetch(ctx, val, ign)

	// If we receive an 'ident failed' error
	// it is because the table permission
	// expression contains a field check,
	// and therefore we must check each
	// record individually to see if it can
	// be accessed or not.

	if err != queryIdentFailed {
		if res, ok := res.(bool); ok && !res {
			return &PermsError{table: tb.VA}
		}
	}

	return nil

}

func (e *executor) fetchLimit(ctx context.Context, val sql.Expr) (int, error) {

	v, err := e.fetch(ctx, val, nil)
	if err != nil {
		return -1, err
	}

	switch v := v.(type) {
	case float64:
		return int(v), nil
	case int64:
		return int(v), nil
	case nil:
		return -1, nil
	default:
		return -1, &LimitError{found: v}
	}

}

func (e *executor) fetchStart(ctx context.Context, val sql.Expr) (int, error) {

	v, err := e.fetch(ctx, val, nil)
	if err != nil {
		return -1, err
	}

	switch v := v.(type) {
	case float64:
		return int(v), nil
	case int64:
		return int(v), nil
	case nil:
		return -1, nil
	default:
		return -1, &StartError{found: v}
	}

}

func (e *executor) fetchVersion(ctx context.Context, val sql.Expr) (int64, error) {

	if v, ok := val.(int64); ok {
		return v, nil
	}

	v, err := e.fetch(ctx, val, nil)
	if err != nil {
		return math.MaxInt64, err
	}

	switch v := v.(type) {
	case time.Time:
		return v.UnixNano(), nil
	case nil:
		return math.MaxInt64, nil
	default:
		return math.MaxInt64, &VersnError{found: v}
	}

}

func (e *executor) fetchOutputs(ctx context.Context, stm *sql.SelectStatement) (int, error) {

	l, err := e.fetchLimit(ctx, stm.Limit)
	if err != nil {
		return -1, err
	}

	if len(stm.What) == 1 {
		if _, ok := stm.What[0].(*sql.Thing); ok {
			l = 1
		}
	}

	return l, nil

}

func calcAsBool(i interface{}) bool {

	switch v := i.(type) {
	default:
		return false
	case bool:
		return v
	case int64:
		return v > 0
	case float64:
		return v > 0
	case string:
		return v != ""
	case time.Time:
		return v.UnixNano() > 0
	case *sql.Thing:
		return true
	case []interface{}:
		return len(v) > 0
	case map[string]interface{}:
		return len(v) > 0
	}

}

func calcAsMath(i interface{}) float64 {

	switch v := i.(type) {
	default:
		return 0
	case bool:
		if v {
			return 1
		}
		return 0
	case int64:
		return float64(v)
	case float64:
		return v
	case time.Time:
		return float64(v.UnixNano())
	}

}

func binaryBool(op sql.Token, l, r interface{}) interface{} {

	a := calcAsBool(l)
	b := calcAsBool(r)

	switch op {
	case sql.AND:
		return a && b
	case sql.OR:
		return a || b
	}

	return nil

}

func binaryMath(op sql.Token, l, r interface{}) interface{} {

	a := calcAsMath(l)
	b := calcAsMath(r)

	switch op {
	case sql.ADD, sql.INC:
		return a + b
	case sql.SUB, sql.DEC:
		return a - b
	case sql.MUL:
		return a * b
	case sql.DIV:
		if b != 0 {
			return a / b
		}
	}

	return nil

}

func binaryCheck(op sql.Token, l, r, lo, ro interface{}, d *data.Doc) interface{} {

	switch lo.(type) {
	case *sql.Void:
		switch ro.(type) {
		case *sql.Null:
			return op == sql.NEQ
		case *sql.Void:
			return op == sql.EQ
		case *sql.Empty:
			return op == sql.EQ
		}
	case *sql.Empty:
		switch ro.(type) {
		case *sql.Null:
			return op == sql.EQ
		case *sql.Void:
			return op == sql.EQ
		case *sql.Empty:
			return op == sql.EQ
		}
	}

	switch ro.(type) {
	case *sql.Void:
		switch lo.(type) {
		case *sql.Null:
			return op == sql.NEQ
		case *sql.Void:
			return op == sql.EQ
		case *sql.Empty:
			return op == sql.EQ
		}
	case *sql.Empty:
		switch lo.(type) {
		case *sql.Null:
			return op == sql.EQ
		case *sql.Void:
			return op == sql.EQ
		case *sql.Empty:
			return op == sql.EQ
		}
	}

	if d != nil {

		switch lo.(type) {
		case *sql.Void:
			switch r := ro.(type) {
			case *sql.Ident:
				if op == sql.EQ {
					return d.Exists(r.VA) == false
				} else if op == sql.NEQ {
					return d.Exists(r.VA) == true
				}
			}
		case *sql.Null:
			switch r := ro.(type) {
			case *sql.Ident:
				if op == sql.EQ {
					return d.Exists(r.VA) == true && d.Get(r.VA).Data() == nil
				} else if op == sql.NEQ {
					return d.Exists(r.VA) == false || d.Get(r.VA).Data() != nil
				}
			}
		}

		switch ro.(type) {
		case *sql.Void:
			switch l := lo.(type) {
			case *sql.Ident:
				if op == sql.EQ {
					return d.Exists(l.VA) == false
				} else if op == sql.NEQ {
					return d.Exists(l.VA) == true
				}
			}
		case *sql.Null:
			switch l := lo.(type) {
			case *sql.Ident:
				if op == sql.EQ {
					return d.Exists(l.VA) == true && d.Get(l.VA).Data() == nil
				} else if op == sql.NEQ {
					return d.Exists(l.VA) == false || d.Get(l.VA).Data() != nil
				}
			}
		}

	}

	switch l := l.(type) {

	case nil:
		switch r := r.(type) {
		case nil:
			return op == sql.EQ
		case *sql.Null:
			return op == sql.EQ
		case *sql.Empty:
			return op == sql.EQ
		case []interface{}:
			return chkArrayR(op, l, r)
		}

	case *sql.Null:
		switch r := r.(type) {
		case nil:
			return op == sql.EQ
		case *sql.Null:
			return op == sql.EQ
		case *sql.Empty:
			return op == sql.EQ
		case []interface{}:
			return chkArrayR(op, l, r)
		}

	case *sql.Empty:
		switch r := r.(type) {
		case nil:
			return op == sql.EQ
		case *sql.Null:
			return op == sql.EQ
		case *sql.Empty:
			return op == sql.EQ
		case string:
			return chkLen(op, r)
		case []interface{}:
			return chkArrayR(op, l, r)
		case map[string]interface{}:
			return chkObject(op, r, l)
		}

	case *sql.Thing:
		switch r := r.(type) {
		case *sql.Thing:
			return chkThing(op, l, r)
		case string:
			return chkString(op, r, l.String())
		case []interface{}:
			return chkArrayR(op, l, r)
		}

	case bool:
		switch r := r.(type) {
		case bool:
			return chkBool(op, l, r)
		case string:
			if b, err := strconv.ParseBool(r); err == nil {
				return chkBool(op, l, b)
			}
		case *regexp.Regexp:
			return chkRegex(op, strconv.FormatBool(l), r)
		case []interface{}:
			return chkArrayR(op, l, r)
		}

	case string:
		switch r := r.(type) {
		case bool:
			if b, err := strconv.ParseBool(l); err == nil {
				return chkBool(op, r, b)
			}
		case string:
			return chkString(op, l, r)
		case int64:
			if n, err := strconv.ParseInt(l, 10, 64); err == nil {
				return chkInt(op, r, n)
			}
		case float64:
			if n, err := strconv.ParseFloat(l, 64); err == nil {
				return chkFloat(op, r, n)
			}
		case time.Time:
			return chkString(op, l, r.String())
		case *sql.Empty:
			return chkLen(op, l)
		case *sql.Thing:
			return chkString(op, l, r.String())
		case *regexp.Regexp:
			return chkRegex(op, l, r)
		case []interface{}:
			return chkArrayR(op, l, r)
		}

	case int64:
		switch r := r.(type) {
		case string:
			if n, err := strconv.ParseInt(r, 10, 64); err == nil {
				return chkInt(op, l, n)
			}
		case int64:
			return chkInt(op, l, r)
		case float64:
			return chkFloat(op, float64(l), r)
		case time.Time:
			return chkInt(op, l, r.UnixNano())
		case *regexp.Regexp:
			return chkRegex(op, strconv.FormatInt(l, 10), r)
		case []interface{}:
			return chkArrayR(op, l, r)
		}

	case float64:
		switch r := r.(type) {
		case string:
			if n, err := strconv.ParseFloat(r, 64); err == nil {
				return chkFloat(op, l, n)
			}
		case int64:
			return chkFloat(op, l, float64(r))
		case float64:
			return chkFloat(op, l, r)
		case time.Time:
			return chkFloat(op, l, float64(r.UnixNano()))
		case *regexp.Regexp:
			return chkRegex(op, strconv.FormatFloat(l, 'g', -1, 64), r)
		case []interface{}:
			return chkArrayR(op, l, r)
		}

	case time.Time:
		switch r := r.(type) {
		case string:
			return chkString(op, l.String(), r)
		case int64:
			return chkInt(op, l.UnixNano(), r)
		case float64:
			return chkFloat(op, float64(l.UnixNano()), r)
		case time.Time:
			return chkInt(op, l.UnixNano(), r.UnixNano())
		case *regexp.Regexp:
			return chkRegex(op, l.String(), r)
		case []interface{}:
			return chkArrayR(op, l, r)
		}

	case []interface{}:
		switch r := r.(type) {
		default:
			return chkArrayL(op, l, r)
		case bool:
			return chkArrayL(op, l, r)
		case string:
			return chkArrayL(op, l, r)
		case int64:
			return chkArrayL(op, l, r)
		case float64:
			return chkArrayL(op, l, r)
		case time.Time:
			return chkArrayL(op, l, r)
		case *regexp.Regexp:
			return chkMatch(op, l, r)
		case []interface{}:
			return chkArray(op, l, r)
		case map[string]interface{}:
			return chkArrayL(op, l, r)
		}

	case map[string]interface{}:
		switch r := r.(type) {
		default:
			return chkObject(op, l, r)
		case []interface{}:
			return chkArrayR(op, l, r)
		case map[string]interface{}:
			return chkObject(op, l, r)
		}

	}

	return negOp(op)

}

func posOp(op sql.Token) bool {
	return chkOp(op) > 1
}

func negOp(op sql.Token) bool {
	return chkOp(op) < 0
}

func chkOp(op sql.Token) int8 {
	switch op {
	case sql.EQ, sql.SIN, sql.INS, sql.MAT, sql.ANY:
		return +1
	case sql.NEQ, sql.SNI, sql.NIS, sql.NAT, sql.MAY:
		return -1
	case sql.CONTAINSALL:
		return +1
	case sql.CONTAINSSOME:
		return +1
	case sql.CONTAINSNONE:
		return -1
	case sql.ALLCONTAINEDIN:
		return +1
	case sql.SOMECONTAINEDIN:
		return +1
	case sql.NONECONTAINEDIN:
		return -1
	default:
		return 0
	}
}

func chkLen(op sql.Token, s string) (val bool) {
	switch op {
	case sql.EQ:
		return len(s) == 0
	case sql.NEQ:
		return len(s) != 0
	}
	return negOp(op)
}

func chkBool(op sql.Token, a, b bool) (val bool) {
	switch op {
	case sql.EQ:
		return a == b
	case sql.NEQ:
		return a != b
	}
	return negOp(op)
}

func chkString(op sql.Token, a, b string) (val bool) {
	switch op {
	case sql.EQ:
		return a == b
	case sql.NEQ:
		return a != b
	case sql.LT:
		return a < b
	case sql.LTE:
		return a <= b
	case sql.GT:
		return a > b
	case sql.GTE:
		return a >= b
	case sql.INS:
		return strings.Contains(b, a) == true
	case sql.NIS:
		return strings.Contains(b, a) == false
	case sql.SIN:
		return strings.Contains(a, b) == true
	case sql.SNI:
		return strings.Contains(a, b) == false
	case sql.MAT:
		b, e := search.New(language.Und, search.Loose).IndexString(a, b)
		return b != -1 && e != -1
	case sql.NAT:
		b, e := search.New(language.Und, search.Loose).IndexString(a, b)
		return b == -1 && e == -1
	case sql.MAY:
		b, e := search.New(language.Und, search.Loose).IndexString(a, b)
		return b != -1 && e != -1
	}
	return negOp(op)
}

func chkInt(op sql.Token, a, b int64) (val bool) {
	switch op {
	case sql.EQ:
		return a == b
	case sql.NEQ:
		return a != b
	case sql.LT:
		return a < b
	case sql.LTE:
		return a <= b
	case sql.GT:
		return a > b
	case sql.GTE:
		return a >= b
	}
	return negOp(op)
}

func chkFloat(op sql.Token, a, b float64) (val bool) {
	switch op {
	case sql.EQ:
		return a == b
	case sql.NEQ:
		return a != b
	case sql.LT:
		return a < b
	case sql.LTE:
		return a <= b
	case sql.GT:
		return a > b
	case sql.GTE:
		return a >= b
	}
	return negOp(op)
}

func chkThing(op sql.Token, a, b *sql.Thing) (val bool) {
	switch op {
	case sql.EQ:
		return a.TB == b.TB && a.ID == b.ID
	case sql.NEQ:
		return a.TB != b.TB || a.ID != b.ID
	}
	return negOp(op)
}

func chkRegex(op sql.Token, a string, r *regexp.Regexp) (val bool) {
	switch op {
	case sql.EQ:
		return r.MatchString(a) == true
	case sql.NEQ:
		return r.MatchString(a) == false
	case sql.ANY:
		return r.MatchString(a) == true
	}
	return negOp(op)
}

func chkObject(op sql.Token, m map[string]interface{}, i interface{}) (val bool) {
	switch op {
	case sql.EQ:
		switch i.(type) {
		case *sql.Empty:
			return len(m) == 0
		default:
			return reflect.TypeOf(m) == reflect.TypeOf(i) && reflect.DeepEqual(m, i) == true
		}
	case sql.NEQ:
		switch i.(type) {
		case *sql.Empty:
			return len(m) != 0
		default:
			return reflect.TypeOf(m) != reflect.TypeOf(i) || reflect.DeepEqual(m, i) == false
		}
	}
	return negOp(op)
}

func chkArrayL(op sql.Token, a []interface{}, i interface{}) (val bool) {
	switch op {
	case sql.EQ:
		switch i.(type) {
		case *sql.Empty:
			return len(a) == 0
		default:
			return false
		}
	case sql.NEQ:
		switch i.(type) {
		case *sql.Empty:
			return len(a) != 0
		default:
			return true
		}
	case sql.SIN:
		switch i.(type) {
		case nil, *sql.Null:
			return data.Consume(a).Contains(nil) == true
		default:
			return data.Consume(a).Contains(i) == true
		}
	case sql.SNI:
		switch i.(type) {
		case nil, *sql.Null:
			return data.Consume(a).Contains(nil) == false
		default:
			return data.Consume(a).Contains(i) == false
		}
	case sql.MAT:
		switch s := i.(type) {
		case string:
			return chkSearch(op, a, s)
		}
	case sql.NAT:
		switch s := i.(type) {
		case string:
			return chkSearch(op, a, s)
		}
	case sql.MAY:
		switch s := i.(type) {
		case string:
			return chkSearch(op, a, s)
		}
	}
	return negOp(op)
}

func chkArrayR(op sql.Token, i interface{}, a []interface{}) (val bool) {
	switch op {
	case sql.EQ:
		switch i.(type) {
		case *sql.Empty:
			return len(a) == 0
		default:
			return false
		}
	case sql.NEQ:
		switch i.(type) {
		case *sql.Empty:
			return len(a) != 0
		default:
			return true
		}
	case sql.INS:
		switch i.(type) {
		case nil, *sql.Null:
			return data.Consume(a).Contains(nil) == true
		default:
			return data.Consume(a).Contains(i) == true
		}
	case sql.NIS:
		switch i.(type) {
		case nil, *sql.Null:
			return data.Consume(a).Contains(nil) == false
		default:
			return data.Consume(a).Contains(i) == false
		}
	case sql.MAT:
		switch s := i.(type) {
		case string:
			return chkSearch(op, a, s)
		}
	case sql.NAT:
		switch s := i.(type) {
		case string:
			return chkSearch(op, a, s)
		}
	case sql.MAY:
		switch s := i.(type) {
		case string:
			return chkSearch(op, a, s)
		}
	}
	return negOp(op)
}

func chkArray(op sql.Token, a []interface{}, b []interface{}) (val bool) {
	switch op {
	case sql.EQ:
		if reflect.TypeOf(a) == reflect.TypeOf(b) && reflect.DeepEqual(a, b) == true {
			return true
		}
	case sql.NEQ:
		if reflect.TypeOf(a) != reflect.TypeOf(b) || reflect.DeepEqual(a, b) == false {
			return true
		}
	case sql.SIN:
		return data.Consume(a).Contains(b) == true
	case sql.SNI:
		return data.Consume(a).Contains(b) == false
	case sql.INS:
		return data.Consume(b).Contains(a) == true
	case sql.NIS:
		return data.Consume(b).Contains(a) == false
	case sql.CONTAINSALL:
		for _, v := range b {
			if data.Consume(a).Contains(v) == false {
				return false
			}
		}
		return true
	case sql.CONTAINSSOME:
		for _, v := range b {
			if data.Consume(a).Contains(v) == true {
				return true
			}
		}
		return false
	case sql.CONTAINSNONE:
		for _, v := range b {
			if data.Consume(a).Contains(v) == true {
				return false
			}
		}
		return true
	case sql.ALLCONTAINEDIN:
		for _, v := range a {
			if data.Consume(b).Contains(v) == false {
				return false
			}
		}
		return true
	case sql.SOMECONTAINEDIN:
		for _, v := range a {
			if data.Consume(b).Contains(v) == true {
				return true
			}
		}
		return false
	case sql.NONECONTAINEDIN:
		for _, v := range a {
			if data.Consume(b).Contains(v) == true {
				return false
			}
		}
		return true
	}
	return
}

func chkMatch(op sql.Token, a []interface{}, r *regexp.Regexp) (val bool) {

	if len(a) == 0 {
		return op == sql.NEQ
	}

	for _, v := range a {

		var s string

		switch c := v.(type) {
		default:
			return false
		case string:
			s = c
		case bool:
			s = strconv.FormatBool(c)
		case int64:
			s = strconv.FormatInt(c, 10)
		case float64:
			s = strconv.FormatFloat(c, 'g', -1, 64)
		case time.Time:
			s = c.String()
		}

		if op == sql.EQ {
			if chkRegex(sql.EQ, s, r) == false {
				return false
			}
		}

		if op == sql.NEQ {
			if chkRegex(sql.EQ, s, r) == false {
				return true
			}
		}

		if op == sql.ANY {
			if chkRegex(sql.EQ, s, r) == true {
				return true
			}
		}

	}

	switch op {
	case sql.EQ:
		return true
	case sql.NEQ:
		return false
	case sql.ANY:
		return false
	}

	return

}

func chkSearch(op sql.Token, a []interface{}, r string) (val bool) {

	if len(a) == 0 {
		return op == sql.NAT
	}

	for _, v := range a {

		var s string

		switch c := v.(type) {
		default:
			return false
		case string:
			s = c
		case bool:
			s = strconv.FormatBool(c)
		case int64:
			s = strconv.FormatInt(c, 10)
		case float64:
			s = strconv.FormatFloat(c, 'g', -1, 64)
		case time.Time:
			s = c.String()
		}

		if op == sql.MAT {
			b, e := search.New(language.Und, search.Loose).IndexString(s, r)
			if b == -1 && e == -1 {
				return false
			}
		}

		if op == sql.NAT {
			b, e := search.New(language.Und, search.Loose).IndexString(s, r)
			if b == -1 && e == -1 {
				return true
			}
		}

		if op == sql.MAY {
			b, e := search.New(language.Und, search.Loose).IndexString(s, r)
			if b != -1 && e != -1 {
				return true
			}
		}

	}

	switch op {
	case sql.MAT:
		return true
	case sql.NAT:
		return false
	case sql.MAY:
		return false
	}

	return

}
