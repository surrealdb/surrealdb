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
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"time"

	// "github.com/abcum/surreal/cnf"
	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
	"github.com/abcum/surreal/util/deep"
	"github.com/abcum/surreal/util/fncs"
	// "github.com/abcum/surreal/util/keys"
)

var ign = data.New()

func (e *executor) fetch(ctx context.Context, val interface{}, doc *data.Doc) (out interface{}, err error) {

	switch val := val.(type) {
	default:
		return val, nil
	case *sql.Thing:
		return val, nil
	case *sql.Value:
		return val.ID, nil
	case []byte:
		return string(val), nil
	case []interface{}:
		// TODO do we really need to copy?
		return deep.Copy(val), nil
	case map[string]interface{}:
		// TODO do we really need to copy?
		return deep.Copy(val), nil

	// case *sql.Thing:

	// 	if doc == nil {
	// 		return val, nil
	// 	}

	// 	s := &sql.SelectStatement{
	// 		KV: cnf.Settings.DB.Base, NS: "test", DB: "test",
	// 		Expr: []*sql.Field{{Expr: &sql.All{}, Field: "*"}},
	// 		What: []sql.Expr{val},
	// 	}
	// 	i := newIterator(e, ctx, s, false)
	// 	key := &keys.Thing{KV: s.KV, NS: s.NS, DB: s.DB, TB: val.TB, ID: val.ID}
	// 	i.processThing(ctx, key)
	// 	res, err := i.Yield(ctx)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	if len(res) > 0 {
	// 		return res[0], nil
	// 	}
	// 	return val, nil

	case *sql.Ident:

		switch {
		case doc == ign:
			return val, queryIdentFailed
		case doc != nil:
			res := doc.Get(val.ID).Data()
			return e.fetch(ctx, res, doc)
		default:
			return val, nil
		}

	case *sql.Param:

		if obj, ok := ctx.Value(ctxKeySubs).(*data.Doc); ok {
			if res := obj.Get(val.ID).Data(); res != nil {
				return e.fetch(ctx, res, doc)
			}
		}
		if obj, ok := ctx.Value(ctxKeyVars).(*data.Doc); ok {
			if res := obj.Get(val.ID).Data(); res != nil {
				return e.fetch(ctx, res, doc)
			}
		}
		return nil, nil

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

	case *sql.PathExpression:

		for _, v := range val.Expr {
			fmt.Printf("%T %v\n", v, v)
			switch v := v.(type) {
			case *sql.JoinExpression:
				switch v.Join {
				case sql.DOT:
				case sql.OEDGE:
				case sql.IEDGE:
				case sql.BEDGE:
				}
			case *sql.PartExpression:

				switch v.Part.(type) {
				case *sql.Thing:
				default:
				}

				fmt.Printf("  %T %v\n", v.Part, v.Part)
			}
		}

	case *sql.BinaryExpression:

		l, err := e.fetch(ctx, val.LHS, doc)
		if err != nil {
			return nil, err
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
		case sql.EQ, sql.NEQ, sql.ANY, sql.LT, sql.LTE, sql.GT, sql.GTE, sql.SIN, sql.SNI, sql.INS, sql.NIS:
			return binaryCheck(val.Op, l, r, val.LHS, val.RHS, doc), nil
		}

	}

	return nil, nil

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

	if d != nil {

		switch l := lo.(type) {

		case *sql.Void:

			switch r.(type) {
			case nil:
				return op == sql.NEQ
			}

		case *sql.Ident:

			switch r.(type) {

			case *sql.Void:
				if op == sql.EQ {
					return d.Exists(l.ID) == false
				} else if op == sql.NEQ {
					return d.Exists(l.ID) == true
				}

			case nil:
				if op == sql.EQ {
					return d.Exists(l.ID) == true && d.Get(l.ID).Data() == nil
				} else if op == sql.NEQ {
					return d.Exists(l.ID) == false || d.Get(l.ID).Data() != nil
				}

			case *sql.Empty:
				if op == sql.EQ {
					return d.Exists(l.ID) == false || d.Get(l.ID).Data() == nil
				} else if op == sql.NEQ {
					return d.Exists(l.ID) == true && d.Get(l.ID).Data() != nil
				}

			}

		}

		switch r := ro.(type) {

		case *sql.Void:

			switch l.(type) {
			case nil:
				return op == sql.NEQ
			}

		case *sql.Ident:

			switch l.(type) {

			case *sql.Void:
				if op == sql.EQ {
					return d.Exists(r.ID) == false
				} else if op == sql.NEQ {
					return d.Exists(r.ID) == true
				}

			case nil:
				if op == sql.EQ {
					return d.Exists(r.ID) == true && d.Get(r.ID).Data() == nil
				} else if op == sql.NEQ {
					return d.Exists(r.ID) == false || d.Get(r.ID).Data() != nil
				}

			case *sql.Empty:
				if op == sql.EQ {
					return d.Exists(r.ID) == false || d.Get(r.ID).Data() == nil
				} else if op == sql.NEQ {
					return d.Exists(r.ID) == true && d.Get(r.ID).Data() != nil
				}

			}

		}

	}

	switch l := l.(type) {

	case nil:
		switch r := r.(type) {
		default:
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
		case nil:
			return op == sql.EQ
		case []interface{}:
			return chkArrayR(op, l, r)
		case map[string]interface{}:
			return chkObject(op, r, l)
		}

	case *sql.Thing:
		switch r := r.(type) {
		default:
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
		case *sql.Thing:
			return chkThing(op, l, r)
		case string:
			return chkString(op, r, l.String())
		case []interface{}:
			return chkArrayR(op, l, r)
		}

	case bool:
		switch r := r.(type) {
		default:
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
		case bool:
			return chkBool(op, l, r)
		case string:
			if b, err := strconv.ParseBool(r); err == nil {
				return chkBool(op, l, b)
			}
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
		case *regexp.Regexp:
			return chkRegex(op, strconv.FormatBool(l), r)
		case []interface{}:
			return chkArrayR(op, l, r)
		case map[string]interface{}:
			return chkObject(op, r, l)
		}

	case string:
		switch r := r.(type) {
		default:
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
		case bool:
			if b, err := strconv.ParseBool(l); err == nil {
				return chkBool(op, r, b)
			}
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
		case string:
			return chkString(op, l, r)
		case int64:
			if n, err := strconv.ParseInt(l, 10, 64); err == nil {
				return chkInt(op, r, n)
			}
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
		case float64:
			if n, err := strconv.ParseFloat(l, 64); err == nil {
				return chkFloat(op, r, n)
			}
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
		case time.Time:
			return chkString(op, l, r.String())
		case *sql.Thing:
			return chkString(op, l, r.String())
		case *regexp.Regexp:
			return chkRegex(op, l, r)
		case []interface{}:
			return chkArrayR(op, l, r)
		case map[string]interface{}:
			return chkObject(op, r, l)
		}

	case int64:
		switch r := r.(type) {
		default:
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
		case string:
			if n, err := strconv.ParseInt(r, 10, 64); err == nil {
				return chkInt(op, l, n)
			}
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
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
		case map[string]interface{}:
			return chkObject(op, r, l)
		}

	case float64:
		switch r := r.(type) {
		default:
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
		case string:
			if n, err := strconv.ParseFloat(r, 64); err == nil {
				return chkFloat(op, l, n)
			}
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
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
		case map[string]interface{}:
			return chkObject(op, r, l)
		}

	case time.Time:
		switch r := r.(type) {
		default:
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
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
		case map[string]interface{}:
			return chkObject(op, r, l)
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
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
		case []interface{}:
			return chkArrayR(op, l, r)
		case map[string]interface{}:
			return chkObject(op, l, r)
		}

	}

	return nil

}

func chkVoid(op sql.Token, a, b bool) (val bool) {
	return
}

func chkNull(op sql.Token, a, b bool) (val bool) {
	return
}

func chkBool(op sql.Token, a, b bool) (val bool) {
	switch op {
	case sql.EQ:
		return a == b
	case sql.NEQ:
		return a != b
	case sql.SNI:
		return true
	case sql.NIS:
		return true
	case sql.CONTAINSNONE:
		return true
	}
	return
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
	case sql.SNI:
		return true
	case sql.NIS:
		return true
	case sql.CONTAINSNONE:
		return true
	}
	return
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
	case sql.SNI:
		return true
	case sql.NIS:
		return true
	case sql.CONTAINSNONE:
		return true
	}
	return
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
	case sql.SNI:
		return true
	case sql.NIS:
		return true
	case sql.CONTAINSNONE:
		return true
	}
	return
}

func chkThing(op sql.Token, a, b *sql.Thing) (val bool) {
	switch op {
	case sql.EQ:
		return a.TB == b.TB && a.ID == b.ID
	case sql.NEQ:
		return a.TB != b.TB || a.ID != b.ID
	case sql.SNI:
		return true
	case sql.NIS:
		return true
	case sql.CONTAINSNONE:
		return true
	}
	return
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
	return
}

func chkObject(op sql.Token, m map[string]interface{}, i interface{}) (val bool) {
	switch op {
	case sql.EQ:
		if reflect.TypeOf(m) == reflect.TypeOf(i) && reflect.DeepEqual(m, i) == true {
			return true
		}
	case sql.NEQ:
		if reflect.TypeOf(m) != reflect.TypeOf(i) || reflect.DeepEqual(m, i) == false {
			return true
		}
	case sql.SNI:
		return true
	case sql.NIS:
		return true
	case sql.CONTAINSNONE:
		return true
	}
	return
}

func chkArrayL(op sql.Token, a []interface{}, i interface{}) (val bool) {
	switch op {
	case sql.EQ:
		return false
	case sql.NEQ:
		return true
	case sql.SIN:
		if i == nil {
			return data.Consume(a).Contains(nil) == true
		} else {
			return data.Consume(a).Contains(i) == true
		}
	case sql.SNI:
		if i == nil {
			return data.Consume(a).Contains(nil) == false
		} else {
			return data.Consume(a).Contains(i) == false
		}
	case sql.INS:
		return false
	case sql.NIS:
		return true
	case sql.CONTAINSNONE:
		return true
	}
	return
}

func chkArrayR(op sql.Token, i interface{}, a []interface{}) (val bool) {
	switch op {
	case sql.EQ:
		return false
	case sql.NEQ:
		return true
	case sql.SIN:
		return false
	case sql.SNI:
		return true
	case sql.INS:
		if i == nil {
			return data.Consume(a).Contains(nil) == true
		} else {
			return data.Consume(a).Contains(i) == true
		}
	case sql.NIS:
		if i == nil {
			return data.Consume(a).Contains(nil) == false
		} else {
			return data.Consume(a).Contains(i) == false
		}
	case sql.CONTAINSNONE:
		return true
	}
	return
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
	}
	return
}

func chkMatch(op sql.Token, a []interface{}, r *regexp.Regexp) (val bool) {

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
			if chkRegex(sql.EQ, s, r) == true {
				return false
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
		return true
	case sql.ANY:
		return false
	}

	return

}
