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

package item

import (
	"reflect"
	"regexp"
	"strconv"
	"time"

	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
)

func (this *Doc) Check(cond sql.Expr) (val bool) {

	switch expr := cond.(type) {
	case *sql.BinaryExpression:
		if !this.chkOne(expr) {
			return false
		}
	}

	return true

}

func (this *Doc) chkOne(expr *sql.BinaryExpression) (val bool) {

	op := expr.Op
	lhs := this.getChk(expr.LHS)
	rhs := this.getChk(expr.RHS)

	switch lhs.(type) {
	case bool, string, int64, float64, time.Time:
		switch rhs.(type) {
		case bool, string, int64, float64, time.Time:

			if op == sql.EEQ {
				return lhs == rhs
			}

			if op == sql.NEE {
				return lhs != rhs
			}

			if op == sql.EQ && lhs == rhs {
				return true
			}

			if op == sql.NEQ && lhs == rhs {
				return false
			}

		}
	}

	switch l := expr.LHS.(type) {

	case *sql.Ident:

		switch r := expr.RHS.(type) {

		case *sql.Void:
			if op == sql.EQ {
				return this.current.Exists(l.ID) == false
			} else if op == sql.NEQ {
				return this.current.Exists(l.ID) == true
			}

		case *sql.Null:
			if op == sql.EQ {
				return this.current.Exists(l.ID) == true && this.current.Get(l.ID).Data() == nil
			} else if op == sql.NEQ {
				return this.current.Exists(l.ID) == true && this.current.Get(l.ID).Data() != nil
			}

		case *sql.Empty:
			if op == sql.EQ {
				return this.current.Exists(l.ID) == false || this.current.Get(l.ID).Data() == nil
			} else if op == sql.NEQ {
				return this.current.Exists(l.ID) == true && this.current.Get(l.ID).Data() != nil
			}

		case *sql.Thing:
			if thing, ok := this.current.Get(l.ID).Data().(*sql.Thing); ok {
				if op == sql.EQ {
					return thing.TB == r.TB && thing.ID == r.ID
				} else if op == sql.NEQ {
					return thing.TB != r.TB || thing.ID != r.ID
				}
			}

		}

	}

	switch r := expr.RHS.(type) {

	case *sql.Ident:

		switch l := expr.LHS.(type) {

		case *sql.Void:
			if op == sql.EQ {
				return this.current.Exists(r.ID) == false
			} else if op == sql.NEQ {
				return this.current.Exists(r.ID) == true
			}

		case *sql.Null:
			if op == sql.EQ {
				return this.current.Exists(r.ID) == true && this.current.Get(r.ID).Data() == nil
			} else if op == sql.NEQ {
				return this.current.Exists(r.ID) == true && this.current.Get(r.ID).Data() != nil
			}

		case *sql.Empty:
			if op == sql.EQ {
				return this.current.Exists(r.ID) == false || this.current.Get(r.ID).Data() == nil
			} else if op == sql.NEQ {
				return this.current.Exists(r.ID) == true && this.current.Get(r.ID).Data() != nil
			}

		case *sql.Thing:
			if thing, ok := this.current.Get(r.ID).Data().(*sql.Thing); ok {
				if op == sql.EQ {
					return thing.TB == l.TB && thing.ID == l.ID
				} else if op == sql.NEQ {
					return thing.TB != l.TB || thing.ID != l.ID
				}
			}

		}

	}

	switch l := lhs.(type) {

	case nil:
		switch r := rhs.(type) {
		default:
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
		case nil:
			return op == sql.EQ
		case []interface{}:
			return chkArrayR(op, l, r)
		case map[string]interface{}:
			return chkObject(op, r, l)
		}

	case *sql.Void:
		switch r := rhs.(type) {
		default:
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
		case *sql.Void:
			return op == sql.EQ
		case *sql.Empty:
			return op == sql.EQ
		case []interface{}:
			return chkArrayR(op, l, r)
		case map[string]interface{}:
			return chkObject(op, r, l)
		}

	case *sql.Null:
		switch r := rhs.(type) {
		default:
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
		case *sql.Null:
			return op == sql.EQ
		case *sql.Empty:
			return op == sql.EQ
		case []interface{}:
			return chkArrayR(op, l, r)
		case map[string]interface{}:
			return chkObject(op, r, l)
		}

	case *sql.Empty:
		switch r := rhs.(type) {
		default:
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
		case *sql.Null:
			return op == sql.EQ
		case *sql.Void:
			return op == sql.EQ
		case *sql.Empty:
			return op == sql.EQ
		case []interface{}:
			return chkArrayR(op, l, r)
		case map[string]interface{}:
			return chkObject(op, r, l)
		}

	case *sql.Thing:
		switch r := rhs.(type) {
		default:
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
		case *sql.Thing:
			return chkThing(op, l, r)
		case string:
			return chkString(op, r, l.String())
		}

	case bool:
		switch r := rhs.(type) {
		default:
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
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
		case map[string]interface{}:
			return chkObject(op, r, l)
		}

	case string:
		switch r := rhs.(type) {
		default:
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
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
		switch r := rhs.(type) {
		default:
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
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
		case map[string]interface{}:
			return chkObject(op, r, l)
		}

	case float64:
		switch r := rhs.(type) {
		default:
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
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
		case map[string]interface{}:
			return chkObject(op, r, l)
		}

	case time.Time:
		switch r := rhs.(type) {
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
		switch r := rhs.(type) {
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
		switch r := rhs.(type) {
		default:
			return op == sql.NEQ || op == sql.SNI || op == sql.NIS || op == sql.CONTAINSNONE
		case []interface{}:
			return chkArrayR(op, l, r)
		case map[string]interface{}:
			return chkObject(op, l, r)
		}

	}

	return

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
		if _, ok := i.(*sql.Null); ok {
			return data.Consume(a).Contains(nil) == true
		} else {
			return data.Consume(a).Contains(i) == true
		}
	case sql.SNI:
		if _, ok := i.(*sql.Null); ok {
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
		if _, ok := i.(*sql.Null); ok {
			return data.Consume(a).Contains(nil) == true
		} else {
			return data.Consume(a).Contains(i) == true
		}
	case sql.NIS:
		if _, ok := i.(*sql.Null); ok {
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

func (this *Doc) getChk(expr sql.Expr) interface{} {

	switch val := expr.(type) {
	default:
		return nil
	case time.Time:
		return val
	case *regexp.Regexp:
		return val
	case bool, int64, float64, string:
		return val
	case []interface{}, map[string]interface{}:
		return val
	case *sql.Void:
		return val
	case *sql.Null:
		return val
	case *sql.Empty:
		return val
	case *sql.Param:
		return this.runtime.Get(val.ID).Data()
	case *sql.Ident:
		return this.current.Get(val.ID).Data()
	}

}
