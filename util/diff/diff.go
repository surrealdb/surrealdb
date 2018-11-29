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

package diff

import (
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/abcum/surreal/sql"

	"github.com/abcum/surreal/util/data"

	"github.com/sergi/go-diff/diffmatchpatch"
)

type operations struct {
	ops []*operation
}

type operation struct {
	op     string
	from   string
	path   string
	value  interface{}
	before interface{}
}

func Diff(old, now map[string]interface{}) []interface{} {
	out := &operations{}
	out.diff(old, now, "")
	return out.diffs()
}

func Patch(old map[string]interface{}, ops []interface{}) map[string]interface{} {
	out := &operations{}
	out.load(ops)
	return out.patch(old)
}

func (o *operations) load(ops []interface{}) {

	for _, v := range ops {

		if obj, ok := v.(map[string]interface{}); ok {

			op := &operation{}

			op.value = obj["value"]

			if str, ok := obj["op"].(string); ok {
				op.op = str
			}

			if str, ok := obj["from"].(string); ok {
				op.from = str
			}

			if str, ok := obj["path"].(string); ok {
				op.path = str
			}

			o.ops = append(o.ops, op)

		}

	}

}

func (o *operations) diffs() (ops []interface{}) {

	ops = make([]interface{}, len(o.ops))

	sort.Slice(o.ops, func(i, j int) bool {
		return o.ops[i].path < o.ops[j].path
	})

	for k, v := range o.ops {

		op := make(map[string]interface{})

		if len(v.op) > 0 {
			op["op"] = v.op
		}

		if len(v.from) > 0 {
			op["from"] = v.from
		}

		if len(v.path) > 0 {
			op["path"] = v.path
		}

		if v.value != nil {
			op["value"] = v.value
		}

		ops[k] = op

	}

	return

}

func isIn(a int, list []int) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func route(path string, part string) string {
	if len(path) == 0 {
		return "/" + part
	} else {
		if part[0] == '/' {
			return path + part
		} else {
			return path + "/" + part
		}
	}
}

func (o *operations) op(op, from, path string, before, after interface{}) {

	o.ops = append(o.ops, &operation{
		op:     op,
		from:   from,
		path:   path,
		value:  after,
		before: before,
	})

}

func (o *operations) diff(old, now map[string]interface{}, path string) {

	for key, after := range now {

		p := route(path, key)

		// Check if the value existed
		before, ok := old[key]

		// Value did not previously exist
		if !ok {
			o.op("add", "", p, nil, after)
			continue
		}

		// Data type is now completely different
		if reflect.TypeOf(after) != reflect.TypeOf(before) {
			o.op("replace", "", p, before, after)
			continue
		}

		// Check whether the values have changed
		o.vals(before, after, p)

	}

	for key, before := range old {

		p := route(path, key)

		// Check if the value exists
		after, ok := now[key]

		// Value now no longer exists
		if !ok {
			o.op("remove", "", p, before, after)
			continue
		}

	}

	var used []int

	for i := len(o.ops) - 1; i >= 0; i-- {
		if iv := o.ops[i]; !isIn(i, used) && iv.op == "add" {
			for j := len(o.ops) - 1; j >= 0; j-- {
				if jv := o.ops[j]; !isIn(j, used) && jv.op == "remove" {
					if reflect.DeepEqual(iv.value, jv.before) {
						used = append(used, []int{i, j}...)
						o.op("move", jv.path, iv.path, nil, nil)
					}
				}
			}
		}
	}

	sort.Sort(sort.Reverse(sort.IntSlice(used)))

	for _, i := range used {
		o.ops = append(o.ops[:i], o.ops[i+1:]...)
	}

}

func (o *operations) patch(old map[string]interface{}) (now map[string]interface{}) {

	obj := data.Consume(old)

	for _, v := range o.ops {

		path := strings.Split(v.path, "/")

		prev := path[:len(path)-1]

		switch v.op {
		case "add":
			switch obj.Get(prev...).Data().(type) {
			case []interface{}:
				obj.Append(v.value, prev...)
			default:
				obj.Set(v.value, path...)
			}
		case "remove":
			obj.Del(path...)
		case "replace":
			obj.Set(v.value, path...)
		case "change":
			if txt, ok := obj.Get(path...).Data().(string); ok {
				dmp := diffmatchpatch.New()
				dif, _ := dmp.DiffFromDelta(txt, v.value.(string))
				str := dmp.DiffText2(dif)
				obj.Set(str, path...)
			}
		}

	}

	return old

}

func (o *operations) text(old, now string, path string) {

	dmp := diffmatchpatch.New()

	dif := dmp.DiffMain(old, now, false)

	txt := dmp.DiffToDelta(dif)

	o.op("change", "", path, old, txt)

}

func (o *operations) vals(old, now interface{}, path string) {

	if reflect.TypeOf(old) != reflect.TypeOf(now) {
		o.op("replace", "", path, old, now)
		return
	}

	switch ov := old.(type) {
	default:
		if !reflect.DeepEqual(old, now) {
			o.op("replace", "", path, old, now)
		}
	case *sql.Thing:
		nv := now.(*sql.Thing)
		if ov.TB != nv.TB || ov.ID != nv.ID {
			o.op("replace", "", path, old, now)
		}
	case bool:
		if ov != now.(bool) {
			o.op("replace", "", path, old, now)
		}
	case int64:
		if ov != now.(int64) {
			o.op("replace", "", path, old, now)
		}
	case float64:
		if ov != now.(float64) {
			o.op("replace", "", path, old, now)
		}
	case string:
		if ov != now.(string) {
			o.text(ov, now.(string), path)
		}
	case nil:
		switch now.(type) {
		case nil:
		default:
			o.op("replace", "", path, old, now)
		}
	case map[string]interface{}:
		o.diff(ov, now.(map[string]interface{}), path)
	case []interface{}:
		o.arrs(ov, now.([]interface{}), path)
	}

}

func (o *operations) arrs(old, now []interface{}, path string) {

	var i int

	for i = 0; i < len(old) && i < len(now); i++ {
		o.vals(old[i], now[i], route(path, strconv.Itoa(i)))
	}

	for j := i; j < len(now); j++ {
		if j >= len(old) || !reflect.DeepEqual(now[j], old[j]) {
			o.op("add", "", route(path, strconv.Itoa(j)), nil, now[j])
		}
	}

	for j := i; j < len(old); j++ {
		if j >= len(now) || !reflect.DeepEqual(old[j], now[j]) {
			o.op("remove", "", route(path, strconv.Itoa(j)), old[j], nil)
		}
	}

}
