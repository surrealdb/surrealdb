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

	"github.com/sergi/go-diff/diffmatchpatch"
)

type Operation struct {
	Op     string      `cork:"op,omietmpty" json:"op,omietmpty"`
	From   string      `cork:"from,omitempty" json:"from,omitempty"`
	Path   string      `cork:"path,omitempty" json:"path,omitempty"`
	Value  interface{} `cork:"value,omitempty" json:"value,omitempty"`
	Before interface{} `cork:"-" json:"-"`
}

type Operations struct {
	Ops []*Operation
}

func Diff(old, now map[string]interface{}) (ops *Operations) {

	ops = &Operations{}

	ops.diff(old, now, "")

	return

}

func (o *Operations) Patch(old map[string]interface{}) (now map[string]interface{}, err error) {
	return nil, nil
}

func (o *Operations) Rebase(other *Operations) (ops *Operations, err error) {
	return nil, nil
}

func (o *Operations) Out() (ops []map[string]interface{}) {

	for _, v := range o.Ops {

		op := make(map[string]interface{})

		if len(v.Op) > 0 {
			op["op"] = v.Op
		}

		if len(v.From) > 0 {
			op["from"] = v.From
		}

		if len(v.Path) > 0 {
			op["path"] = v.Path
		}

		if v.Value != nil {
			op["value"] = v.Value
		}

		ops = append(ops, op)

	}

	return

}

func route(path string, part string) string {
	if len(path) == 0 {
		return "/" + part
	} else {
		if path[0] == '/' {
			return path + part
		} else {
			return path + "/" + part
		}
	}
}

func (o *Operations) op(op, from, path string, before, after interface{}) {

	o.Ops = append(o.Ops, &Operation{
		Op:     op,
		From:   from,
		Path:   path,
		Value:  after,
		Before: before,
	})

}

func (o *Operations) diff(old, now map[string]interface{}, path string) {

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

	for i := len(o.Ops) - 1; i >= 0; i-- {
		if iv := o.Ops[i]; !isIn(i, used) && iv.Op == "add" {
			for j := len(o.Ops) - 1; j >= 0; j-- {
				if jv := o.Ops[j]; !isIn(j, used) && jv.Op == "remove" {
					if reflect.DeepEqual(iv.Value, jv.Before) {
						used = append(used, []int{i, j}...)
						o.op("move", jv.Path, iv.Path, nil, nil)
					}
				}
			}
		}
	}

	sort.Sort(sort.Reverse(sort.IntSlice(used)))

	for _, i := range used {
		o.Ops = append(o.Ops[:i], o.Ops[i+1:]...)
	}

}

func isIn(a int, list []int) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func (o *Operations) text(old, now string, path string) {

	dmp := diffmatchpatch.New()

	dif := dmp.DiffMain(old, now, false)

	txt := dmp.DiffToDelta(dif)

	o.op("change", "", path, old, txt)

}

func (o *Operations) vals(old, now interface{}, path string) {

	if reflect.TypeOf(old) != reflect.TypeOf(now) {
		o.op("replace", "", path, old, now)
		return
	}

	switch ov := old.(type) {
	default:
		if !reflect.DeepEqual(old, now) {
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

func (o *Operations) arrs(old, now []interface{}, path string) {

	var i int

	for i = 0; i < len(old) && i < len(now); i++ {
		o.vals(old[i], now[i], strconv.Itoa(i))
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
