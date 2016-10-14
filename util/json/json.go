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

package json

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

type Operation struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value,omitempty"`
}

type ByPath []Operation

func (a ByPath) Len() int           { return len(a) }
func (a ByPath) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByPath) Less(i, j int) bool { return a[i].Path < a[j].Path }

func NewPatch(op, path string, value interface{}) Operation {
	return Operation{Op: op, Path: path, Value: value}
}

// CreatePatch creates a patch as specified in http://jsonpatch.com/
//
// 'a' is original, 'b' is the modified document. Both are to be given as json encoded content.
// The function will return an array of Operations
//
// An error will be returned if any of the two documents are invalid.
func Diff(a, b interface{}) ([]Operation, error) {

	va, oka := a.([]byte)
	vb, okb := b.([]byte)

	if oka && okb {
		ia := map[string]interface{}{}
		ib := map[string]interface{}{}
		err := json.Unmarshal(va, &ia)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(vb, &ib)
		if err != nil {
			return nil, err
		}
		return diff(ia, ib, "", []Operation{})
	}

	ma, oka := a.(map[string]interface{})
	mb, okb := b.(map[string]interface{})

	if oka && okb {
		return diff(ma, mb, "", []Operation{})
	}

	return nil, fmt.Errorf("Invalid input format")

}

// Returns true if the values matches (must be json types)
// The types of the values must match, otherwise it will always return false
// If two map[string]interface{} are given, all elements must match.
func matchesValue(av, bv interface{}) bool {
	if reflect.TypeOf(av) != reflect.TypeOf(bv) {
		return false
	}
	switch at := av.(type) {
	case string:
		bt := bv.(string)
		if bt == at {
			return true
		}
	case float64:
		bt := bv.(float64)
		if bt == at {
			return true
		}
	case bool:
		bt := bv.(bool)
		if bt == at {
			return true
		}
	case map[string]interface{}:
		bt := bv.(map[string]interface{})
		for key := range at {
			if !matchesValue(at[key], bt[key]) {
				return false
			}
		}
		for key := range bt {
			if !matchesValue(at[key], bt[key]) {
				return false
			}
		}
		return true
	case []interface{}:
		bt := bv.([]interface{})
		if len(bt) != len(at) {
			return false
		}
		for key := range at {
			if !matchesValue(at[key], bt[key]) {
				return false
			}
		}
		for key := range bt {
			if !matchesValue(at[key], bt[key]) {
				return false
			}
		}
		return true
	}
	return false
}

func makePath(path string, newPart interface{}) string {
	if path == "" {
		return fmt.Sprintf("/%v", newPart)
	} else {
		if strings.HasSuffix(path, "/") {
			path = path + fmt.Sprintf("%v", newPart)
		} else {
			path = path + fmt.Sprintf("/%v", newPart)
		}
	}
	return path
}

// diff returns the (recursive) difference between a and b as an array of Operations.
func diff(a, b map[string]interface{}, path string, patch []Operation) ([]Operation, error) {
	for key, bv := range b {
		p := makePath(path, key)
		av, ok := a[key]
		// value was added
		if !ok {
			patch = append(patch, NewPatch("add", p, bv))
			continue
		}
		// If types have changed, replace completely
		if reflect.TypeOf(av) != reflect.TypeOf(bv) {
			patch = append(patch, NewPatch("replace", p, bv))
			continue
		}
		// Types are the same, compare values
		var err error
		patch, err = handleValues(av, bv, p, patch)
		if err != nil {
			return nil, err
		}
	}
	// Now add all deleted values as nil
	for key := range a {
		_, found := b[key]
		if !found {
			p := makePath(path, key)

			patch = append(patch, NewPatch("remove", p, nil))
		}
	}
	return patch, nil
}

func handleValues(av, bv interface{}, p string, patch []Operation) ([]Operation, error) {
	var err error
	switch at := av.(type) {
	case map[string]interface{}:
		bt := bv.(map[string]interface{})
		patch, err = diff(at, bt, p, patch)
		if err != nil {
			return nil, err
		}
	case string, float64, bool:
		if !matchesValue(av, bv) {
			patch = append(patch, NewPatch("replace", p, bv))
		}
	case []interface{}:
		bt := bv.([]interface{})
		if len(at) != len(bt) {
			// arrays are not the same
			patch = append(patch, compareArray(at, bt, p)...)

		} else {
			for i, _ := range bt {
				patch, err = handleValues(at[i], bt[i], makePath(p, i), patch)
				if err != nil {
					return nil, err
				}
			}
		}
	case nil:
		switch bv.(type) {
		case nil:
			// Both nil, fine.
		default:
			patch = append(patch, NewPatch("add", p, bv))
		}
	default:
		panic(fmt.Sprintf("Unknown type:%T ", av))
	}
	return patch, nil
}

func compareArray(av, bv []interface{}, p string) []Operation {
	retval := []Operation{}
	//	var err error
	for i, v := range av {
		found := false
		for _, v2 := range bv {
			if reflect.DeepEqual(v, v2) {
				found = true
			}
		}
		if !found {
			retval = append(retval, NewPatch("remove", makePath(p, i), nil))
		}
	}

	for i, v := range bv {
		found := false
		for _, v2 := range av {
			if reflect.DeepEqual(v, v2) {
				found = true
			}
		}
		if !found {
			retval = append(retval, NewPatch("add", makePath(p, i), v))
		}
	}

	return retval
}
