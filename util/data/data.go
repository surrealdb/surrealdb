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

package data

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"encoding/json"

	"github.com/abcum/surreal/util/deep"
	"github.com/abcum/surreal/util/pack"
)

const (
	one int8 = iota
	many
)

const (
	choose int8 = iota
	remove
)

// Doc holds a reference to the core data object, or a selected path.
type Doc struct {
	data interface{}
	call Fetcher
}

// Fetcher is used when fetching values.
type Fetcher func(key string, val interface{}, path []string) interface{}

// Iterator is used when iterating over items.
type Iterator func(key string, val interface{}) error

// New creates a new data object.
func New() *Doc {
	return &Doc{data: map[string]interface{}{}}
}

// Consume converts a GO interface into a data object.
func Consume(input interface{}) *Doc {
	return &Doc{data: input}
}

// Consume converts a GO interface into a data object.
func ConsumeWithFetch(input interface{}, fetcher Fetcher) *Doc {
	return &Doc{data: input, call: fetcher}
}

// Data returns the internal data object as an interface.
func (d *Doc) Data() interface{} {
	return d.data
}

// Copy returns a duplicated copy of the internal data object.
func (d *Doc) Copy() *Doc {
	return &Doc{data: deep.Copy(d.data)}
}

// Fetch adds a function to be used for retrieving and converting values.
func (d *Doc) Fetch(fetcher Fetcher) {
	d.call = fetcher
}

// Encode encodes the data object to a byte slice.
func (d *Doc) Encode() (dst []byte) {
	dst = pack.Encode(d.data)
	return
}

// Decode decodes the byte slice into a data object.
func (d *Doc) Decode(src []byte) *Doc {
	pack.Decode(src, &d.data)
	return d
}

func (d *Doc) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Data())
}

func (d *Doc) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &d.data)
}

// --------------------------------------------------------------------------------

func (d *Doc) path(path ...string) (paths []string) {

	for _, p := range path {
		for j, i, o := 0, 0, false; i < len(p); i++ {
			switch {
			case i == len(p)-1:
				if len(p[j:]) > 0 {
					paths = append(paths, p[j:])
				}
			case p[i] == '.':
				if len(p[j:i]) > 0 {
					paths = append(paths, p[j:i])
				}
				j, i = i+1, i+0
			case p[i] == '[':
				if len(p[j:i]) > 0 {
					paths = append(paths, p[j:i])
				}
				j, i, o = i, i+1, true
			case p[i] == ']' && o:
				if len(p[j:i+1]) > 0 {
					paths = append(paths, p[j:i+1])
				}
				j, i, o = i+1, i+0, false
			}
		}
	}

	return

}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func trim(s string) string {
	if s[0] == '[' && s[len(s)-1] == ']' {
		return s[1 : len(s)-1]
	}
	return s
}

func (d *Doc) what(p string, a []interface{}, t int8) (o []interface{}, i []int, r int8) {

	p = trim(p)

	i = []int{}

	o = []interface{}{}

	// If there are no items in the
	// original array, then return
	// an empty array immediately.

	if len(a) == 0 {
		if strings.ContainsAny(p, ":*") {
			return o, i, many
		} else {
			return o, i, one
		}
	}

	// If the array index is a star
	// or a colon only, then return
	// the full array immediately

	if p == "*" || p == ":" {
		switch t {
		case choose:
			for k := range a {
				i = append(i, k)
			}
			return a, i, many
		case remove:
			for k := range a {
				i = append(i, k)
			}
			return o, i, many
		}
	}

	// Split the specified array index
	// by colons, so that we can get
	// the specified array items.

	c := strings.Count(p, ":")

	if c == 0 {

		switch p {
		case "0", "first":
			if t == choose {
				i = append(i, 0)
				o = append(o, a[0])
			} else {
				for k := range a[1:] {
					i = append(i, k)
				}
				o = append(o, a[1:]...)
			}
		case "$", "last":
			if t == choose {
				i = append(i, len(a)-1)
				o = append(o, a[len(a)-1])
			} else {
				for k := range a[:len(a)-1] {
					i = append(i, k)
				}
				o = append(o, a[:len(a)-1]...)
			}
		default:
			if z, e := strconv.Atoi(p); e == nil {
				if len(a) > z {
					switch t {
					case choose:
						i = append(i, z)
						o = append(o, a[z])
					case remove:
						for k := range append(a[:z], a[z+1:]...) {
							i = append(i, k)
						}
						o = append(o, append(a[:z], a[z+1:]...)...)
					}
				} else {
					switch t {
					case remove:
						for k := range a {
							i = append(i, k)
						}
						o = append(o, a[:]...)
					}
				}
			}
		}

		return o, i, one

	}

	if c == 1 {

		var e error
		var s, f int

		b := []int{0, len(a)}
		x := strings.Split(p, ":")

		for k := range x {
			switch x[k] {
			case "":
			case "0", "first":
				b[k] = 0
			case "$", "last":
				b[k] = len(a)
			default:
				if b[k], e = strconv.Atoi(x[k]); e != nil {
					return nil, nil, many
				}
			}
		}

		s = b[0]
		s = max(s, 0)
		s = min(s, len(a))

		f = b[1]
		f = max(f, 0)
		f = min(f, len(a))

		if t == choose {
			for k, v := range a[s:f] {
				i = append(i, k)
				o = append(o, v)
			}
		} else {
			for k, v := range append(a[:s], a[f+1:]...) {
				i = append(i, k)
				o = append(o, v)
			}
		}

		return o, i, many

	}

	return nil, nil, many

}

// --------------------------------------------------------------------------------

// Reset empties and resets the data at the specified path.
func (d *Doc) Reset(path ...string) (*Doc, error) {
	return d.Set(map[string]interface{}{}, path...)
}

// Valid checks whether the value at the specified path is nil.
func (d *Doc) Valid(path ...string) bool {
	if !d.Exists(path...) {
		return false
	}
	return d.Get(path...).Data() != nil
}

// Array sets the specified path to an array.
func (d *Doc) Array(path ...string) (*Doc, error) {
	return d.Set([]interface{}{}, path...)
}

// Object sets the specified path to an object.
func (d *Doc) Object(path ...string) (*Doc, error) {
	return d.Set(map[string]interface{}{}, path...)
}

// --------------------------------------------------------------------------------

// New sets the value at the specified path if it does not exist.
func (d *Doc) New(value interface{}, path ...string) (*Doc, error) {
	if !d.Exists(path...) {
		return d.Set(value, path...)
	}
	return d.Get(path...), nil
}

// Iff sets the value at the specified path if it is not nil, or deletes it.
func (d *Doc) Iff(value interface{}, path ...string) (*Doc, error) {
	if value != nil {
		return d.Set(value, path...)
	}
	return &Doc{data: nil}, d.Del(path...)
}

// Keys retrieves the object keys at the specified path.
func (d *Doc) Keys(path ...string) *Doc {

	path = d.path(path...)

	out := []interface{}{}

	if m, ok := d.Get(path...).Data().(map[string]interface{}); ok {
		for k := range m {
			out = append(out, k)
		}
	}

	return &Doc{data: out}

}

// Vals retrieves the object values at the specified path.
func (d *Doc) Vals(path ...string) *Doc {

	path = d.path(path...)

	out := []interface{}{}

	if m, ok := d.Get(path...).Data().(map[string]interface{}); ok {
		for _, v := range m {
			out = append(out, v)
		}
	}

	return &Doc{data: out}

}

// --------------------------------------------------------------------------------

// Exists checks whether the specified path exists.
func (d *Doc) Exists(path ...string) bool {

	path = d.path(path...)

	// If the value found at the current
	// path part is undefined, then just
	// return false immediately

	if d.data == nil {
		return false
	}

	// Define the temporary object so
	// that we can loop over and traverse
	// down the path parts of the data

	object := d.data

	// Loop over each part of the path
	// whilst detecting if the data at
	// the current path is an {} or []

	for k, p := range path {

		p = trim(p)

		// If the value found at the current
		// path part is an object, then move
		// to the next part of the path

		if m, ok := object.(map[string]interface{}); ok {
			if object, ok = m[p]; !ok {
				return false
			}
			continue
		}

		// If the value found at the current
		// path part is an array, then perform
		// the query on the specified items

		if a, ok := object.([]interface{}); ok {

			c, _, r := d.what(p, a, choose)

			if len(c) == 0 {
				return false
			}

			if r == one {
				if d.call != nil {
					c[0] = d.call(p, c[0], path[k+1:])
				}
				return ConsumeWithFetch(c[0], d.call).Exists(path[k+1:]...)
			}

			if r == many {
				for _, v := range c {
					if d.call != nil {
						v = d.call(p, v, path[k+1:])
					}
					if !ConsumeWithFetch(v, d.call).Exists(path[k+1:]...) {
						return false
					}
				}
				return true
			}

		}

		return false

	}

	return true

}

// --------------------------------------------------------------------------------

// Get gets the value or values at a specified path.
func (d *Doc) Get(path ...string) *Doc {

	path = d.path(path...)

	// If the value found at the current
	// path part is undefined, then just
	// return false immediately

	if d.data == nil {
		return &Doc{data: nil}
	}

	// Define the temporary object so
	// that we can loop over and traverse
	// down the path parts of the data

	object := d.data

	// Loop over each part of the path
	// whilst detecting if the data at
	// the current path is an {} or []

	for k, p := range path {

		p = trim(p)

		// If the value found at the current
		// path part is an object, then move
		// to the next part of the path

		if m, ok := object.(map[string]interface{}); ok {
			switch p {
			default:
				if d.call != nil {
					object = d.call(p, m[p], path[k+1:])
				} else {
					object = m[p]
				}
			case "*":
				object = m
			}
			continue
		}

		// If the value found at the current
		// path part is an array, then perform
		// the query on the specified items

		if a, ok := object.([]interface{}); ok {

			c, _, r := d.what(p, a, choose)

			if len(c) == 0 {
				return &Doc{data: nil}
			}

			if r == one {
				if d.call != nil {
					c[0] = d.call(p, c[0], path[k+1:])
				}
				return ConsumeWithFetch(c[0], d.call).Get(path[k+1:]...)
			}

			if r == many {
				out := []interface{}{}
				for _, v := range c {
					if d.call != nil {
						v = d.call(p, v, path[k+1:])
					}
					res := ConsumeWithFetch(v, d.call).Get(path[k+1:]...)
					out = append(out, res.data)
				}
				return &Doc{data: out}
			}

		}

		return &Doc{data: nil}

	}

	return &Doc{data: object}

}

// --------------------------------------------------------------------------------

// Set sets the value or values at a specified path.
func (d *Doc) Set(value interface{}, path ...string) (*Doc, error) {

	path = d.path(path...)

	if len(path) == 0 {
		d.data = value
		return d, nil
	}

	// If the value found at the current
	// path part is undefined, then ensure
	// that it is an object

	if d.data == nil {
		d.data = map[string]interface{}{}
	}

	// Define the temporary object so
	// that we can loop over and traverse
	// down the path parts of the data

	object := d.data

	// Loop over each part of the path
	// whilst detecting if the data at
	// the current path is an {} or []

	for k, p := range path {

		p = trim(p)

		// If the value found at the current
		// path part is an object, then move
		// to the next part of the path

		if m, ok := object.(map[string]interface{}); ok {
			if k == len(path)-1 {
				m[p] = value
			} else if m[p] == nil {
				m[p] = map[string]interface{}{}
			}
			object = m[p]
			continue
		}

		// If the value found at the current
		// path part is an array, then perform
		// the query on the specified items

		if a, ok := object.([]interface{}); ok {

			c, i, r := d.what(p, a, choose)

			if len(c) == 0 {
				return &Doc{data: nil}, nil
			}

			if r == one {
				if k == len(path)-1 {
					a[i[0]] = value
					object = a[i[0]]
					continue
				} else {
					return ConsumeWithFetch(a[i[0]], d.call).Set(value, path[k+1:]...)
				}
			}

			if r == many {
				out := []interface{}{}
				for j, v := range c {
					if k == len(path)-1 {
						a[i[j]] = value
						out = append(out, value)
					} else {
						res, _ := ConsumeWithFetch(v, d.call).Set(value, path[k+1:]...)
						if res.data != nil {
							out = append(out, res.data)
						}
					}
				}
				return &Doc{data: out}, nil
			}

		}

	}

	return &Doc{data: object}, nil

}

// --------------------------------------------------------------------------------

// Del deletes the value or values at a specified path.
func (d *Doc) Del(path ...string) error {

	path = d.path(path...)

	// If the value found at the current
	// path part is undefined, then return
	// a not an object error

	if d.data == nil {
		return fmt.Errorf("Item is not an object")
	}

	// Define the temporary object so
	// that we can loop over and traverse
	// down the path parts of the data

	object := d.data

	// Loop over each part of the path
	// whilst detecting if the data at
	// the current path is an {} or []

	for k, p := range path {

		p = trim(p)

		// If the value found at the current
		// path part is an object, then move
		// to the next part of the path

		if m, ok := object.(map[string]interface{}); ok {
			if k == len(path)-1 {
				delete(m, p)
			} else if m[p] == nil {
				return fmt.Errorf("Item at path %s is not an object", path)
			}
			object = m[p]
			continue
		}

		// If the value found at the current
		// path part is an array, then perform
		// the query on the specified items

		if a, ok := object.([]interface{}); ok {

			var r int8
			var c []interface{}

			if k == len(path)-1 {
				c, _, r = d.what(p, a, remove)
			} else {
				c, _, r = d.what(p, a, choose)
			}

			if r == one {
				if k == len(path)-1 {
					d.Set(c, path[:len(path)-1]...)
					continue
				} else {
					if len(c) != 0 {
						return ConsumeWithFetch(c[0], d.call).Del(path[k+1:]...)
					}
				}
			}

			if r == many {
				if k == len(path)-1 {
					d.Set(c, path[:len(path)-1]...)
					continue
				} else {
					for _, v := range c {
						ConsumeWithFetch(v, d.call).Del(path[k+1:]...)
					}
					break
				}
			}

		}

	}

	return nil

}

// --------------------------------------------------------------------------------

// Append appends an item or an array of items to an array at the specified path.
func (d *Doc) Append(value interface{}, path ...string) (*Doc, error) {

	a, ok := d.Get(path...).Data().([]interface{})
	if !ok {
		return &Doc{data: nil}, fmt.Errorf("Not an array")
	}

	if values, ok := value.([]interface{}); ok {
		for _, value := range values {
			a = append(a, value)
		}
	} else {
		a = append(a, value)
	}

	return d.Set(a, path...)

}

// ArrayAdd appends an item or an array of items to an array at the specified path.
func (d *Doc) ArrayAdd(value interface{}, path ...string) (*Doc, error) {

	a, ok := d.Get(path...).Data().([]interface{})
	if !ok {
		return &Doc{data: nil}, fmt.Errorf("Not an array")
	}

	if values, ok := value.([]interface{}); ok {
	outer:
		for _, value := range values {
			for _, v := range a {
				if reflect.DeepEqual(v, value) {
					continue outer
				}
			}
			a = append(a, value)
		}
	} else {
		for _, v := range a {
			if reflect.DeepEqual(v, value) {
				return ConsumeWithFetch(a, d.call), nil
			}
		}
		a = append(a, value)
	}

	return d.Set(a, path...)

}

// ArrayDel deletes an item or an array of items from an array at the specified path.
func (d *Doc) ArrayDel(value interface{}, path ...string) (*Doc, error) {

	a, ok := d.Get(path...).Data().([]interface{})
	if !ok {
		return &Doc{data: nil}, fmt.Errorf("Not an array")
	}

	if values, ok := value.([]interface{}); ok {
		for _, value := range values {
			for i := len(a) - 1; i >= 0; i-- {
				v := a[i]
				if reflect.DeepEqual(v, value) {
					copy(a[i:], a[i+1:])
					a[len(a)-1] = nil
					a = a[:len(a)-1]
				}
			}
		}
	} else {
		for i := len(a) - 1; i >= 0; i-- {
			v := a[i]
			if reflect.DeepEqual(v, value) {
				copy(a[i:], a[i+1:])
				a[len(a)-1] = nil
				a = a[:len(a)-1]
			}
		}
	}

	return d.Set(a, path...)

}

// --------------------------------------------------------------------------------

// Contains checks whether the value exists within the array at the specified path.
func (d *Doc) Contains(value interface{}, path ...string) bool {

	a, ok := d.Get(path...).Data().([]interface{})
	if !ok {
		return false
	}

	for _, v := range a {
		if reflect.DeepEqual(v, value) {
			return true
		}
	}

	return false

}

// --------------------------------------------------------------------------------

// Inc increments an item, or appends an item to an array at the specified path.
func (d *Doc) Inc(value interface{}, path ...string) (*Doc, error) {

	switch cur := d.Get(path...).Data().(type) {
	case nil:
		switch inc := value.(type) {
		case int64:
			return d.Set(0+inc, path...)
		case float64:
			return d.Set(0+inc, path...)
		default:
			return d.Set([]interface{}{value}, path...)
		}
	case int64:
		switch inc := value.(type) {
		case int64:
			return d.Set(cur+inc, path...)
		case float64:
			return d.Set(float64(cur)+inc, path...)
		}
	case float64:
		switch inc := value.(type) {
		case int64:
			return d.Set(cur+float64(inc), path...)
		case float64:
			return d.Set(cur+inc, path...)
		}
	case []interface{}:
		return d.ArrayAdd(value, path...)
	}

	return &Doc{data: nil}, fmt.Errorf("Not possible to increment.")

}

// Dec decrements an item, or removes an item from an array at the specified path.
func (d *Doc) Dec(value interface{}, path ...string) (*Doc, error) {

	switch cur := d.Get(path...).Data().(type) {
	case nil:
		switch inc := value.(type) {
		case int64:
			return d.Set(0-inc, path...)
		case float64:
			return d.Set(0-inc, path...)
		}
	case int64:
		switch inc := value.(type) {
		case int64:
			return d.Set(cur-inc, path...)
		case float64:
			return d.Set(float64(cur)-inc, path...)
		}
	case float64:
		switch inc := value.(type) {
		case int64:
			return d.Set(cur-float64(inc), path...)
		case float64:
			return d.Set(cur-inc, path...)
		}
	case []interface{}:
		return d.ArrayDel(value, path...)
	}

	return &Doc{data: nil}, fmt.Errorf("Not possible to decrement.")

}

// --------------------------------------------------------------------------------

func (d *Doc) Diff(n *Doc) map[string]interface{} {

	var initial = make(map[string]interface{})
	var current = make(map[string]interface{})
	var changes = make(map[string]interface{})

	d.Each(func(key string, val interface{}) error {
		initial[key] = val
		return nil
	})

	n.Each(func(key string, val interface{}) error {
		current[key] = val
		return nil
	})

	for k, v := range current {
		if o, ok := initial[k]; ok {
			if reflect.DeepEqual(o, v) {
				continue
			}
		}
		changes[k] = v
	}

	for k := range initial {
		if _, ok := current[k]; !ok {
			changes[k] = nil
		}
	}

	return changes

}

// --------------------------------------------------------------------------------

func (d *Doc) join(parts ...[]string) string {
	var path []string
	for _, part := range parts {
		path = append(path, part...)
	}
	return strings.Join(path, ".")
}

// --------------------------------------------------------------------------------

// Each loops through the values in the data doc.
func (d *Doc) Each(exec Iterator) error {

	return d.each(exec, nil)

}

func (d *Doc) each(exec Iterator, prev []string) error {

	// Define the temporary object so
	// that we can loop over and traverse
	// down the path parts of the data

	object := d.data

	// If the value found at the current
	// path part is an object, then move
	// to the next part of the path

	if m, ok := object.(map[string]interface{}); ok {
		exec(d.join(prev), make(map[string]interface{}))
		for k, v := range m {
			var keep []string
			keep = append(keep, prev...)
			keep = append(keep, k)
			ConsumeWithFetch(v, d.call).each(exec, keep)
		}
		return nil
	}

	// If the value found at the current
	// path part is an array, then perform
	// the query on the specified items

	if a, ok := object.([]interface{}); ok {
		exec(d.join(prev), make([]interface{}, len(a)))
		for i, v := range a {
			var keep []string
			keep = append(keep, prev...)
			keep = append(keep, fmt.Sprintf("[%d]", i))
			ConsumeWithFetch(v, d.call).each(exec, keep)
		}
		return nil
	}

	return exec(d.join(prev), object)

}

// --------------------------------------------------------------------------------

// Walk walks the value or values at a specified path.
func (d *Doc) Walk(exec Iterator, path ...string) error {

	path = d.path(path...)

	return d.walk(exec, nil, path...)

}

func (d *Doc) walk(exec Iterator, prev []string, path ...string) error {

	if len(path) == 0 {
		return nil
	}

	// If the value found at the current
	// path part is undefined, then ensure
	// that it is an object

	if d.data == nil {
		d.data = map[string]interface{}{}
	}

	// Define the temporary object so
	// that we can loop over and traverse
	// down the path parts of the data

	object := d.data

	// Loop over each part of the path
	// whilst detecting if the data at
	// the current path is an {} or []

	for k, p := range path {

		p = trim(p)

		// If the value found at the current
		// path part is an object, then move
		// to the next part of the path

		if m, ok := object.(map[string]interface{}); ok {
			if object, ok = m[p]; !ok {
				return exec(d.join(prev, path), nil)
			}
			continue
		}

		// If the value found at the current
		// path part is an array, then perform
		// the query on the specified items

		if a, ok := object.([]interface{}); ok {

			c, i, r := d.what(p, a, choose)

			if r == one && len(c) == 0 {
				return fmt.Errorf("No item with index %s in array, using path %s", p, path)
			}

			if r == one {
				if k == len(path)-1 {
					var keep []string
					keep = append(keep, prev...)
					keep = append(keep, path[:k]...)
					keep = append(keep, fmt.Sprintf("[%d]", i[0]))
					return exec(d.join(keep), c[0])
				} else {
					var keep []string
					keep = append(keep, prev...)
					keep = append(keep, path[:k]...)
					keep = append(keep, fmt.Sprintf("[%d]", i[0]))
					return ConsumeWithFetch(c[0], d.call).walk(exec, keep, path[k+1:]...)
				}
			}

			if r == many {
				for j, v := range c {
					if k == len(path)-1 {
						var keep []string
						keep = append(keep, prev...)
						keep = append(keep, path[:k]...)
						keep = append(keep, fmt.Sprintf("[%d]", i[j]))
						if err := exec(d.join(keep), v); err != nil {
							return err
						}
					} else {
						var keep []string
						keep = append(keep, prev...)
						keep = append(keep, path[:k]...)
						keep = append(keep, fmt.Sprintf("[%d]", i[j]))
						if err := ConsumeWithFetch(v, d.call).walk(exec, keep, path[k+1:]...); err != nil {
							return err
						}
					}
				}
				return nil
			}

		}

		// The current path item is not an object or an array
		// but there are still other items in the search path.

		return fmt.Errorf("Can not get path %s from %v", path, object)

	}

	return exec(d.join(prev, path), object)

}
