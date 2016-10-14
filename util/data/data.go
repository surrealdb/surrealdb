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
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/abcum/surreal/util/deep"
	"github.com/abcum/surreal/util/pack"
)

// Doc holds a reference to the core data object, or a selected path.
type Doc struct {
	data interface{}
}

// New creates a new data object.
func New() *Doc {
	return &Doc{map[string]interface{}{}}
}

// Consume converts a GO interface into a data object.
func Consume(input interface{}) *Doc {
	return &Doc{input}
}

// Data returns the internal data object as an interface.
func (d *Doc) Data() interface{} {
	return d.data
}

// Copy returns a duplicated copy of the internal data object.
func (d *Doc) Copy() (i interface{}) {
	return deep.Copy(d.data)
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

// --------------------------------------------------------------------------------

func (d *Doc) path(path ...string) (paths []string) {
	for _, p := range path {
		paths = append(paths, strings.Split(p, ".")...)
	}
	return
}

// --------------------------------------------------------------------------------

// Reset empties and resets the data at the specified path.
func (d *Doc) Reset(path ...string) (*Doc, error) {
	return d.Set(nil, path...)
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

// Iff sets the value at the specified path if it is not nil, or deleted it.
func (d *Doc) Iff(value interface{}, path ...string) (*Doc, error) {
	if value != nil {
		return d.Set(value, path...)
	}
	return &Doc{nil}, d.Del(path...)
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

	return &Doc{out}

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

	return &Doc{out}

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

			var i int
			var e error

			if p == "*" {
				e = errors.New("")
			} else if p == "first" {
				i = 0
			} else if p == "last" {
				i = len(a) - 1
			} else {
				i, e = strconv.Atoi(p)
			}

			// If the path part is a numeric index
			// then run the query on the specified
			// index of the current data array

			if e == nil {
				if 0 == len(a) || i >= len(a) {
					return false
				}
				return Consume(a[i]).Exists(path[k+1:]...)
			}

			// If the path part is an asterisk
			// then run the query on all of the
			// items in the current data array

			if p == "*" {

				for _, v := range a {
					if Consume(v).Exists(path[k+1:]...) {
						return true
					}
				}

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
		return &Doc{nil}
	}

	// Define the temporary object so
	// that we can loop over and traverse
	// down the path parts of the data

	object := d.data

	// Loop over each part of the path
	// whilst detecting if the data at
	// the current path is an {} or []

	for k, p := range path {

		// If the value found at the current
		// path part is an object, then move
		// to the next part of the path

		if m, ok := object.(map[string]interface{}); ok {
			object = m[p]
			continue
		}

		// If the value found at the current
		// path part is an array, then perform
		// the query on the specified items

		if a, ok := object.([]interface{}); ok {

			var i int
			var e error

			if p == "*" {
				e = errors.New("")
			} else if p == "first" {
				i = 0
			} else if p == "last" {
				i = len(a) - 1
			} else if p == "length" {
				return &Doc{len(a)}
			} else {
				i, e = strconv.Atoi(p)
			}

			// If the path part is a numeric index
			// then run the query on the specified
			// index of the current data array

			if e == nil {
				if 0 == len(a) || i >= len(a) {
					return &Doc{nil}
				}
				return Consume(a[i]).Get(path[k+1:]...)
			}

			// If the path part is an asterisk
			// then run the query on all of the
			// items in the current data array

			if p == "*" {

				out := []interface{}{}

				for _, v := range a {
					res := Consume(v).Get(path[k+1:]...)
					if res.data != nil {
						out = append(out, res.data)
					}
				}

				return &Doc{out}

			}

		}

		return &Doc{nil}

	}

	return &Doc{object}

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
		}

		// If the value found at the current
		// path part is an array, then perform
		// the query on the specified items

		if a, ok := object.([]interface{}); ok {

			var i int
			var e error

			if p == "*" {
				e = errors.New("")
			} else if p == "first" {
				i = 0
			} else if p == "last" {
				i = len(a) - 1
			} else {
				i, e = strconv.Atoi(p)
			}

			// If the path part is a numeric index
			// then run the query on the specified
			// index of the current data array

			if e == nil {

				if 0 == len(a) || i >= len(a) {
					return &Doc{nil}, fmt.Errorf("No item with index %d in array, using path %s", i, path)
				}

				if k == len(path)-1 {
					a[i] = value
					object = a[i]
				} else {
					return Consume(a[i]).Set(value, path[k+1:]...)
				}

			}

			// If the path part is an asterisk
			// then run the query on all of the
			// items in the current data array

			if p == "*" {

				out := []interface{}{}

				for i := range a {

					if k == len(path)-1 {
						a[i] = value
						out = append(out, a[i])
					} else {
						res, _ := Consume(a[i]).Set(value, path[k+1:]...)
						if res.data != nil {
							out = append(out, res.data)
						}
					}

				}

				return &Doc{out}, nil

			}

		}

	}

	return &Doc{object}, nil

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
		}

		// If the value found at the current
		// path part is an array, then perform
		// the query on the specified items

		if a, ok := object.([]interface{}); ok {

			var i int
			var e error

			if p == "*" {
				e = errors.New("")
			} else if p == "first" {
				i = 0
			} else if p == "last" {
				i = len(a) - 1
			} else {
				i, e = strconv.Atoi(p)
			}

			// If the path part is a numeric index
			// then run the query on the specified
			// index of the current data array

			if e == nil {

				if 0 == len(a) || i >= len(a) {
					return fmt.Errorf("No item with index %d in array, using path %s", i, path)
				}

				if k == len(path)-1 {
					copy(a[i:], a[i+1:])
					a[len(a)-1] = nil
					a = a[:len(a)-1]
					d.Set(a, path[:len(path)-1]...)
				} else {
					return Consume(a[i]).Del(path[k+1:]...)
				}

			}

			// If the path part is an asterisk
			// then run the query on all of the
			// items in the current data array

			if p == "*" {

				for i := len(a) - 1; i >= 0; i-- {

					if k == len(path)-1 {
						copy(a[i:], a[i+1:])
						a[len(a)-1] = nil
						a = a[:len(a)-1]
						d.Set(a, path[:len(path)-1]...)
					} else {
						Consume(a[i]).Del(path[k+1:]...)
					}

				}

			}

		}

	}

	return nil

}

// --------------------------------------------------------------------------------

// ArrayDel appends an item or an array of items to an array at the specified path.
func (d *Doc) ArrayAdd(value interface{}, path ...string) (*Doc, error) {

	a, ok := d.Get(path...).Data().([]interface{})
	if !ok {
		return &Doc{nil}, fmt.Errorf("Not an array")
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
				return nil, nil
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
		return &Doc{nil}, fmt.Errorf("Not an array")
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

	return &Doc{nil}, fmt.Errorf("Not possible to increment.")

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

	return &Doc{nil}, fmt.Errorf("Not possible to decrement.")

}
