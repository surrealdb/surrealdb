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
	"reflect"
	"strings"
	"time"

	"github.com/ugorji/go/codec"
)

// -----------------------------------------------------------------------------------------

var (
	// ErrOutOfBounds - Index out of bounds.
	ErrOutOfBounds = errors.New("out of bounds")

	// ErrNotObj - The target is not an object type.
	ErrNotObj = errors.New("not an object")

	// ErrNotArray - The target is not an array type.
	ErrNotArray = errors.New("not an array")

	// ErrNotUnique - The target is not an array type.
	ErrNotUnique = errors.New("not a unique array item")

	// ErrPathCollision - Creating a path failed because an element collided with an existing value.
	ErrPathCollision = errors.New("encountered value collision whilst building path")

	// ErrInvalidInputObj - The input value was not a map[string]interface{}.
	ErrInvalidInputObj = errors.New("invalid input object")

	// ErrInvalidInputText - The input data could not be parsed.
	ErrInvalidInputText = errors.New("input text could not be parsed")

	// ErrInvalidPath - The filepath was not valid.
	ErrInvalidPath = errors.New("invalid file path")

	// ErrInvalidBuffer - The input buffer contained an invalid JSON string
	ErrInvalidBuffer = errors.New("input buffer contained invalid JSON")
)

// -----------------------------------------------------------------------------------------

// Doc holds a reference to the core data object, or a selected path.
type Doc struct {
	data interface{}
}

// -----------------------------------------------------------------------------------------

// New creates a new data object.
func New() *Doc {
	return &Doc{map[string]interface{}{}}
}

// Consume converts a GO interface into a data object.
func Consume(input interface{}) *Doc {
	return &Doc{input}
}

// NewFromJSON converts a JSON byte slice into a data object.
func NewFromJSON(input []byte) *Doc {

	doc := New()

	var opt codec.JsonHandle
	opt.MapType = reflect.TypeOf(map[string]interface{}(nil))

	codec.NewDecoderBytes(input, &opt).Decode(doc.data)

	return doc

}

// NewFromCBOR converts a CBOR byte slice into a data object.
func NewFromCBOR(input []byte) *Doc {

	doc := New()

	var opt codec.CborHandle
	opt.MapType = reflect.TypeOf(map[string]interface{}(nil))
	opt.SetInterfaceExt(reflect.TypeOf(time.Time{}), 1, extTime{})

	codec.NewDecoderBytes(input, &opt).Decode(doc.data)

	return doc

}

// NewFromPACK converts a MsgPack byte slice into a data object.
func NewFromPACK(input []byte) *Doc {

	doc := New()

	var opt codec.MsgpackHandle
	opt.WriteExt = true
	opt.RawToString = true
	opt.MapType = reflect.TypeOf(map[string]interface{}(nil))
	opt.SetBytesExt(reflect.TypeOf(time.Time{}), 1, extTime{})

	codec.NewDecoderBytes(input, &opt).Decode(doc.data)

	return doc

}

// Data returns the internal data object as an interface.
func (d *Doc) Data() interface{} {
	return d.data
}

// Reset resets and empties the internal data object.
func (d *Doc) Reset() {
	d.data = nil
}

// ToJSON converts the data object to a JSON byte slice.
func (d *Doc) ToJSON() (data []byte) {

	var opt codec.JsonHandle
	opt.Canonical = true
	opt.AsSymbols = codec.AsSymbolDefault

	codec.NewEncoderBytes(&data, &opt).Encode(d.data)

	return

}

// ToCBOR converts the data object to a CBOR byte slice.
func (d *Doc) ToCBOR() (data []byte) {

	var opt codec.CborHandle
	opt.Canonical = true
	opt.AsSymbols = codec.AsSymbolDefault
	opt.SetInterfaceExt(reflect.TypeOf(time.Time{}), 1, extTime{})

	codec.NewEncoderBytes(&data, &opt).Encode(d.data)

	return

}

// ToPACK converts the data object to a MsgPack byte slice.
func (d *Doc) ToPACK() (data []byte) {

	var opt codec.MsgpackHandle
	opt.WriteExt = true
	opt.Canonical = true
	opt.AsSymbols = codec.AsSymbolDefault
	opt.SetBytesExt(reflect.TypeOf(time.Time{}), 1, extTime{})

	codec.NewEncoderBytes(&data, &opt).Encode(d.data)

	return

}

// -----------------------------------------------------------------------------------------

// d.path("this", "is", "a.string", "and.something", "else")

func (d *Doc) path(path ...string) (paths []string) {
	for _, p := range path {
		paths = append(paths, strings.Split(p, ".")...)
	}
	return
}

// Search - Attempt to find and return an object within the JSON structure by specifying the hierarchy
// of field names to locate the target. If the search encounters an array and has not reached the end
// target then it will iterate each object of the array for the target and return all of the results in
// a JSON array.

// Exists - Checks whether a path exists.
func (d *Doc) Exists(path ...string) bool {

	path = d.path(path...)

	var object interface{}

	object = d.data
	for target := 0; target < len(path); target++ {
		if mmap, ok := object.(map[string]interface{}); ok {
			object, ok = mmap[path[target]]
			if !ok {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

// Index - Attempt to find and return an object with a JSON array by specifying the index of the
// target.
func (d *Doc) Index(index int) *Doc {
	if array, ok := d.Data().([]interface{}); ok {
		if index >= len(array) {
			return &Doc{nil}
		}
		return &Doc{array[index]}
	}
	return &Doc{nil}
}

// Children - Return a slice of all the children of the array. This also works for objects, however,
// the children returned for an object will NOT be in order and you lose the names of the returned
// objects this way.
func (d *Doc) Children() ([]*Doc, error) {
	if array, ok := d.Data().([]interface{}); ok {
		children := make([]*Doc, len(array))
		for i := 0; i < len(array); i++ {
			children[i] = &Doc{array[i]}
		}
		return children, nil
	}
	return nil, ErrNotArray
}

// ChildrenMap - Return a map of all the children of an object.
func (d *Doc) ChildrenMap() (map[string]*Doc, error) {
	if mmap, ok := d.Data().(map[string]interface{}); ok {
		children := map[string]*Doc{}
		for name, obj := range mmap {
			children[name] = &Doc{obj}
		}
		return children, nil
	}
	return nil, ErrNotObj
}

// -----------------------------------------------------------------------------------------

// Search - Attempt to find and return an object within the JSON structure by specifying the hierarchy
// of field names to locate the target. If the search encounters an array and has not reached the end
// target then it will iterate each object of the array for the target and return all of the results in
// a JSON array.
func (d *Doc) Get(path ...string) *Doc {

	path = d.path(path...)

	var object interface{}

	object = d.data
	for target := 0; target < len(path); target++ {
		if mmap, ok := object.(map[string]interface{}); ok {
			object = mmap[path[target]]
		} else if marray, ok := object.([]interface{}); ok {
			tmpArray := []interface{}{}
			for _, val := range marray {
				tmp := &Doc{val}
				res := tmp.Get(path[target:]...).Data()
				if res != nil {
					tmpArray = append(tmpArray, res)
				}
			}
			if len(tmpArray) == 0 {
				return &Doc{nil}
			}
			return &Doc{tmpArray}
		} else {
			return &Doc{nil}
		}
	}
	return &Doc{object}
}

// Set the value at the specified path if it does not exist.
func (d *Doc) New(value interface{}, path ...string) (*Doc, error) {
	if !d.Exists(path...) {
		return d.Set(value, path...)
	}
	return nil, nil
}

// Set the value at the specified path.
func (d *Doc) Set(value interface{}, path ...string) (*Doc, error) {

	path = d.path(path...)

	if len(path) == 0 {
		d.data = value
		return d, nil
	}
	var object interface{}
	if d.data == nil {
		d.data = map[string]interface{}{}
	}
	object = d.data
	for target := 0; target < len(path); target++ {
		if mmap, ok := object.(map[string]interface{}); ok {
			if target == len(path)-1 {
				mmap[path[target]] = value
			} else if mmap[path[target]] == nil {
				mmap[path[target]] = map[string]interface{}{}
			}
			object = mmap[path[target]]
		} else {
			return &Doc{nil}, ErrPathCollision
		}
	}
	return &Doc{object}, nil
}

// Del - Delete an element at a JSON path, an error is returned if the element does not exist.
func (d *Doc) Del(path ...string) error {

	path = d.path(path...)

	var object interface{}

	if d.data == nil {
		return ErrNotObj
	}
	object = d.data
	for target := 0; target < len(path); target++ {
		if mmap, ok := object.(map[string]interface{}); ok {
			if target == len(path)-1 {
				delete(mmap, path[target])
			} else if mmap[path[target]] == nil {
				return ErrNotObj
			}
			object = mmap[path[target]]
		} else {
			return ErrNotObj
		}
	}
	return nil
}

// SetIndex - Set a value of an array element based on the index.
func (d *Doc) SetIndex(value interface{}, index int) (*Doc, error) {
	if array, ok := d.Data().([]interface{}); ok {
		if index >= len(array) {
			return &Doc{nil}, ErrOutOfBounds
		}
		array[index] = value
		return &Doc{array[index]}, nil
	}
	return &Doc{nil}, ErrNotArray
}

// Array - Create a new JSON array at a path. Returns an error if the path contains a collision with
// a non object type.
func (d *Doc) Array(path ...string) (*Doc, error) {
	return d.Set([]interface{}{}, path...)
}

// Object - Create a new JSON object at a path. Returns an error if the path contains a collision with
// a non object type.
func (d *Doc) Object(path ...string) (*Doc, error) {
	return d.Set(map[string]interface{}{}, path...)
}

// -----------------------------------------------------------------------------------------

// ArrayAdd - Append a unique value onto a JSON array.
func (d *Doc) ArrayAdd(value interface{}, path ...string) error {
	array, ok := d.Get(path...).Data().([]interface{})
	if !ok {
		return ErrNotArray
	}
	for _, item := range array {
		if reflect.DeepEqual(item, value) {
			return ErrNotUnique
		}
	}
	array = append(array, value)
	_, err := d.Set(array, path...)
	return err
}

// ArrayDel - Append a unique value onto a JSON array.
func (d *Doc) ArrayDel(value interface{}, path ...string) error {
	array, ok := d.Get(path...).Data().([]interface{})
	if !ok {
		return ErrNotArray
	}
	for i, item := range array {
		if reflect.DeepEqual(item, value) {
			array = append(array[:i], array[i+1:]...)
			break
		}
	}
	_, err := d.Set(array, path...)
	return err
}

// ArrayAppend - Append a value onto a JSON array.
func (d *Doc) ArrayAppend(value interface{}, path ...string) error {
	array, ok := d.Get(path...).Data().([]interface{})
	if !ok {
		return ErrNotArray
	}
	array = append(array, value)
	_, err := d.Set(array, path...)
	return err
}

// ArrayRemove - Remove an element from a JSON array.
func (d *Doc) ArrayRemove(index int, path ...string) error {
	if index < 0 {
		return ErrOutOfBounds
	}
	array, ok := d.Get(path...).Data().([]interface{})
	if !ok {
		return ErrNotArray
	}
	if index < len(array) {
		array = append(array[:index], array[index+1:]...)
	} else {
		return ErrOutOfBounds
	}
	_, err := d.Set(array, path...)
	return err
}

// ArrayElement - Access an element from a JSON array.
func (d *Doc) ArrayElement(index int, path ...string) (*Doc, error) {
	if index < 0 {
		return &Doc{nil}, ErrOutOfBounds
	}
	array, ok := d.Get(path...).Data().([]interface{})
	if !ok {
		return &Doc{nil}, ErrNotArray
	}
	if index < len(array) {
		return &Doc{array[index]}, nil
	}
	return &Doc{nil}, ErrOutOfBounds
}

// ArrayCount - Count the number of elements in a JSON array.
func (d *Doc) ArrayCount(path ...string) (int, error) {
	if array, ok := d.Get(path...).Data().([]interface{}); ok {
		return len(array), nil
	}
	return 0, ErrNotArray
}
