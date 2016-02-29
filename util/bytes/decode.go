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

package bytes

import (
	"encoding"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
)

type Decoder struct {
	Order binary.ByteOrder
	r     *reader
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		Order: binary.BigEndian,
		r:     &reader{r},
	}
}

func (d *Decoder) Decode(v interface{}) (err error) {
	// Check if the type implements the encoding.BinaryUnmarshaler interface, and use it if so.
	if i, ok := v.(encoding.BinaryUnmarshaler); ok {
		var l uint64
		if l, err = binary.ReadUvarint(d.r); err != nil {
			return
		}
		buf := make([]byte, l)
		_, err = d.r.Read(buf)
		return i.UnmarshalBinary(buf)
	}

	// Otherwise, use reflection.
	rv := reflect.Indirect(reflect.ValueOf(v))
	if !rv.CanAddr() {
		return errors.New("binary: can only Decode to pointer type")
	}
	t := rv.Type()

	switch t.Kind() {

	case reflect.Array:
		len := t.Len()
		for i := 0; i < int(len); i++ {
			if err = d.Decode(rv.Index(i).Addr().Interface()); err != nil {
				return
			}
		}

	case reflect.Slice:
		var l uint64
		if l, err = binary.ReadUvarint(d.r); err != nil {
			return
		}
		if t.Kind() == reflect.Slice {
			rv.Set(reflect.MakeSlice(t, int(l), int(l)))
		} else if int(l) != t.Len() {
			return errors.New("binary: encoded size != real size")
		}
		for i := 0; i < int(l); i++ {
			if err = d.Decode(rv.Index(i).Addr().Interface()); err != nil {
				return
			}
		}

	case reflect.Struct:
		l := rv.NumField()
		for i := 0; i < l; i++ {
			if v := rv.Field(i); v.CanSet() && t.Field(i).Name != "_" {
				if err = d.Decode(v.Addr().Interface()); err != nil {
					return
				}
			}
		}

	case reflect.Map:
		var l uint64
		if l, err = binary.ReadUvarint(d.r); err != nil {
			return
		}
		kt := t.Key()
		vt := t.Elem()
		rv.Set(reflect.MakeMap(t))
		for i := 0; i < int(l); i++ {
			kv := reflect.Indirect(reflect.New(kt))
			if err = d.Decode(kv.Addr().Interface()); err != nil {
				return
			}
			vv := reflect.Indirect(reflect.New(vt))
			if err = d.Decode(vv.Addr().Interface()); err != nil {
				return
			}
			rv.SetMapIndex(kv, vv)
		}

	case reflect.String:
		var l uint64
		if l, err = binary.ReadUvarint(d.r); err != nil {
			return
		}
		buf := make([]byte, l)
		_, err = d.r.Read(buf)
		rv.SetString(string(buf))

	case reflect.Bool:
		var out byte
		err = binary.Read(d.r, d.Order, &out)
		rv.SetBool(out != 0)

	case reflect.Int:
		var out int64
		err = binary.Read(d.r, d.Order, &out)
		rv.SetInt(out)

	case reflect.Uint:
		var out uint64
		err = binary.Read(d.r, d.Order, &out)
		rv.SetUint(out)

	case reflect.Int8, reflect.Uint8, reflect.Int16, reflect.Uint16,
		reflect.Int32, reflect.Uint32, reflect.Int64, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		err = binary.Read(d.r, d.Order, v)

	default:
		return errors.New("binary: unsupported type " + t.String())
	}

	return

}
