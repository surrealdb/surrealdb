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

type Encoder struct {
	Order binary.ByteOrder
	w     io.Writer
	buf   []byte
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		Order: binary.BigEndian,
		w:     w,
		buf:   make([]byte, 8),
	}
}

func (e *Encoder) writeVarint(v int) error {
	l := binary.PutUvarint(e.buf, uint64(v))
	_, err := e.w.Write(e.buf[:l])
	return err
}

func (b *Encoder) Encode(v interface{}) (err error) {

	switch cv := v.(type) {

	case encoding.BinaryMarshaler:
		buf, err := cv.MarshalBinary()
		if err != nil {
			return err
		}
		if err = b.writeVarint(len(buf)); err != nil {
			return err
		}
		_, err = b.w.Write(buf)

	case []byte: // fast-path byte arrays
		if err = b.writeVarint(len(cv)); err != nil {
			return
		}
		_, err = b.w.Write(cv)

	default:
		rv := reflect.Indirect(reflect.ValueOf(v))
		t := rv.Type()
		switch t.Kind() {
		case reflect.Array:
			l := t.Len()
			for i := 0; i < l; i++ {
				if err = b.Encode(rv.Index(i).Addr().Interface()); err != nil {
					return
				}
			}

		case reflect.Slice:
			l := rv.Len()
			if err = b.writeVarint(l); err != nil {
				return
			}
			for i := 0; i < l; i++ {
				if err = b.Encode(rv.Index(i).Addr().Interface()); err != nil {
					return
				}
			}

		case reflect.Struct:
			l := rv.NumField()
			for i := 0; i < l; i++ {
				if v := rv.Field(i); v.CanSet() && t.Field(i).Name != "_" {
					// take the address of the field, so structs containing structs
					// are correctly encoded.
					if err = b.Encode(v.Addr().Interface()); err != nil {
						return
					}
				}
			}

		case reflect.Map:
			l := rv.Len()
			if err = b.writeVarint(l); err != nil {
				return
			}
			for _, key := range rv.MapKeys() {
				value := rv.MapIndex(key)
				if err = b.Encode(key.Interface()); err != nil {
					return err
				}
				if err = b.Encode(value.Interface()); err != nil {
					return err
				}
			}

		case reflect.String:
			if err = b.writeVarint(rv.Len()); err != nil {
				return
			}
			_, err = b.w.Write([]byte(rv.String()))

		case reflect.Bool:
			var out byte
			if rv.Bool() {
				out = 1
			}
			err = binary.Write(b.w, b.Order, out)

		case reflect.Int:
			err = binary.Write(b.w, b.Order, int64(rv.Int()))

		case reflect.Uint:
			err = binary.Write(b.w, b.Order, int64(rv.Uint()))

		case reflect.Int8, reflect.Uint8, reflect.Int16, reflect.Uint16,
			reflect.Int32, reflect.Uint32, reflect.Int64, reflect.Uint64,
			reflect.Float32, reflect.Float64,
			reflect.Complex64, reflect.Complex128:
			err = binary.Write(b.w, b.Order, v)

		default:
			return errors.New("binary: unsupported type " + t.String())
		}
	}

	return

}
