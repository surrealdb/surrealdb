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

package keys

import (
	"encoding/binary"
	"io"
	"math"
	"reflect"
	"time"
	"unsafe"
)

type writer struct {
	io.Writer
}

func newWriter(w io.Writer) *writer {
	return &writer{
		w,
	}
}

func (w *writer) Write(i interface{}) {

	switch v := i.(type) {

	case []byte:
		w.Writer.Write(v)

	case string:
		w.writeString(v)

	case time.Time:
		w.writeTime(v)

	case uint:
		w.writeNumber(float64(v))
	case uint8:
		w.writeNumber(float64(v))
	case uint16:
		w.writeNumber(float64(v))
	case uint32:
		w.writeNumber(float64(v))
	case uint64:
		w.writeNumber(float64(v))

	case int:
		w.writeNumber(float64(v))
	case int8:
		w.writeNumber(float64(v))
	case int16:
		w.writeNumber(float64(v))
	case int32:
		w.writeNumber(float64(v))
	case int64:
		w.writeNumber(float64(v))

	case float32:
		w.writeNumber(float64(v))
	case float64:
		w.writeNumber(float64(v))

	}

}

func (w *writer) writeString(v string) {
	b := *(*[]byte)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&v))))
	w.Write(b)
}

func (w *writer) writeTime(v time.Time) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v.UTC().UnixNano()))
	w.Write(b)
}

func (w *writer) writeNumber(v float64) {
	b := make([]byte, 8)
	if v < 0 {
		w.Write(bNEG)
		binary.BigEndian.PutUint64(b, ^math.Float64bits(v))
	} else {
		w.Write(bPOS)
		binary.BigEndian.PutUint64(b, math.Float64bits(v))
	}
	w.Write(b)
}
