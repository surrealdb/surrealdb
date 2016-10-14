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

func (w *writer) write(i interface{}) {

	switch v := i.(type) {

	case []byte:
		w.Write(v)

	case string:
		w.writeStr(v)

	case time.Time:
		w.writeTme(v)

	case uint:
		w.writeNum(float64(v))
	case uint8:
		w.writeNum(float64(v))
	case uint16:
		w.writeNum(float64(v))
	case uint32:
		w.writeNum(float64(v))
	case uint64:
		w.writeNum(float64(v))

	case int:
		w.writeNum(float64(v))
	case int8:
		w.writeNum(float64(v))
	case int16:
		w.writeNum(float64(v))
	case int32:
		w.writeNum(float64(v))
	case int64:
		w.writeNum(float64(v))

	case float32:
		w.writeNum(float64(v))
	case float64:
		w.writeNum(float64(v))

	}

}

func (w *writer) writeStr(v string) {
	b := *(*[]byte)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&v))))
	w.Write(b)
}

func (w *writer) writeTme(v time.Time) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v.UTC().UnixNano()))
	w.Write(b)
}

func (w *writer) writeNum(v float64) {
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
