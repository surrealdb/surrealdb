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
	"math"
	"reflect"
	"time"
	"unsafe"

	"github.com/abcum/bump"
)

type writer struct {
	w *bump.Writer
}

func newWriter() *writer {
	return &writer{
		w: bump.NewWriter(nil),
	}
}

func (w *writer) writeOne(v byte) {
	w.w.WriteByte(v)
}

func (w *writer) writeMany(v []byte) {
	w.w.WriteBytes(v)
}

func (w *writer) writeTime(v time.Time) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v.UTC().UnixNano()))
	w.w.WriteBytes(b)
}

func (w *writer) writeFloat(v float64) {
	b := make([]byte, 8)
	if v < 0 {
		w.w.WriteByte(bNEG)
		binary.BigEndian.PutUint64(b, ^math.Float64bits(v))
	} else {
		w.w.WriteByte(bPOS)
		binary.BigEndian.PutUint64(b, math.Float64bits(v))
	}
	w.w.WriteBytes(b)
}

func (w *writer) writeString(v string) {
	b := *(*[]byte)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&v))))
	w.w.WriteBytes(b)
}
