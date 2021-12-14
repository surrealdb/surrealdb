// Copyright © 2016 SurrealDB Ltd.
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
	"sync"
	"time"
)

type encoder struct {
	w *writer
}

var encoders = sync.Pool{
	New: func() interface{} {
		return &encoder{
			w: newWriter(),
		}
	},
}

// Encode encodes an interface using the unicode collation algorithm.
func Encode(items ...interface{}) []byte {
	return encode(items...)
}

// encode encodes an interface using the unicode collation algorithm.
func encode(items ...interface{}) (dst []byte) {
	enc := newEncoder(&dst)
	enc.Encode(items...)
	enc.Reset()
	return
}

func newEncoder(b *[]byte) *encoder {
	e := encoders.Get().(*encoder)
	e.w.w.ResetBytes(b)
	return e
}

func (e *encoder) Reset() {
	encoders.Put(e)
}

func (e *encoder) Encode(items ...interface{}) {

	for _, item := range items {

		switch value := item.(type) {

		case nil:

			e.w.writeOne(bNIL)
			e.w.writeOne(bEND)

		case bool:

			e.w.writeOne(bVAL)
			if value {
				e.w.writeOne(bVAL)
			}
			e.w.writeOne(bEND)

		case time.Time:

			e.w.writeOne(bTME)
			e.w.writeTime(value)
			e.w.writeOne(bEND)

		case []byte:

			e.w.writeOne(bSTR)
			e.w.writeMany(value)
			e.w.writeOne(bEND)
			e.w.writeOne(bEND)

		case string:

			if value == Ignore {
				break
			}

			if value == Prefix {
				e.w.writeOne(bPRE)
				e.w.writeOne(bEND)
				break
			}

			if value == Suffix {
				e.w.writeOne(bSUF)
				e.w.writeOne(bEND)
				break
			}

			e.w.writeOne(bSTR)
			e.w.writeString(value)
			e.w.writeOne(bEND)
			e.w.writeOne(bEND)

		case int:

			e.w.writeFloat(float64(value))
			e.w.writeOne(bEND)

		case int8:

			e.w.writeFloat(float64(value))
			e.w.writeOne(bEND)

		case int16:

			e.w.writeFloat(float64(value))
			e.w.writeOne(bEND)

		case int32:

			e.w.writeFloat(float64(value))
			e.w.writeOne(bEND)

		case int64:

			e.w.writeFloat(float64(value))
			e.w.writeOne(bEND)

		case uint:

			e.w.writeFloat(float64(value))
			e.w.writeOne(bEND)

		case uint8:

			e.w.writeFloat(float64(value))
			e.w.writeOne(bEND)

		case uint16:

			e.w.writeFloat(float64(value))
			e.w.writeOne(bEND)

		case uint32:

			e.w.writeFloat(float64(value))
			e.w.writeOne(bEND)

		case uint64:

			e.w.writeFloat(float64(value))
			e.w.writeOne(bEND)

		case float32:

			e.w.writeFloat(float64(value))
			e.w.writeOne(bEND)

		case float64:

			e.w.writeFloat(float64(value))
			e.w.writeOne(bEND)

		case []time.Time:

			e.w.writeOne(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.writeOne(bEND)

		case []bool:

			e.w.writeOne(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.writeOne(bEND)

		case []string:

			e.w.writeOne(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.writeOne(bEND)

		case []int:

			e.w.writeOne(bARR)
			for _, val := range value {
				e.w.writeFloat(float64(val))
				e.w.writeOne(bEND)
			}
			e.w.writeOne(bEND)

		case []int8:

			e.w.writeOne(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.writeOne(bEND)

		case []int16:

			e.w.writeOne(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.writeOne(bEND)

		case []int32:

			e.w.writeOne(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.writeOne(bEND)

		case []int64:

			e.w.writeOne(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.writeOne(bEND)

		case []uint:

			e.w.writeOne(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.writeOne(bEND)

		case []uint16:

			e.w.writeOne(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.writeOne(bEND)

		case []uint32:

			e.w.writeOne(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.writeOne(bEND)

		case []uint64:

			e.w.writeOne(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.writeOne(bEND)

		case []float32:

			e.w.writeOne(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.writeOne(bEND)

		case []float64:

			e.w.writeOne(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.writeOne(bEND)

		case []interface{}:

			e.w.writeOne(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.writeOne(bEND)

		}

	}

}
