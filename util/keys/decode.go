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

type decoder struct {
	r *reader
}

var decoders = sync.Pool{
	New: func() interface{} {
		return &decoder{
			r: newReader(),
		}
	},
}

// decode decodes an encoded string using the unicode collation algorithm.
func decode(data []byte, items ...interface{}) {
	dec := newDecoder(data)
	dec.Decode(items...)
	dec.Reset()
	return
}

func newDecoder(b []byte) *decoder {
	d := decoders.Get().(*decoder)
	d.r.r.ResetBytes(b)
	return d
}

func (d *decoder) Reset() {
	decoders.Put(d)
}

func (d *decoder) Decode(items ...interface{}) {

	for _, item := range items {

		switch value := item.(type) {

		case *time.Time:
			*value = d.r.readTime()

		case *bool:
			*value = d.r.readBool()

		case *[]byte:
			*value = d.r.readBytes()

		case *string:
			*value = d.r.readString()

		case *int:
			*value = int(d.r.readFloat())

		case *int8:
			*value = int8(d.r.readFloat())

		case *int16:
			*value = int16(d.r.readFloat())

		case *int32:
			*value = int32(d.r.readFloat())

		case *int64:
			*value = int64(d.r.readFloat())

		case *uint:
			*value = uint(d.r.readFloat())

		case *uint8:
			*value = uint8(d.r.readFloat())

		case *uint16:
			*value = uint16(d.r.readFloat())

		case *uint32:
			*value = uint32(d.r.readFloat())

		case *uint64:
			*value = uint64(d.r.readFloat())

		case *float32:
			*value = float32(d.r.readFloat())

		case *float64:
			*value = float64(d.r.readFloat())

		case *[]time.Time:
			if d.r.readNext(bARR) {
				for !d.r.readNext(bEND) {
					*value = append(*value, d.r.readTime())
				}
			}

		case *[]bool:
			if d.r.readNext(bARR) {
				for !d.r.readNext(bEND) {
					*value = append(*value, d.r.readBool())
				}
			}

		case *[]string:
			if d.r.readNext(bARR) {
				for !d.r.readNext(bEND) {
					*value = append(*value, d.r.readString())
				}
			}

		case *[]int:
			if d.r.readNext(bARR) {
				for !d.r.readNext(bEND) {
					*value = append(*value, int(d.r.readFloat()))
				}
			}

		case *[]int8:
			if d.r.readNext(bARR) {
				for !d.r.readNext(bEND) {
					*value = append(*value, int8(d.r.readFloat()))
				}
			}

		case *[]int16:
			if d.r.readNext(bARR) {
				for !d.r.readNext(bEND) {
					*value = append(*value, int16(d.r.readFloat()))
				}
			}

		case *[]int32:
			if d.r.readNext(bARR) {
				for !d.r.readNext(bEND) {
					*value = append(*value, int32(d.r.readFloat()))
				}
			}

		case *[]int64:
			if d.r.readNext(bARR) {
				for !d.r.readNext(bEND) {
					*value = append(*value, int64(d.r.readFloat()))
				}
			}

		case *[]uint:
			if d.r.readNext(bARR) {
				for !d.r.readNext(bEND) {
					*value = append(*value, uint(d.r.readFloat()))
				}
			}

		case *[]uint16:
			if d.r.readNext(bARR) {
				for !d.r.readNext(bEND) {
					*value = append(*value, uint16(d.r.readFloat()))
				}
			}

		case *[]uint32:
			if d.r.readNext(bARR) {
				for !d.r.readNext(bEND) {
					*value = append(*value, uint32(d.r.readFloat()))
				}
			}

		case *[]uint64:
			if d.r.readNext(bARR) {
				for !d.r.readNext(bEND) {
					*value = append(*value, uint64(d.r.readFloat()))
				}
			}

		case *[]float32:
			if d.r.readNext(bARR) {
				for !d.r.readNext(bEND) {
					*value = append(*value, float32(d.r.readFloat()))
				}
			}

		case *[]float64:
			if d.r.readNext(bARR) {
				for !d.r.readNext(bEND) {
					*value = append(*value, float64(d.r.readFloat()))
				}
			}

		case *[]interface{}:
			*value = d.r.readArray()

		case *interface{}:

			switch d.r.lookNext() {
			default:
				*value = d.r.readAny()
			case bNIL:
				*value = d.r.readNull()
			case bVAL:
				*value = d.r.readBool()
			case bTME:
				*value = d.r.readTime()
			case bNEG, bPOS:
				*value = d.r.readFloat()
			case bSTR, bPRE, bSUF:
				*value = d.r.readString()
			case bARR:
				*value = d.r.readArray()

			}

		}

	}

}
