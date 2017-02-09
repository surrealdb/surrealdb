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
	"bytes"
	"io"
	"time"
)

type encoder struct {
	w *writer
}

// encode encodes an interface using the unicode collation algorithm.
func encode(items ...interface{}) []byte {
	b := &bytes.Buffer{}
	newEncoder(b).Encode(items...)
	return b.Bytes()
}

func newEncoder(w io.Writer) *encoder {
	return &encoder{
		w: newWriter(w),
	}
}

func (e *encoder) Encode(items ...interface{}) {

	for _, item := range items {

		switch value := item.(type) {

		case nil:

			e.w.write(bNIL)
			e.w.write(bEND)

		case bool:

			e.w.write(bVAL)
			if value {
				e.w.write(bVAL)
			}
			e.w.write(bEND)

		case time.Time:

			e.w.write(bTME)
			e.w.write(value)
			e.w.write(bEND)

		case []byte:

			e.w.write(bSTR)
			e.w.write(value)
			e.w.write(bEND)
			e.w.write(bEND)

		case string:

			if value == Ignore {
				break
			}

			if value == Prefix {
				e.w.write(bPRE)
				e.w.write(bEND)
				break
			}

			if value == Suffix {
				e.w.write(bSUF)
				e.w.write(bEND)
				break
			}

			e.w.write(bSTR)
			e.w.write(value)
			e.w.write(bEND)
			e.w.write(bEND)

		case float32, float64:

			e.w.write(value)
			e.w.write(bEND)

		case int, int8, int16, int32, int64:

			e.w.write(value)
			e.w.write(bEND)

		case uint, uint8, uint16, uint32, uint64:

			e.w.write(value)
			e.w.write(bEND)

		case []time.Time:

			e.w.write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.write(bEND)

		case []bool:

			e.w.write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.write(bEND)

		case []string:

			e.w.write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.write(bEND)

		case []int:

			e.w.write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.write(bEND)

		case []int8:

			e.w.write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.write(bEND)

		case []int16:

			e.w.write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.write(bEND)

		case []int32:

			e.w.write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.write(bEND)

		case []int64:

			e.w.write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.write(bEND)

		case []uint:

			e.w.write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.write(bEND)

		case []uint16:

			e.w.write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.write(bEND)

		case []uint32:

			e.w.write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.write(bEND)

		case []uint64:

			e.w.write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.write(bEND)

		case []float32:

			e.w.write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.write(bEND)

		case []float64:

			e.w.write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.write(bEND)

		case []interface{}:

			e.w.write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.write(bEND)

		}

	}

}
