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

// Encode encodes an interface using the unicode collation algorithm.
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

			e.w.Write(bNIL)
			e.w.Write(bEND)

		case bool:

			e.w.Write(bVAL)
			if value {
				e.w.Write(bVAL)
			}
			e.w.Write(bEND)

		case time.Time:

			e.w.Write(bTME)
			e.w.Write(value)
			e.w.Write(bEND)

		case []byte:

			e.w.Write(bSTR)
			e.w.Write(value)
			e.w.Write(bEND)
			e.w.Write(bEND)

		case string:

			if value == Ignore {
				break
			}

			if value == Prefix {
				e.w.Write(bPRE)
				e.w.Write(bEND)
				break
			}

			if value == Suffix {
				e.w.Write(bSUF)
				e.w.Write(bEND)
				break
			}

			e.w.Write(bSTR)
			e.w.Write(value)
			e.w.Write(bEND)
			e.w.Write(bEND)

		case float32, float64:

			e.w.Write(value)
			e.w.Write(bEND)

		case int, int8, int16, int32, int64:

			e.w.Write(value)
			e.w.Write(bEND)

		case uint, uint8, uint16, uint32, uint64:

			e.w.Write(value)
			e.w.Write(bEND)

		case []time.Time:

			e.w.Write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bEND)

		case []bool:

			e.w.Write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bEND)

		case []string:

			e.w.Write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bEND)

		case []int:

			e.w.Write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bEND)

		case []int8:

			e.w.Write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bEND)

		case []int16:

			e.w.Write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bEND)

		case []int32:

			e.w.Write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bEND)

		case []int64:

			e.w.Write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bEND)

		case []uint:

			e.w.Write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bEND)

		case []uint16:

			e.w.Write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bEND)

		case []uint32:

			e.w.Write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bEND)

		case []uint64:

			e.w.Write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bEND)

		case []float32:

			e.w.Write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bEND)

		case []float64:

			e.w.Write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bEND)

		case []interface{}:

			e.w.Write(bARR)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bEND)

		}

	}

}
