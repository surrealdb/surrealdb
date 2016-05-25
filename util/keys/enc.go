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
	"encoding"
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

			e.w.Write(bNILL)
			e.w.Write(bTERM)

		case bool:

			e.w.Write(bBOOL)
			if value {
				e.w.Write(bBOOL)
			}
			e.w.Write(bTERM)

		case time.Time:

			e.w.Write(bTIME)
			e.w.Write(value)
			e.w.Write(bTERM)

		case []byte:

			e.w.Write(bSTRING)
			e.w.Write(value)
			e.w.Write(bTERM)
			e.w.Write(bTERM)

		case string:

			if value == Prefix {
				e.w.Write(bPREFIX)
				e.w.Write(bTERM)
				break
			}

			if value == Suffix {
				e.w.Write(bSUFFIX)
				e.w.Write(bTERM)
				break
			}

			e.w.Write(bSTRING)
			e.w.Write(value)
			e.w.Write(bTERM)
			e.w.Write(bTERM)

		case float32, float64:

			e.w.Write(bNUMBER)
			e.w.Write(value)
			e.w.Write(bTERM)

		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:

			e.w.Write(bNUMBER)
			e.w.Write(value)
			e.w.Write(bTERM)

		case []bool:

			e.w.Write(bARRAY)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bTERM)

		case []string:

			e.w.Write(bARRAY)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bTERM)

		case []int:

			e.w.Write(bARRAY)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bTERM)

		case []int8:

			e.w.Write(bARRAY)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bTERM)

		case []int16:

			e.w.Write(bARRAY)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bTERM)

		case []int32:

			e.w.Write(bARRAY)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bTERM)

		case []int64:

			e.w.Write(bARRAY)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bTERM)

		case []uint:

			e.w.Write(bARRAY)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bTERM)

		case []uint16:

			e.w.Write(bARRAY)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bTERM)

		case []uint32:

			e.w.Write(bARRAY)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bTERM)

		case []uint64:

			e.w.Write(bARRAY)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bTERM)

		case []float32:

			e.w.Write(bARRAY)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bTERM)

		case []float64:

			e.w.Write(bARRAY)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bTERM)

		case []interface{}:

			e.w.Write(bARRAY)
			for _, val := range value {
				e.Encode(val)
			}
			e.w.Write(bTERM)

		case encoding.TextMarshaler:

			buf, _ := value.MarshalText()

			e.w.Write(bSTRING)
			e.w.Write(buf)
			e.w.Write(bTERM)
			e.w.Write(bTERM)

		}

	}

}
