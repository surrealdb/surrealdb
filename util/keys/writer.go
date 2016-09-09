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
	"time"
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
		w.Writer.Write([]byte(v))

	case time.Time:
		binary.Write(w.Writer, binary.BigEndian, v.UnixNano())

	case uint:
		w.Write(bBOOL)
		binary.Write(w.Writer, binary.BigEndian, float64(v))

	case uint8:
		w.Write(bBOOL)
		binary.Write(w.Writer, binary.BigEndian, float64(v))

	case uint16:
		w.Write(bBOOL)
		binary.Write(w.Writer, binary.BigEndian, float64(v))

	case uint32:
		w.Write(bBOOL)
		binary.Write(w.Writer, binary.BigEndian, float64(v))

	case uint64:
		w.Write(bBOOL)
		binary.Write(w.Writer, binary.BigEndian, float64(v))

	case int:
		if v < 0 {
			w.Write(bNILL)
			binary.Write(w.Writer, binary.BigEndian, 0-float64(v))
		} else {
			w.Write(bBOOL)
			binary.Write(w.Writer, binary.BigEndian, float64(v))
		}

	case int8:
		if v < 0 {
			w.Write(bNILL)
			binary.Write(w.Writer, binary.BigEndian, 0-float64(v))
		} else {
			w.Write(bBOOL)
			binary.Write(w.Writer, binary.BigEndian, float64(v))
		}

	case int16:
		if v < 0 {
			w.Write(bNILL)
			binary.Write(w.Writer, binary.BigEndian, 0-float64(v))
		} else {
			w.Write(bBOOL)
			binary.Write(w.Writer, binary.BigEndian, float64(v))
		}

	case int32:
		if v < 0 {
			w.Write(bNILL)
			binary.Write(w.Writer, binary.BigEndian, 0-float64(v))
		} else {
			w.Write(bBOOL)
			binary.Write(w.Writer, binary.BigEndian, float64(v))
		}

	case int64:
		if v < 0 {
			w.Write(bNILL)
			binary.Write(w.Writer, binary.BigEndian, 0-float64(v))
		} else {
			w.Write(bBOOL)
			binary.Write(w.Writer, binary.BigEndian, float64(v))
		}

	case float32:
		if v < 0 {
			w.Write(bNILL)
			binary.Write(w.Writer, binary.BigEndian, 0-float64(v))
		} else {
			w.Write(bBOOL)
			binary.Write(w.Writer, binary.BigEndian, float64(v))
		}

	case float64:
		if v < 0 {
			w.Write(bNILL)
			binary.Write(w.Writer, binary.BigEndian, 0-float64(v))
		} else {
			w.Write(bBOOL)
			binary.Write(w.Writer, binary.BigEndian, float64(v))
		}

	}

}
