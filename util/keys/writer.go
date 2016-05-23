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
	// "golang.org/x/text/collate"
	// "golang.org/x/text/language"
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
	case uint:
		binary.Write(w.Writer, binary.BigEndian, int64(v))
	case uint8:
		binary.Write(w.Writer, binary.BigEndian, int64(v))
	case uint16:
		binary.Write(w.Writer, binary.BigEndian, int64(v))
	case uint32:
		binary.Write(w.Writer, binary.BigEndian, int64(v))
	case uint64:
		binary.Write(w.Writer, binary.BigEndian, int64(v))
	case int:
		binary.Write(w.Writer, binary.BigEndian, int64(v))
	case int8:
		binary.Write(w.Writer, binary.BigEndian, int64(v))
	case int16:
		binary.Write(w.Writer, binary.BigEndian, int64(v))
	case int32:
		binary.Write(w.Writer, binary.BigEndian, int64(v))
	case int64:
		binary.Write(w.Writer, binary.BigEndian, int64(v))
	case float32:
		binary.Write(w.Writer, binary.BigEndian, int64(v))
	case float64:
		binary.Write(w.Writer, binary.BigEndian, int64(v))
	case time.Time:
		binary.Write(w.Writer, binary.BigEndian, v.UnixNano())

	}

}
