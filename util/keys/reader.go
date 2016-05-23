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
	"bufio"
	"encoding/binary"
	"io"
	"time"
)

type reader struct {
	*bufio.Reader
}

func newReader(r io.Reader) *reader {
	return &reader{
		bufio.NewReader(r),
	}
}

func (r *reader) ReadNext(exp byte) (fnd bool) {
	byt, _ := r.ReadByte()
	if exp == byt {
		return true
	}
	r.UnreadByte()
	return false
}

func (r *reader) ReadUpto(exp ...byte) (byt []byte) {

	for i := 0; i < len(exp); i++ {
		if i == 0 {
			rng, _ := r.ReadBytes(exp[i])
			byt = append(byt, rng...)
		}
		if i >= 1 {
			if r.ReadNext(exp[i]) {
				byt = append(byt, exp[i])
				continue
			} else {
				i = 0
			}
		}
	}

	return byt[:len(byt)-len(exp)]

}

// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------

func (r *reader) FindNext() (byt byte) {
	byt, _ = r.ReadByte()
	r.UnreadByte()
	return
}

func (r *reader) FindNull() (val interface{}) {
	if r.ReadNext(cNILL) {
		r.ReadNext(cTERM)
		return
	}
	return
}

func (r *reader) FindTime() (val time.Time) {
	if r.ReadNext(cTIME) {
		var out int64
		binary.Read(r.Reader, binary.BigEndian, &out)
		val = time.Unix(0, out).UTC()
		r.ReadNext(cTERM)
		return
	}
	return
}

func (r *reader) FindBool() (val bool) {
	if r.ReadNext(cBOOL) {
		val = r.ReadNext(cBOOL)
		r.ReadNext(cTERM)
		return
	}
	return
}

func (r *reader) FindBytes() (val []byte) {
	if r.ReadNext(cSTRING) {
		val = r.ReadUpto(cTERM, cTERM)
		return
	}
	return
}

func (r *reader) FindString() (val string) {
	if r.ReadNext(cPREFIX) {
		val = Prefix
		r.ReadNext(cTERM)
		return
	}
	if r.ReadNext(cSUFFIX) {
		val = Suffix
		r.ReadNext(cTERM)
		return
	}
	if r.ReadNext(cSTRING) {
		val = string(r.ReadUpto(cTERM, cTERM))
		return
	}
	return
}

func (r *reader) FindNumber() (val int64) {
	if r.ReadNext(cNUMBER) {
		binary.Read(r.Reader, binary.BigEndian, &val)
		r.ReadNext(cTERM)
		return
	}
	return
}

func (r *reader) FindNumberInt() (val int) {
	return int(r.FindNumber())
}

func (r *reader) FindNumberInt8() (val int8) {
	return int8(r.FindNumber())
}

func (r *reader) FindNumberInt16() (val int16) {
	return int16(r.FindNumber())
}

func (r *reader) FindNumberInt32() (val int32) {
	return int32(r.FindNumber())
}

func (r *reader) FindNumberInt64() (val int64) {
	return int64(r.FindNumber())
}

func (r *reader) FindNumberUint() (val uint) {
	return uint(r.FindNumber())
}

func (r *reader) FindNumberUint8() (val uint8) {
	return uint8(r.FindNumber())
}

func (r *reader) FindNumberUint16() (val uint16) {
	return uint16(r.FindNumber())
}

func (r *reader) FindNumberUint32() (val uint32) {
	return uint32(r.FindNumber())
}

func (r *reader) FindNumberUint64() (val uint64) {
	return uint64(r.FindNumber())
}

func (r *reader) FindNumberFloat32() (val float32) {
	return float32(r.FindNumber())
}

func (r *reader) FindNumberFloat64() (val float64) {
	return float64(r.FindNumber())
}
