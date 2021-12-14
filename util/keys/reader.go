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
	"encoding/binary"
	"math"
	"time"

	"github.com/surrealdb/bump"
)

type reader struct {
	r *bump.Reader
}

func newReader() *reader {
	return &reader{
		r: bump.NewReader(nil),
	}
}

func (r *reader) unread() {

}

func (r *reader) lookNext() (byt byte) {
	byt, _ = r.r.PeekByte()
	return
}

func (r *reader) readNext(exp byte) (fnd bool) {
	if byt, _ := r.r.PeekByte(); byt == exp {
		r.r.ReadByte()
		return true
	}
	return false
}

func (r *reader) readSize(sze int) (byt []byte) {
	byt, _ = r.r.ReadBytes(sze)
	return byt
}

func (r *reader) readUpto(exp ...byte) (byt []byte) {

LOOP:
	for {

		bit, _ := r.r.ReadByte()
		byt = append(byt, bit)
		if bit != exp[0] {
			continue
		}

		for j := 1; j < len(exp); j++ {
			if r.readNext(exp[j]) {
				byt = append(byt, bit)
				continue
			}
			break LOOP
		}

		break

	}

	return byt[:len(byt)-len(exp)]

}

// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------
// --------------------------------------------------

func (r *reader) readAny() (val interface{}) {
	return r.readUpto(bEND)
}

func (r *reader) readNull() (val interface{}) {
	if r.readNext(bNIL) {
		r.readNext(bEND)
	}
	return
}

func (r *reader) readTime() (val time.Time) {
	if r.readNext(bTME) {
		bin := r.readSize(8)
		dec := binary.BigEndian.Uint64(bin)
		val = time.Unix(0, int64(dec)).UTC()
		r.readNext(bEND)
	}
	return
}

func (r *reader) readBool() (val bool) {
	if r.readNext(bVAL) {
		val = r.readNext(bVAL)
		r.readNext(bEND)
	}
	return
}

func (r *reader) readBytes() (val []byte) {
	if r.readNext(bSTR) {
		val = r.readUpto(bEND, bEND)
	}
	return
}

func (r *reader) readFloat() (val float64) {
	if r.readNext(bNEG) {
		bin := r.readSize(8)
		dec := binary.BigEndian.Uint64(bin)
		val = math.Float64frombits(^dec)
		r.readNext(bEND)
	} else if r.readNext(bPOS) {
		bin := r.readSize(8)
		dec := binary.BigEndian.Uint64(bin)
		val = math.Float64frombits(dec)
		r.readNext(bEND)
	}
	return
}

func (r *reader) readString() (val string) {
	if r.readNext(bSTR) {
		val = string(r.readUpto(bEND, bEND))
	} else if r.readNext(bPRE) {
		val = Prefix
		r.readNext(bEND)
	} else if r.readNext(bSUF) {
		val = Suffix
		r.readNext(bEND)
	}
	return
}

func (r *reader) readNumber() (val interface{}) {
	var num float64
	if r.readNext(bNEG) {
		bin := r.readSize(8)
		dec := binary.BigEndian.Uint64(bin)
		num = math.Float64frombits(^dec)
		r.readNext(bEND)
	} else if r.readNext(bPOS) {
		bin := r.readSize(8)
		dec := binary.BigEndian.Uint64(bin)
		num = math.Float64frombits(dec)
		r.readNext(bEND)
	}
	if math.Trunc(num) == num {
		return int64(math.Trunc(num))
	}
	return num
}

func (r *reader) readArray() (val []interface{}) {
	if r.readNext(bARR) {
		for !r.readNext(bEND) {
			switch r.lookNext() {
			default:
				val = append(val, []interface{}{r.readAny()}...)
			case bNIL:
				val = append(val, []interface{}{r.readNull()}...)
			case bVAL:
				val = append(val, []interface{}{r.readBool()}...)
			case bTME:
				val = append(val, []interface{}{r.readTime()}...)
			case bNEG, bPOS:
				val = append(val, []interface{}{r.readNumber()}...)
			case bSTR, bPRE, bSUF:
				val = append(val, []interface{}{r.readString()}...)
			case bARR:
				val = append(val, []interface{}{r.readArray()}...)
			}
		}
		r.readNext(bEND)
	}
	return
}
