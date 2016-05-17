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

import "time"

type Full struct {
	N     interface{}   // nil
	B     bool          // true
	F     bool          // false
	S     string        // string
	T     time.Time     // time.Time
	N64   int64         // negative int64
	N32   int32         // negative int32
	N16   int16         // negative int16
	N8    int8          // negative int8
	I     int           // positive int
	I8    int8          // positive int8
	I16   int16         // positive int16
	I32   int32         // positive int32
	I64   int64         // positive int64
	UI    uint          // positive uint
	UI8   uint8         // positive uint8
	UI16  uint16        // positive uint16
	UI32  uint32        // positive uint32
	UI64  uint64        // positive uint64
	NF32  float32       // negative float32
	NF64  float64       // negative float64
	F32   float32       // positive float32
	F64   float64       // positive float64
	AB    []bool        // bool array
	AS    []string      // string array
	AI8   []int8        // int8 array
	AI16  []int16       // int16 array
	AI32  []int32       // int32 array
	AI64  []int64       // int64 array
	AUI8  []uint8       // uint8 array
	AUI16 []uint16      // uint16 array
	AUI32 []uint32      // uint32 array
	AUI64 []uint64      // uint64 array
	AF32  []float32     // float32 array
	AF64  []float64     // float64 array
	IN    interface{}   // interface{}
	IB    interface{}   // interface{} true
	IF    interface{}   // interface{} talse
	IT    interface{}   // interface{} time.Time
	II    interface{}   // interface{} number
	ID    interface{}   // interface{} double
	INA   interface{}   // interface{} array
	AIN   []interface{} // []interface{} array
}

func (f *Full) String() (s string) {
	return "Test key"
}

func (f *Full) Encode() []byte {

	return encode(
		f.N, f.B, f.F, f.S, f.T,
		f.N64, f.N32, f.N16, f.N8,
		f.I, f.I8, f.I16, f.I32, f.I64,
		f.UI, f.UI8, f.UI16, f.UI32, f.UI64,
		f.NF32, f.NF64, f.F32, f.F64,
		f.AB, f.AS,
		f.AI8, f.AI16, f.AI32, f.AI64,
		f.AUI8, f.AUI16, f.AUI32, f.AUI64,
		f.AF32, f.AF64,
		f.IN, f.IB, f.IF, f.IT, f.II, f.ID, f.INA, f.AIN,
	)

}

func (f *Full) Decode(data []byte) {

	decode(
		data,
		&f.N, &f.B, &f.F, &f.S, &f.T,
		&f.N64, &f.N32, &f.N16, &f.N8,
		&f.I, &f.I8, &f.I16, &f.I32, &f.I64,
		&f.UI, &f.UI8, &f.UI16, &f.UI32, &f.UI64,
		&f.NF32, &f.NF64, &f.F32, &f.F64,
		&f.AB, &f.AS,
		&f.AI8, &f.AI16, &f.AI32, &f.AI64,
		&f.AUI8, &f.AUI16, &f.AUI32, &f.AUI64,
		&f.AF32, &f.AF64,
		&f.IN, &f.IB, &f.IF, &f.IT, &f.II, &f.ID, &f.INA, &f.AIN,
	)

}
