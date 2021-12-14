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

import "time"

type Full struct {
	N     interface{}   // nil
	B     bool          // true
	F     bool          // false
	S     string        // string
	T     time.Time     // time.Time
	NI64  int64         // negative int64
	NI32  int32         // negative int32
	NI16  int16         // negative int16
	NI8   int8          // negative int8
	NI    int           // negative int
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
	NF64  float64       // negative float64
	NF32  float32       // negative float32
	F32   float32       // positive float32
	F64   float64       // positive float64
	AB    []bool        // bool array
	AS    []string      // string array
	AT    []time.Time   // time array
	AI    []int         // int array
	AI8   []int8        // int8 array
	AI16  []int16       // int16 array
	AI32  []int32       // int32 array
	AI64  []int64       // int64 array
	AUI   []uint        // uint array
	AUI8  []uint8       // uint8 array
	AUI16 []uint16      // uint16 array
	AUI32 []uint32      // uint32 array
	AUI64 []uint64      // uint64 array
	AF32  []float32     // float32 array
	AF64  []float64     // float64 array
	IN    interface{}   // interface{}
	IB    interface{}   // interface{} true
	IF    interface{}   // interface{} false
	IT    interface{}   // interface{} time.Time
	II    interface{}   // interface{} number
	ID    interface{}   // interface{} double
	INA   interface{}   // interface{} array
	AIN   []interface{} // []interface{} array
}

func (f *Full) Copy() Key {
	return &Full{
		N:     f.N,
		B:     f.B,
		F:     f.F,
		S:     f.S,
		T:     f.T,
		NI64:  f.NI64,
		NI32:  f.NI32,
		NI16:  f.NI16,
		NI8:   f.NI8,
		NI:    f.NI,
		I:     f.I,
		I8:    f.I8,
		I16:   f.I16,
		I32:   f.I32,
		I64:   f.I64,
		UI:    f.UI,
		UI8:   f.UI8,
		UI16:  f.UI16,
		UI32:  f.UI32,
		UI64:  f.UI64,
		NF64:  f.NF64,
		NF32:  f.NF32,
		F32:   f.F32,
		F64:   f.F64,
		AB:    f.AB,
		AS:    f.AS,
		AT:    f.AT,
		AI:    f.AI,
		AI8:   f.AI8,
		AI16:  f.AI16,
		AI32:  f.AI32,
		AI64:  f.AI64,
		AUI:   f.AUI,
		AUI8:  f.AUI8,
		AUI16: f.AUI16,
		AUI32: f.AUI32,
		AUI64: f.AUI64,
		AF32:  f.AF32,
		AF64:  f.AF64,
		IN:    f.IN,
		IB:    f.IB,
		IF:    f.IF,
		IT:    f.IT,
		II:    f.II,
		ID:    f.ID,
		INA:   f.INA,
		AIN:   f.AIN,
	}
}

func (f *Full) String() (s string) {
	return "Test key"
}

func (f *Full) Encode() []byte {

	return encode(
		f.N, f.B, f.F, f.S, f.T,
		f.NI64, f.NI32, f.NI16, f.NI8, f.NI,
		f.I, f.I8, f.I16, f.I32, f.I64,
		f.UI, f.UI8, f.UI16, f.UI32, f.UI64,
		f.NF64, f.NF32, f.F32, f.F64,
		f.AB, f.AS, f.AT,
		f.AI, f.AI8, f.AI16, f.AI32, f.AI64,
		f.AUI, f.AUI8, f.AUI16, f.AUI32, f.AUI64,
		f.AF32, f.AF64,
		f.IN, f.IB, f.IF, f.IT, f.II, f.ID, f.INA, f.AIN,
	)

}

func (f *Full) Decode(data []byte) {

	decode(
		data,
		&f.N, &f.B, &f.F, &f.S, &f.T,
		&f.NI64, &f.NI32, &f.NI16, &f.NI8, &f.NI,
		&f.I, &f.I8, &f.I16, &f.I32, &f.I64,
		&f.UI, &f.UI8, &f.UI16, &f.UI32, &f.UI64,
		&f.NF64, &f.NF32, &f.F32, &f.F64,
		&f.AB, &f.AS, &f.AT,
		&f.AI, &f.AI8, &f.AI16, &f.AI32, &f.AI64,
		&f.AUI, &f.AUI8, &f.AUI16, &f.AUI32, &f.AUI64,
		&f.AF32, &f.AF64,
		&f.IN, &f.IB, &f.IF, &f.IT, &f.II, &f.ID, &f.INA, &f.AIN,
	)

}
