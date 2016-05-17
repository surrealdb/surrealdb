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
	"log"
	"time"
)

type decoder struct {
	r *reader
}

// Decode decodes an encoded string using the unicode collation algorithm.
func decode(data []byte, items ...interface{}) {
	newDecoder(bytes.NewReader(data)).Decode(items...)
}

func newDecoder(r io.Reader) *decoder {
	return &decoder{
		r: newReader(r),
	}
}

func (d *decoder) Decode(items ...interface{}) {

	for _, item := range items {

		switch value := item.(type) {

		case *time.Time:
			*value = d.r.FindTime()

		case *bool:
			*value = d.r.FindBool()

		case *[]byte:
			*value = d.r.FindBytes()

		case *string:
			*value = d.r.FindString()

		case *int:
			*value = d.r.FindNumberInt()

		case *int8:
			*value = d.r.FindNumberInt8()

		case *int16:
			*value = d.r.FindNumberInt16()

		case *int32:
			*value = d.r.FindNumberInt32()

		case *int64:
			*value = d.r.FindNumberInt64()

		case *uint:
			*value = d.r.FindNumberUint()

		case *uint8:
			*value = d.r.FindNumberUint8()

		case *uint16:
			*value = d.r.FindNumberUint16()

		case *uint32:
			*value = d.r.FindNumberUint32()

		case *uint64:
			*value = d.r.FindNumberUint64()

		case *float32:
			*value = d.r.FindNumberFloat32()

		case *float64:
			*value = d.r.FindNumberFloat64()

		case *[]bool:
			if d.r.ReadNext(cARRAY) {
				for !d.r.ReadNext(cTERM) {
					*value = append(*value, d.r.FindBool())
				}
			}

		case *[]string:
			if d.r.ReadNext(cARRAY) {
				for !d.r.ReadNext(cTERM) {
					*value = append(*value, d.r.FindString())
				}
			}

		case *[]int:
			if d.r.ReadNext(cARRAY) {
				for !d.r.ReadNext(cTERM) {
					*value = append(*value, d.r.FindNumberInt())
				}
			}

		case *[]int8:
			if d.r.ReadNext(cARRAY) {
				for !d.r.ReadNext(cTERM) {
					*value = append(*value, d.r.FindNumberInt8())
				}
			}

		case *[]int16:
			if d.r.ReadNext(cARRAY) {
				for !d.r.ReadNext(cTERM) {
					*value = append(*value, d.r.FindNumberInt16())
				}
			}

		case *[]int32:
			if d.r.ReadNext(cARRAY) {
				for !d.r.ReadNext(cTERM) {
					*value = append(*value, d.r.FindNumberInt32())
				}
			}

		case *[]int64:
			if d.r.ReadNext(cARRAY) {
				for !d.r.ReadNext(cTERM) {
					*value = append(*value, d.r.FindNumberInt64())
				}
			}

		case *[]uint:
			if d.r.ReadNext(cARRAY) {
				for !d.r.ReadNext(cTERM) {
					*value = append(*value, d.r.FindNumberUint())
				}
			}

		case *[]uint16:
			if d.r.ReadNext(cARRAY) {
				for !d.r.ReadNext(cTERM) {
					*value = append(*value, d.r.FindNumberUint16())
				}
			}

		case *[]uint32:
			if d.r.ReadNext(cARRAY) {
				for !d.r.ReadNext(cTERM) {
					*value = append(*value, d.r.FindNumberUint32())
				}
			}

		case *[]uint64:
			if d.r.ReadNext(cARRAY) {
				for !d.r.ReadNext(cTERM) {
					*value = append(*value, d.r.FindNumberUint64())
				}
			}

		case *[]float32:
			if d.r.ReadNext(cARRAY) {
				for !d.r.ReadNext(cTERM) {
					*value = append(*value, d.r.FindNumberFloat32())
				}
			}

		case *[]float64:
			if d.r.ReadNext(cARRAY) {
				for !d.r.ReadNext(cTERM) {
					*value = append(*value, d.r.FindNumberFloat64())
				}
			}

		case *[]interface{}:

			if d.r.ReadNext(cARRAY) {
				for !d.r.ReadNext(cTERM) {
					switch fnd := d.r.FindNext(); fnd {
					default:
						log.Panicf("No item found in *[]interface{} - but found %#v %#q", fnd, fnd)
					case cNILL:
						*value = append(*value, d.r.FindNull())
					case cBOOL:
						*value = append(*value, d.r.FindBool())
					case cTIME:
						*value = append(*value, d.r.FindTime())
					case cNUMBER:
						*value = append(*value, d.r.FindNumber())
					case cSTRING, cPREFIX, cSUFFIX:
						*value = append(*value, d.r.FindString())
					}
				}
				d.r.ReadNext(cTERM)
			}

		case *interface{}:

			switch fnd := d.r.FindNext(); fnd {
			default:
				log.Panicf("No item found in *interface{} - but found %#v %#q", fnd, fnd)
			case cNILL:
				*value = d.r.FindNull()
			case cBOOL:
				*value = d.r.FindBool()
			case cTIME:
				*value = d.r.FindTime()
			case cNUMBER:
				*value = d.r.FindNumber()
			case cSTRING, cPREFIX, cSUFFIX:
				*value = d.r.FindString()
			case cARRAY:
				if d.r.ReadNext(cARRAY) {
					*value = []interface{}{}
					for !d.r.ReadNext(cTERM) {
						switch d.r.FindNext() {
						default:
							log.Panicf("No item found in *interface{}...[]interface{} - but found %#v %#q", fnd, fnd)
						case cNILL:
							*value = append((*value).([]interface{}), []interface{}{d.r.FindNull()}...)
						case cBOOL:
							*value = append((*value).([]interface{}), []interface{}{d.r.FindBool()}...)
						case cTIME:
							*value = append((*value).([]interface{}), []interface{}{d.r.FindTime()}...)
						case cNUMBER:
							*value = append((*value).([]interface{}), []interface{}{d.r.FindNumber()}...)
						case cSTRING, cPREFIX, cSUFFIX:
							*value = append((*value).([]interface{}), []interface{}{d.r.FindString()}...)
						}
					}
					d.r.ReadNext(cTERM)
				}

			}

		}

	}

}
