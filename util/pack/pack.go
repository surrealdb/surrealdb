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

package pack

import (
	"bytes"
	"reflect"
	"time"

	"encoding/gob"

	"github.com/ugorji/go/codec"

	"github.com/abcum/surreal/sql"
)

func init() {
	gob.Register(time.Time{})
	gob.Register([]interface{}{})
	gob.Register(map[string]interface{}{})
}

func Copy(src interface{}) (dst map[string]interface{}) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	dec := gob.NewDecoder(&buf)
	enc.Encode(src)
	dec.Decode(&dst)
	return
}

// ToJSON converts the data object to a JSON byte slice.
func ToJSON(src interface{}) (dst []byte) {

	var opt codec.JsonHandle
	opt.Canonical = true
	opt.CheckCircularRef = false
	opt.AsSymbols = codec.AsSymbolDefault

	codec.NewEncoderBytes(&dst, &opt).Encode(src)

	return

}

// Encode encodes the data object to a byte slice.
func ToPACK(src interface{}) (dst []byte) {

	var opt codec.MsgpackHandle
	opt.WriteExt = true
	opt.Canonical = true
	opt.RawToString = true
	opt.CheckCircularRef = false
	opt.AsSymbols = codec.AsSymbolDefault
	opt.MapType = reflect.TypeOf(map[string]interface{}(nil))

	opt.SetBytesExt(reflect.TypeOf(time.Time{}), 1, extTime{})
	opt.SetBytesExt(reflect.TypeOf(sql.All{}), 101, extSqlAll{})
	opt.SetBytesExt(reflect.TypeOf(sql.Asc{}), 102, extSqlAsc{})
	opt.SetBytesExt(reflect.TypeOf(sql.Desc{}), 103, extSqlDesc{})
	opt.SetBytesExt(reflect.TypeOf(sql.Null{}), 104, extSqlNull{})
	opt.SetBytesExt(reflect.TypeOf(sql.Void{}), 105, extSqlVoid{})
	opt.SetBytesExt(reflect.TypeOf(sql.Empty{}), 106, extSqlEmpty{})
	opt.SetBytesExt(reflect.TypeOf(sql.Ident{}), 107, extSqlIdent{})
	opt.SetBytesExt(reflect.TypeOf(sql.Table{}), 108, extSqlTable{})
	opt.SetBytesExt(reflect.TypeOf(sql.Thing{}), 109, extSqlThing{})
	opt.SetBytesExt(reflect.TypeOf(sql.Field{}), 110, extSqlField{})
	opt.SetBytesExt(reflect.TypeOf(sql.Group{}), 111, extSqlGroup{})

	codec.NewEncoderBytes(&dst, &opt).Encode(src)

	return

}

// Decode decodes the byte slice into a data object.
func FromPACK(src []byte, dst interface{}) {

	var opt codec.MsgpackHandle
	opt.WriteExt = true
	opt.Canonical = true
	opt.RawToString = true
	opt.CheckCircularRef = false
	opt.AsSymbols = codec.AsSymbolDefault
	opt.MapType = reflect.TypeOf(map[string]interface{}(nil))

	opt.SetBytesExt(reflect.TypeOf(time.Time{}), 1, extTime{})
	opt.SetBytesExt(reflect.TypeOf(sql.All{}), 101, extSqlAll{})
	opt.SetBytesExt(reflect.TypeOf(sql.Asc{}), 102, extSqlAsc{})
	opt.SetBytesExt(reflect.TypeOf(sql.Desc{}), 103, extSqlDesc{})
	opt.SetBytesExt(reflect.TypeOf(sql.Null{}), 104, extSqlNull{})
	opt.SetBytesExt(reflect.TypeOf(sql.Void{}), 105, extSqlVoid{})
	opt.SetBytesExt(reflect.TypeOf(sql.Empty{}), 106, extSqlEmpty{})
	opt.SetBytesExt(reflect.TypeOf(sql.Ident{}), 107, extSqlIdent{})
	opt.SetBytesExt(reflect.TypeOf(sql.Table{}), 108, extSqlTable{})
	opt.SetBytesExt(reflect.TypeOf(sql.Thing{}), 109, extSqlThing{})
	opt.SetBytesExt(reflect.TypeOf(sql.Field{}), 110, extSqlField{})
	opt.SetBytesExt(reflect.TypeOf(sql.Group{}), 111, extSqlGroup{})

	codec.NewDecoderBytes(src, &opt).Decode(dst)

	return

}
