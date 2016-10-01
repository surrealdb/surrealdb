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
	"encoding/gob"
	"github.com/abcum/cork"
	"github.com/abcum/surreal/sql"
)

func init() {

	gob.Register(sql.Null{})
	gob.Register(sql.Void{})
	gob.Register(sql.Empty{})
	gob.Register(sql.Field{})
	gob.Register(sql.Group{})
	gob.Register(sql.Order{})
	gob.Register(sql.Ident{})
	gob.Register(sql.Table{})
	gob.Register(sql.Thing{})
	gob.Register(sql.DiffExpression{})
	gob.Register(sql.MergeExpression{})
	gob.Register(sql.ContentExpression{})
	gob.Register(sql.SelectStatement{})
	gob.Register(sql.CreateStatement{})
	gob.Register(sql.UpdateStatement{})
	gob.Register(sql.ModifyStatement{})
	gob.Register(sql.DeleteStatement{})
	gob.Register(sql.RelateStatement{})
	gob.Register(sql.RecordStatement{})
	gob.Register(sql.DefineViewStatement{})
	gob.Register(sql.DefineTableStatement{})
	gob.Register(sql.DefineRulesStatement{})
	gob.Register(sql.DefineFieldStatement{})
	gob.Register(sql.DefineIndexStatement{})

}

// Encode encodes a data object into a GOB.
func Encode(src interface{}) (dst []byte) {
	buf := bytes.NewBuffer(nil)
	// gob.NewEncoder(buf).Encode(src)
	cork.NewEncoder(buf).Encode(src)
	return buf.Bytes()
}

// Decode decodes a GOB into a data object.
func Decode(src []byte, dst interface{}) {
	buf := bytes.NewBuffer(src)
	// gob.NewDecoder(buf).Decode(&dst)
	cork.NewDecoder(buf).Decode(dst)
	return
}
