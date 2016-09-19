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

	"github.com/abcum/surreal/sql"
)

type extSqlOrder struct{}

func (x extSqlOrder) ReadExt(dst interface{}, src []byte) {
	buf := bytes.NewBuffer(src)
	dec := gob.NewDecoder(buf)
	dec.Decode(dst.(*sql.Order))
	return
}

func (x extSqlOrder) WriteExt(src interface{}) (dst []byte) {
	buf := bytes.NewBuffer(nil)
	switch obj := src.(type) {
	case sql.Order:
		enc := gob.NewEncoder(buf)
		enc.Encode(obj)
	case *sql.Order:
		enc := gob.NewEncoder(buf)
		enc.Encode(obj)
	}
	return buf.Bytes()
}
