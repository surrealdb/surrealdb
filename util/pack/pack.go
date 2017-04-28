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
	"github.com/abcum/cork"
)

var opt cork.Handle

func init() {
	opt.Precision = false
	opt.ArrType = []interface{}{}
	opt.MapType = map[string]interface{}{}
}

// Encode encodes a data object into a CORK.
func Encode(src interface{}) (dst []byte) {
	buf := bytes.NewBuffer(nil)
	cork.NewEncoder(buf).Options(&opt).Encode(src)
	return buf.Bytes()
}

// Decode decodes a CORK into a data object.
func Decode(src []byte, dst interface{}) {
	buf := bytes.NewReader(src)
	cork.NewDecoder(buf).Options(&opt).Decode(dst)
	return
}
