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

package pack

import (
	"github.com/surrealdb/cork"
)

var opt = cork.Handle{
	SortMaps: true,
	ArrType:  make([]interface{}, 0),
	MapType:  make(map[string]interface{}),
}

// Encode encodes a data object into a CORK.
func Encode(src interface{}) (dst []byte) {
	enc := cork.NewEncoderBytesFromPool(&dst)
	enc.Options(&opt)
	err := enc.Encode(src)
	enc.Reset()
	if err != nil {
		panic(err)
	}
	return
}

// Decode decodes a CORK into a data object.
func Decode(src []byte, dst interface{}) {
	dec := cork.NewDecoderBytesFromPool(src)
	dec.Options(&opt)
	err := dec.Decode(dst)
	dec.Reset()
	if err != nil {
		panic(err)
	}
	return
}
