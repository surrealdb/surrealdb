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

package data

import (
	"time"
)

type extTime struct{}

func (x extTime) ReadExt(dst interface{}, src []byte) {
	dst.(*time.Time).UnmarshalBinary(src)
}

func (x extTime) WriteExt(src interface{}) (dst []byte) {
	switch obj := src.(type) {
	case time.Time:
		dst, _ = obj.MarshalBinary()
	case *time.Time:
		dst, _ = obj.MarshalBinary()
	}
	return
}

func (x extTime) UpdateExt(dest interface{}, v interface{}) {
	tt := dest.(*time.Time)
	switch v2 := v.(type) {
	case int64:
		*tt = time.Unix(0, v2).UTC()
	case uint64:
		*tt = time.Unix(0, int64(v2)).UTC()
	}
}

func (x extTime) ConvertExt(v interface{}) interface{} {
	switch v2 := v.(type) {
	case time.Time:
		return v2.UTC().UnixNano()
	case *time.Time:
		return v2.UTC().UnixNano()
	default:
		return nil
	}
}
