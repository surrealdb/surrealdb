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

package item

import (
	"fmt"
	"time"

	"github.com/yuin/gopher-lua"
)

func toLUA(L *lua.LState, value interface{}) lua.LValue {
	switch v := value.(type) {
	case bool:
		return lua.LBool(v)
	case int64:
		return lua.LNumber(v)
	case float64:
		return lua.LNumber(v)
	case string:
		return lua.LString(v)
	case time.Time:
		return lua.LNumber(v.Unix())
	case []interface{}:
		a := L.CreateTable(len(v), 0)
		for _, item := range v {
			a.Append(toLUA(L, item))
		}
		return a
	case map[string]interface{}:
		m := L.CreateTable(0, len(v))
		for key, item := range v {
			m.RawSetH(lua.LString(key), toLUA(L, item))
		}
		return m
	}
	return lua.LNil
}

func frLUA(value lua.LValue) interface{} {
	switch v := value.(type) {
	case *lua.LNilType:
		return nil
	case lua.LBool:
		return bool(v)
	case lua.LString:
		return string(v)
	case lua.LNumber:
		return float64(v)
	case *lua.LTable:
		if c := v.MaxN(); c == 0 {
			m := make(map[string]interface{})
			v.ForEach(func(key, val lua.LValue) {
				str := fmt.Sprint(frLUA(key))
				m[str] = frLUA(val)
			})
			return m
		} else {
			a := make([]interface{}, 0, c)
			for i := 1; i <= c; i++ {
				a = append(a, frLUA(v.RawGetInt(i)))
			}
			return a
		}
	default:
		return v
	}
}
