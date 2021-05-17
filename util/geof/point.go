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

package geof

import (
	"strconv"

	"github.com/abcum/surreal/sql"
)

func ensureSlice(args interface{}) (out []interface{}, ok bool) {
	if i, ok := args.([]interface{}); ok {
		return i, true
	} else {
		return []interface{}{args}, false
	}
}

func ensureFloats(args interface{}) (out []float64) {
	arr, _ := ensureSlice(args)
	for _, val := range arr {
		switch val := val.(type) {
		case int:
			out = append(out, float64(val))
		case int64:
			out = append(out, float64(val))
		case uint:
			out = append(out, float64(val))
		case uint64:
			out = append(out, float64(val))
		case float32:
			out = append(out, float64(val))
		case float64:
			out = append(out, float64(val))
		case string:
			if val, err := strconv.ParseFloat(val, 64); err == nil {
				out = append(out, float64(val))
			}
		}
	}
	return
}

func Point(val interface{}) (out *sql.Point, ok bool) {
	switch val := val.(type) {
	case *sql.Point:
		return val, true
	case []interface{}:
		if p := ensureFloats(val); len(p) == 2 {
			return sql.NewPoint(p[0], p[1]), true
		}
	}
	return nil, false
}
