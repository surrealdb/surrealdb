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

package fncs

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/abcum/surreal/sql"
)

var defaultTime = time.Unix(0, 0)

func outputFloat(val float64) (interface{}, error) {
	switch {
	case math.IsNaN(val):
		return nil, nil
	case math.IsInf(val, 0):
		return nil, nil
	default:
		return val, nil
	}
}

func ensureString(val interface{}) (out string, ok bool) {
	switch val := val.(type) {
	case string:
		return val, true
	case nil:
		return "", true
	default:
		return fmt.Sprint(val), true
	}
}

func ensureBytes(val interface{}) (out []byte, ok bool) {
	switch val := val.(type) {
	case []byte:
		return val, true
	case string:
		return []byte(val), true
	case nil:
		return []byte(""), true
	default:
		return []byte(fmt.Sprint(val)), true
	}
}

func ensureBool(val interface{}) (out bool, ok bool) {
	switch val := val.(type) {
	case bool:
		return val, true
	case string:
		if val, err := strconv.ParseBool(val); err == nil {
			return val, true
		}
	}
	return false, false
}

func ensureInt(val interface{}) (out int64, ok bool) {
	switch val := val.(type) {
	case int:
		return int64(val), true
	case int64:
		return int64(val), true
	case uint:
		return int64(val), true
	case uint64:
		return int64(val), true
	case float32:
		return int64(val), true
	case float64:
		return int64(val), true
	case string:
		if val, err := strconv.ParseFloat(val, 64); err == nil {
			return int64(val), true
		}
	}
	return 0, false
}

func ensureFloat(val interface{}) (out float64, ok bool) {
	switch val := val.(type) {
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return float64(val), true
	case string:
		if val, err := strconv.ParseFloat(val, 64); err == nil {
			return float64(val), true
		}
	}
	return 0, false
}

func ensureTime(val interface{}) (out time.Time, ok bool) {
	switch val := val.(type) {
	case time.Time:
		return val, true
	}
	return defaultTime, false
}

func ensurePoint(val interface{}) (out *sql.Point, ok bool) {
	switch val := val.(type) {
	case *sql.Point:
		return val, true
	}
	return nil, false
}

func ensureCircle(val interface{}) (out *sql.Circle, ok bool) {
	switch val := val.(type) {
	case *sql.Circle:
		return val, true
	}
	return nil, false
}

func ensurePolygon(val interface{}) (out *sql.Polygon, ok bool) {
	switch val := val.(type) {
	case *sql.Polygon:
		return val, true
	}
	return nil, false
}

func ensureGeometry(val interface{}) (out sql.Expr, ok bool) {
	switch val := val.(type) {
	case *sql.Point:
		return val, true
	case *sql.Circle:
		return val, true
	case *sql.Polygon:
		return val, true
	}
	return nil, false
}

func ensureDuration(val interface{}) (out time.Duration, ok bool) {
	switch val := val.(type) {
	case time.Duration:
		return val, true
	}
	return 0, false
}

func ensureSlice(args interface{}) (out []interface{}, ok bool) {
	if i, ok := args.([]interface{}); ok {
		out = i
	} else {
		out = []interface{}{args}
	}
	return out, true
}

func ensureObject(args interface{}) (out map[string]interface{}, ok bool) {
	if i, ok := args.(map[string]interface{}); ok {
		out = i
	} else {
		out = map[string]interface{}{}
	}
	return out, true
}

func ensureInts(args interface{}) (out []int64) {
	arr, _ := ensureSlice(args)
	for _, val := range arr {
		switch val := val.(type) {
		case int:
			out = append(out, int64(val))
		case int64:
			out = append(out, int64(val))
		case uint:
			out = append(out, int64(val))
		case uint64:
			out = append(out, int64(val))
		case float32:
			out = append(out, int64(val))
		case float64:
			out = append(out, int64(val))
		case string:
			if val, err := strconv.ParseFloat(val, 64); err == nil {
				out = append(out, int64(val))
			}
		}
	}
	return
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
