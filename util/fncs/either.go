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
	"context"
)

func either(ctx context.Context, args ...interface{}) (interface{}, error) {
	for _, a := range args {
		switch v := a.(type) {
		case nil:
			continue
		default:
			return v, nil
		case string:
			if v != "" {
				return v, nil
			}
		case int64:
			if v != 0 {
				return v, nil
			}
		case float64:
			if v != 0 {
				return v, nil
			}
		case []interface{}:
			if len(v) != 0 {
				return v, nil
			}
		case map[string]interface{}:
			if len(v) != 0 {
				return v, nil
			}
		}
	}
	return nil, nil
}
