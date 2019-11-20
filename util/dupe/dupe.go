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

package dupe

type Copyable interface {
	Copy() interface{}
}

type Cloneable interface {
	Clone() interface{}
}

func Duplicate(v interface{}) interface{} {

	switch x := v.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(x))
		for k, v := range x {
			switch y := v.(type) {
			case map[string]interface{}:
				out[k] = Duplicate(y)
			case []interface{}:
				out[k] = Duplicate(y)
			default:
				out[k] = y
			}
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(x))
		for k, v := range x {
			switch y := v.(type) {
			case map[string]interface{}:
				out[k] = Duplicate(y)
			case []interface{}:
				out[k] = Duplicate(y)
			default:
				out[k] = y
			}
		}
		return out
	case Cloneable:
		return x.Clone()
	case Copyable:
		return x.Copy()
	default:
		return x
	}

}
