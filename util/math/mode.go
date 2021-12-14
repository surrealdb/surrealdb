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

package math

import "sort"

func Mode(vals []float64) []float64 {

	var out []float64

	switch len(vals) {
	case 1:
		return vals
	case 0:
		return nil
	}

	if !sort.Float64sAreSorted(vals) {
		vals = Sort(vals)
	}

	out = make([]float64, 5)

	cnt, max := 1, 1

	for i := 1; i < len(vals); i++ {
		switch {
		case vals[i] == vals[i-1]:
			cnt++
		case cnt == max && max != 1:
			out = append(out, vals[i-1])
			cnt = 1
		case cnt > max:
			out = append(out[:0], vals[i-1])
			max, cnt = cnt, 1
		default:
			cnt = 1
		}
	}

	switch {
	case cnt == max:
		out = append(out, vals[len(vals)-1])
	case cnt > max:
		out = append(out[:0], vals[len(vals)-1])
		max = cnt
	}

	if max == 1 {
		return nil
	}

	return out
}
