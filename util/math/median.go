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

package math

import "math"

func Median(vals []float64) float64 {

	if len(vals) == 0 {
		return math.NaN()
	}

	l := len(vals)

	switch {
	case l%2 == 0:
		sort := Sort(vals)
		return Mean(sort[l/2-1 : l/2+1])
	default:
		sort := Sort(vals)
		return float64(sort[l/2])
	}

}

func MedianAbsoluteDeviation(vals []float64) float64 {

	if len(vals) == 0 {
		return math.NaN()
	}

	dups := Copy(vals)

	m := Median(dups)

	for k, v := range dups {
		dups[k] = math.Abs(v - m)
	}

	return Median(dups)

}
