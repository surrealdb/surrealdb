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

func Quartile(vals []float64) (float64, float64, float64) {

	if len(vals) == 0 {
		return math.NaN(), math.NaN(), math.NaN()
	}

	l := len(vals)

	switch {
	case l%2 == 0:
		c1 := l / 2
		c2 := l / 2
		sort := Sort(vals)
		return Median(sort[:c1]), Median(sort), Median(sort[c2:])
	default:
		c1 := (l - 1) / 2
		c2 := c1 + 1
		sort := Sort(vals)
		return Median(sort[:c1]), Median(sort), Median(sort[c2:])
	}

}

func InterQuartileRange(vals []float64) float64 {

	if len(vals) == 0 {
		return math.NaN()
	}

	q1, _, q3 := Quartile(vals)

	return q3 - q1

}
