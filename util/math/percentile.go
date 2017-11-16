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

func Percentile(vals []float64, percent float64) float64 {

	if len(vals) == 0 {
		return math.NaN()
	}

	if percent <= 0 || percent > 100 {
		return math.NaN()
	}

	sort := Sort(vals)
	size := Size(vals)
	indx := (percent / 100) * float64(size)

	switch {
	case indx == Whole(indx):
		i := int(indx)
		return sort[i-1]
	case indx > 1:
		i := int(indx)
		return Mean([]float64{sort[i-1], sort[i]})
	default:
		return math.NaN()
	}

}

func NearestRankPercentile(vals []float64, percent float64) float64 {

	if len(vals) == 0 {
		return math.NaN()
	}

	if percent <= 0 || percent > 100 {
		return math.NaN()
	}

	sort := Sort(vals)
	size := Size(vals)

	if percent == 100 {
		return sort[size-1]
	}

	r := int(math.Ceil(float64(size) * percent / 100))

	if r == 0 {
		return sort[0]
	}

	return sort[r-1]

}
