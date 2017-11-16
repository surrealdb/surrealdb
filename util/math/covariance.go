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

func Covariance(a, b []float64) float64 {

	var ss float64

	if len(a) == 0 {
		return math.NaN()
	}

	if len(b) == 0 {
		return math.NaN()
	}

	if len(a) != len(b) {
		return math.NaN()
	}

	l, ma, mb := Size(a), Mean(a), Mean(b)

	for i := 0; i < l; i++ {
		da := (a[i] - ma)
		db := (b[i] - mb)
		ss += (da*db - ss) / float64(i+1)
	}

	return ss * float64(l) / float64(l-1)

}

func PopulationCovariance(a, b []float64) float64 {

	var ss float64

	if len(a) == 0 {
		return math.NaN()
	}

	if len(b) == 0 {
		return math.NaN()
	}

	if len(a) != len(b) {
		return math.NaN()
	}

	l, ma, mb := Size(a), Mean(a), Mean(b)

	for i := 0; i < l; i++ {
		da := (a[i] - ma)
		db := (b[i] - mb)
		ss += da * db
	}

	return ss / float64(l)

}
