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

import "math"

func Mean(vals []float64) float64 {

	if len(vals) == 0 {
		return math.NaN()
	}

	return Sum(vals) / float64(len(vals))

}

func RollingMean(cnt int64, sum float64) float64 {

	return sum / float64(cnt)

}

func GeometricMean(vals []float64) float64 {

	if len(vals) == 0 {
		return math.NaN()
	}

	return math.Pow(Product(vals), 1/float64(len(vals)))

}

func HarmonicMean(vals []float64) float64 {

	var out float64

	if len(vals) == 0 {
		return math.NaN()
	}

	for _, v := range vals {
		if v < 0 {
			return math.NaN()
		} else if v == 0 {
			return math.NaN()
		}
		out += (1 / v)
	}

	return float64(len(vals)) / out

}
