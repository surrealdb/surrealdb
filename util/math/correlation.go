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

func Correlation(a, b []float64) float64 {

	if len(a) == 0 {
		return math.NaN()
	}

	if len(b) == 0 {
		return math.NaN()
	}

	if len(a) != len(b) {
		return math.NaN()
	}

	sa := PopulationStandardDeviation(a)
	sb := PopulationStandardDeviation(b)
	co := PopulationCovariance(a, b)

	if sa == 0 || sb == 0 {
		return 0
	}

	return co / (sa * sb)

}
