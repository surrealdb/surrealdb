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

package nums

import (
	"fmt"
	"strconv"
	"strings"
)

func CountPlaces(v float64) int {

	s := strconv.FormatFloat(v, 'f', -1, 64)
	i := strings.IndexByte(s, '.')

	if i > -1 {
		return len(s) - i - 1
	}

	return 0

}

func FormatPlaces(v float64, p int) float64 {

	switch p {
	case 0:
		return float64(int(v))
	case 1:
		f := fmt.Sprintf("%.1f", v)
		o, _ := strconv.ParseFloat(f, 64)
		return o
	case 2:
		f := fmt.Sprintf("%.2f", v)
		o, _ := strconv.ParseFloat(f, 64)
		return o
	case 3:
		f := fmt.Sprintf("%.3f", v)
		o, _ := strconv.ParseFloat(f, 64)
		return o
	case 4:
		f := fmt.Sprintf("%.4f", v)
		o, _ := strconv.ParseFloat(f, 64)
		return o
	default:
		f := fmt.Sprintf("%.5f", v)
		o, _ := strconv.ParseFloat(f, 64)
		return o
	}

}
