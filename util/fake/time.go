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

package fake

import (
	"math"
	"time"
)

func Time() time.Time {
	return New().Time()
}

func (f *Faker) Time() time.Time {
	n := int64(f.IntegerBetween(0, math.MaxInt64))
	return time.Unix(0, n)
}

func TimeBetween(beg, end time.Time) time.Time {
	return New().TimeBetween(beg, end)
}
func (f *Faker) TimeBetween(beg, end time.Time) time.Time {
	b := int(beg.UnixNano())
	e := int(end.UnixNano())
	n := int64(f.IntegerBetween(b, e))
	return time.Unix(0, n)
}
