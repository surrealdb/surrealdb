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

func Integer() int {
	return New().Integer()
}

func (f *Faker) Integer() int {
	return f.r.Int()
}

func IntegerBetween(beg, end int) int {
	return New().IntegerBetween(beg, end)
}

func (f *Faker) IntegerBetween(beg, end int) int {
	return f.r.Intn(end-beg) + beg
}

func Decimal() float64 {
	return New().Decimal()
}

func (f *Faker) Decimal() float64 {
	return f.r.NormFloat64()
}

func DecimalBetween(beg, end float64) float64 {
	return New().DecimalBetween(beg, end)
}

func (f *Faker) DecimalBetween(beg, end float64) float64 {
	return f.r.Float64()*(end-beg) + beg
}
