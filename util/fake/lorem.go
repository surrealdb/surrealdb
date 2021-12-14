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
	"strings"
)

func (f *Faker) rand() int {
	r := f.r.Float32() * 100
	switch {
	case r < 1.939:
		return 1
	case r < 19.01:
		return 2
	case r < 38.00:
		return 3
	case r < 50.41:
		return 4
	case r < 61.00:
		return 5
	case r < 70.09:
		return 6
	case r < 78.97:
		return 7
	case r < 85.65:
		return 8
	case r < 90.87:
		return 9
	case r < 95.05:
		return 10
	case r < 97.27:
		return 11
	case r < 98.67:
		return 12
	case r < 100.0:
		return 13
	}
	return 2
}

func (f *Faker) word(size int) string {
	if size < 1 {
		size = 1
	}
	if size > 13 {
		size = 13
	}
	for n := f.r.Int() % len(latin); ; n++ {
		if n >= len(latin)-1 {
			n = 0
		}
		if len(latin[n]) == size {
			return latin[n]
		}
	}
	return ""
}

func Lorem() string {
	return New().Lorem()
}

func (f *Faker) Lorem() string {
	return f.word(f.IntegerBetween(4, 13))
}

func Sentence() string {
	return New().Sentence()
}

func (f *Faker) Sentence() string {
	return f.SentenceBetween(5, 22)
}

func SentenceBetween(beg, end int) string {
	return New().SentenceBetween(beg, end)
}

func (f *Faker) SentenceBetween(beg, end int) string {
	s := f.IntegerBetween(beg, end)
	w := []string{}
	for i, c := 0, 0; i < s; i++ {
		if i == 0 {
			w = append(w, strings.Title(f.word(f.rand())))
		} else {
			w = append(w, f.word(f.rand()))
		}
		if c >= 2 || i <= 2 || i >= s-1 {
			continue
		}
		if f.r.Int()%s == 0 {
			w[i-1] += ","
			c++
		}
	}
	return strings.Join(w, " ") + "."
}

func Paragraph() string {
	return New().Paragraph()
}

func (f *Faker) Paragraph() string {
	return f.ParagraphBetween(3, 7)
}

func ParagraphBetween(beg, end int) string {
	return New().ParagraphBetween(beg, end)
}

func (f *Faker) ParagraphBetween(beg, end int) string {
	w := []string{}
	for i := 0; i < f.IntegerBetween(beg, end); i++ {
		w = append(w, f.Sentence())
	}
	return strings.Join(w, " ")
}
