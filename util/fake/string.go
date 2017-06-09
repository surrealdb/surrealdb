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

package fake

import (
	"bytes"
	"fmt"
)

func String() string {
	return New().String()
}

func (f *Faker) String() string {
	var b bytes.Buffer
	for i := 0; i < f.IntegerBetween(1, 100); i++ {
		b.WriteString(f.Alphanum())
	}
	return b.String()
}

func StringLength(size int) string {
	return New().StringLength(size)
}

func (f *Faker) StringLength(size int) string {
	var b bytes.Buffer
	for i := 0; i < size; i++ {
		b.WriteString(f.Alphanum())
	}
	return b.String()
}

func StringBetween(beg, end int) string {
	return New().StringBetween(beg, end)
}

func (f *Faker) StringBetween(beg, end int) string {
	var b bytes.Buffer
	for i := 0; i < f.IntegerBetween(beg, end); i++ {
		b.WriteString(f.Alphanum())
	}
	return b.String()
}

func Syllable() string {
	return New().Syllable()
}

func (f *Faker) Syllable() string {
	if f.Bool() {
		switch f.Bool() {
		case true:
			return fmt.Sprintf("%s%s", f.CharVowel(), f.CharConsonant())
		case false:
			return fmt.Sprintf("%s%s", f.CharConsonant(), f.CharVowel())
		}
	}
	return fmt.Sprintf("%s%s%s", f.CharConsonant(), f.CharVowel(), f.CharConsonant())
}

func Word() string {
	return New().Word()
}

func (f *Faker) Word() string {
	var str [4]string
	for i := 0; i < 4; i++ {
		str[i] = ""
	}
	for i := 0; i < f.IntegerBetween(2, 4); i++ {
		str[i] = f.Syllable()
	}
	return fmt.Sprintf("%s%s%s%s", str[0], str[1], str[2], str[3])
}
