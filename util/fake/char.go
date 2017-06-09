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

var vowels = "aeoui"

var consonants = "bcdfghjklmnpqrstvwxyz"

var lowerchars = "abcdefghijklmnopqrstuvwxyz"

var upperchars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

var alphachars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

var alphanumbs = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func Alphanum() string {
	return New().Alphanum()
}

func (f *Faker) Alphanum() string {
	return string(alphanumbs[f.r.Intn(len(alphanumbs))])
}

func Char() string {
	return New().Char()
}

func (f *Faker) Char() string {
	return string(alphachars[f.r.Intn(len(alphachars))])
}

func CharLower() string {
	return New().CharLower()
}

func (f *Faker) CharLower() string {
	return string(lowerchars[f.r.Intn(len(lowerchars))])
}

func CharUpper() string {
	return New().CharUpper()
}

func (f *Faker) CharUpper() string {
	return string(upperchars[f.r.Intn(len(upperchars))])
}

func CharVowel() string {
	return New().CharVowel()
}

func (f *Faker) CharVowel() string {
	return string(vowels[f.r.Intn(len(vowels))])
}

func CharConsonant() string {
	return New().CharConsonant()
}

func (f *Faker) CharConsonant() string {
	return string(consonants[f.r.Intn(len(consonants))])
}
