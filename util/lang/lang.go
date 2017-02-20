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

package lang

import (
	"regexp"
	"strings"
)

type inflection struct {
	regexp  *regexp.Regexp
	replace string
}

// Regular is a regexp find replace inflection
type Regular struct {
	find    string
	replace string
}

// Irregular is a hard replace inflection,
// containing both singular and plural forms
type Irregular struct {
	singular string
	plural   string
}

// RegularSlice is a slice of Regular inflections
type RegularSlice []Regular

// IrregularSlice is a slice of Irregular inflections
type IrregularSlice []Irregular

var pluralInflections = RegularSlice{
	{"([a-z])$", "${1}s"},
	{"s$", "s"},
	{"^(ax|test)is$", "${1}es"},
	{"(octop|vir)us$", "${1}i"},
	{"(octop|vir)i$", "${1}i"},
	{"(alias|status)$", "${1}es"},
	{"(bu)s$", "${1}ses"},
	{"(buffal|tomat)o$", "${1}oes"},
	{"([ti])um$", "${1}a"},
	{"([ti])a$", "${1}a"},
	{"sis$", "ses"},
	{"(?:([^f])fe|([lr])f)$", "${1}${2}ves"},
	{"(hive)$", "${1}s"},
	{"([^aeiouy]|qu)y$", "${1}ies"},
	{"(x|ch|ss|sh)$", "${1}es"},
	{"(matr|vert|ind)(?:ix|ex)$", "${1}ices"},
	{"^(m|l)ouse$", "${1}ice"},
	{"^(m|l)ice$", "${1}ice"},
	{"^(ox)$", "${1}en"},
	{"^(oxen)$", "${1}"},
	{"(quiz)$", "${1}zes"},
}

var singularInflections = RegularSlice{
	{"s$", ""},
	{"(ss)$", "${1}"},
	{"(n)ews$", "${1}ews"},
	{"([ti])a$", "${1}um"},
	{"((a)naly|(b)a|(d)iagno|(p)arenthe|(p)rogno|(s)ynop|(t)he)(sis|ses)$", "${1}sis"},
	{"(^analy)(sis|ses)$", "${1}sis"},
	{"([^f])ves$", "${1}fe"},
	{"(hive)s$", "${1}"},
	{"(tive)s$", "${1}"},
	{"([lr])ves$", "${1}f"},
	{"([^aeiouy]|qu)ies$", "${1}y"},
	{"(s)eries$", "${1}eries"},
	{"(m)ovies$", "${1}ovie"},
	{"(c)ookies$", "${1}ookie"},
	{"(x|ch|ss|sh)es$", "${1}"},
	{"^(m|l)ice$", "${1}ouse"},
	{"(bus)(es)?$", "${1}"},
	{"(o)es$", "${1}"},
	{"(shoe)s$", "${1}"},
	{"(cris|test)(is|es)$", "${1}is"},
	{"^(a)x[ie]s$", "${1}xis"},
	{"(octop|vir)(us|i)$", "${1}us"},
	{"(alias|status)(es)?$", "${1}"},
	{"^(ox)en", "${1}"},
	{"(vert|ind)ices$", "${1}ex"},
	{"(matr)ices$", "${1}ix"},
	{"(quiz)zes$", "${1}"},
	{"(database)s$", "${1}"},
}

var irregularInflections = IrregularSlice{
	{"person", "people"},
	{"man", "men"},
	{"child", "children"},
	{"sex", "sexes"},
	{"move", "moves"},
	{"mombie", "mombies"},
	{"criterion", "criteria"},
}

var uncountableInflections = []string{"equipment", "information", "rice", "money", "species", "series", "fish", "sheep", "jeans", "police"}

var compiledPlurals []inflection
var compiledSingles []inflection

func init() {

	compiledPlurals = []inflection{}
	compiledSingles = []inflection{}

	for _, uncountable := range uncountableInflections {
		inf := inflection{
			regexp:  regexp.MustCompile("^(?i)(" + uncountable + ")$"),
			replace: "${1}",
		}
		compiledPlurals = append(compiledPlurals, inf)
		compiledSingles = append(compiledSingles, inf)
	}

	for _, value := range irregularInflections {
		infs := []inflection{
			{regexp: regexp.MustCompile(strings.ToUpper(value.singular) + "$"), replace: strings.ToUpper(value.plural)},
			{regexp: regexp.MustCompile(strings.Title(value.singular) + "$"), replace: strings.Title(value.plural)},
			{regexp: regexp.MustCompile(value.singular + "$"), replace: value.plural},
		}
		compiledPlurals = append(compiledPlurals, infs...)
	}

	for _, value := range irregularInflections {
		infs := []inflection{
			{regexp: regexp.MustCompile(strings.ToUpper(value.plural) + "$"), replace: strings.ToUpper(value.singular)},
			{regexp: regexp.MustCompile(strings.Title(value.plural) + "$"), replace: strings.Title(value.singular)},
			{regexp: regexp.MustCompile(value.plural + "$"), replace: value.singular},
		}
		compiledSingles = append(compiledSingles, infs...)
	}

	for i := len(pluralInflections) - 1; i >= 0; i-- {
		value := pluralInflections[i]
		infs := []inflection{
			{regexp: regexp.MustCompile(strings.ToUpper(value.find)), replace: strings.ToUpper(value.replace)},
			{regexp: regexp.MustCompile(value.find), replace: value.replace},
			{regexp: regexp.MustCompile("(?i)" + value.find), replace: value.replace},
		}
		compiledPlurals = append(compiledPlurals, infs...)
	}

	for i := len(singularInflections) - 1; i >= 0; i-- {
		value := singularInflections[i]
		infs := []inflection{
			{regexp: regexp.MustCompile(strings.ToUpper(value.find)), replace: strings.ToUpper(value.replace)},
			{regexp: regexp.MustCompile(value.find), replace: value.replace},
			{regexp: regexp.MustCompile("(?i)" + value.find), replace: value.replace},
		}
		compiledSingles = append(compiledSingles, infs...)
	}

}

// Plural converts a word to its plural form
func Pluralize(str string) string {
	for _, inflection := range compiledPlurals {
		if inflection.regexp.MatchString(str) {
			return inflection.regexp.ReplaceAllString(str, inflection.replace)
		}
	}
	return str
}

// Singular converts a word to its singular form
func Singularize(str string) string {
	for _, inflection := range compiledSingles {
		if inflection.regexp.MatchString(str) {
			return inflection.regexp.ReplaceAllString(str, inflection.replace)
		}
	}
	return str
}
