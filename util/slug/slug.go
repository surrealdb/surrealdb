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

package slug

import (
	"bytes"
	"regexp"
	"strings"

	"github.com/rainycape/unidecode"
)

var (
	regexpUnicode = regexp.MustCompile("[^a-z0-9-_]")
	regexpHyphens = regexp.MustCompile("-+")
)

func Make(s string) (slug string) {
	return MakeLang(s, "en")
}

func MakeLang(s string, l string) (slug string) {

	slug = strings.TrimSpace(s)

	switch l {
	case "de":
		slug = substitute(slug, de)
	case "en":
		slug = substitute(slug, en)
	case "pl":
		slug = substitute(slug, pl)
	case "es":
		slug = substitute(slug, es)
	case "gr":
		slug = substitute(slug, gr)
	case "nl":
		slug = substitute(slug, nl)
	default:
		slug = substitute(slug, en)
	}

	// Process all non ASCII symbols
	slug = unidecode.Unidecode(slug)

	// Format the text as lower case
	slug = strings.ToLower(slug)

	// Process remaining symbols
	slug = regexpUnicode.ReplaceAllString(slug, "-")

	// Process duplicated hyphens
	slug = regexpHyphens.ReplaceAllString(slug, "-")

	// Trim leading hyphens
	slug = strings.Trim(slug, "-")

	return slug

}

func substitute(s string, sub map[rune]string) string {
	var buf bytes.Buffer
	for _, c := range s {
		if d, ok := sub[c]; ok {
			buf.WriteString(d)
		} else {
			buf.WriteRune(c)
		}
	}
	return buf.String()
}
