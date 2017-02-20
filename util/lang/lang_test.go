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
	"strings"
	"testing"
)

var inflections = map[string]string{
	"star":        "stars",
	"STAR":        "STARS",
	"Star":        "Stars",
	"bus":         "buses",
	"fish":        "fish",
	"mouse":       "mice",
	"query":       "queries",
	"ability":     "abilities",
	"agency":      "agencies",
	"movie":       "movies",
	"archive":     "archives",
	"index":       "indices",
	"wife":        "wives",
	"safe":        "saves",
	"half":        "halves",
	"move":        "moves",
	"salesperson": "salespeople",
	"person":      "people",
	"spokesman":   "spokesmen",
	"man":         "men",
	"woman":       "women",
	"basis":       "bases",
	"diagnosis":   "diagnoses",
	"diagnosis_a": "diagnosis_as",
	"datum":       "data",
	"medium":      "media",
	"stadium":     "stadia",
	"analysis":    "analyses",
	"node_child":  "node_children",
	"child":       "children",
	"experience":  "experiences",
	"day":         "days",
	"comment":     "comments",
	"foobar":      "foobars",
	"newsletter":  "newsletters",
	"old_news":    "old_news",
	"news":        "news",
	"series":      "series",
	"species":     "species",
	"quiz":        "quizzes",
	"perspective": "perspectives",
	"ox":          "oxen",
	"photo":       "photos",
	"buffalo":     "buffaloes",
	"tomato":      "tomatoes",
	"dwarf":       "dwarves",
	"elf":         "elves",
	"information": "information",
	"equipment":   "equipment",
	"criterion":   "criteria",
}

// storage is used to restore the state of the global variables
// on each test execution, to ensure no global state pollution
type storage struct {
	singulars    RegularSlice
	plurals      RegularSlice
	irregulars   IrregularSlice
	uncountables []string
}

var backup = storage{}

func TestPluralize(t *testing.T) {
	for key, value := range inflections {
		if v := Pluralize(strings.ToUpper(key)); v != strings.ToUpper(value) {
			t.Errorf("%v's plural should be %v, but got %v", strings.ToUpper(key), strings.ToUpper(value), v)
		}

		if v := Pluralize(strings.Title(key)); v != strings.Title(value) {
			t.Errorf("%v's plural should be %v, but got %v", strings.Title(key), strings.Title(value), v)
		}

		if v := Pluralize(key); v != value {
			t.Errorf("%v's plural should be %v, but got %v", key, value, v)
		}
	}
}

func TestSingularize(t *testing.T) {
	for key, value := range inflections {
		if v := Singularize(strings.ToUpper(value)); v != strings.ToUpper(key) {
			t.Errorf("%v's singular should be %v, but got %v", strings.ToUpper(value), strings.ToUpper(key), v)
		}

		if v := Singularize(strings.Title(value)); v != strings.Title(key) {
			t.Errorf("%v's singular should be %v, but got %v", strings.Title(value), strings.Title(key), v)
		}

		if v := Singularize(value); v != key {
			t.Errorf("%v's singular should be %v, but got %v", value, key, v)
		}
	}
}
