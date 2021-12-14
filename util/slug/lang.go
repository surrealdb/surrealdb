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

package slug

func init() {
	// Merge language subs with the default one
	for _, sub := range []*map[rune]string{&en, &de, &fr, &es, &nl, &pl, &gr} {
		for key, value := range defaults {
			(*sub)[key] = value
		}
	}
}

var defaults = map[rune]string{
	'"':  "",
	'\'': "",
	'’':  "",
	'‒':  "-", // figure dash
	'–':  "-", // en dash
	'—':  "-", // em dash
	'―':  "-", // horizontal bar
}

var en = map[rune]string{
	'&': "and",
	'@': "at",
}

var de = map[rune]string{
	'&': "und",
	'@': "an",
}

var fr = map[rune]string{
	'&': "et",
	'@': "a",
}

var es = map[rune]string{
	'&': "y",
	'@': "en",
}

var nl = map[rune]string{
	'&': "en",
	'@': "at",
}

var pl = map[rune]string{
	'&': "i",
	'@': "na",
}

var gr = map[rune]string{
	'&': "kai",
	'η': "i",
	'ή': "i",
	'Η': "i",
	'ι': "i",
	'ί': "i",
	'Ι': "i",
	'χ': "x",
	'Χ': "x",
}
