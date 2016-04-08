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

package cli

import (
	"strings"

	"github.com/kr/text"
)

func flag(n string) (s string) {

	if f, ok := flags[n]; ok {

		s += "\n\n"

		if u, ok := usage[n]; !ok {

			s += indent(8, wrap(f))

			s += "\n"

		} else {

			s += indent(8, wrap(f+" For example:"))

			s += "\n"

			for _, i := range u {
				s += "\n" + strings.Repeat(" ", 12) + i
			}

			s += "\n"

		}

		// Indent default values
		s += strings.Repeat(" ", 7)

	}

	return

}

func wrap(s string) string {
	return text.Wrap(s, 71)
}

func indent(i int, s string) string {
	return text.Indent(s, strings.Repeat(" ", i))
}
