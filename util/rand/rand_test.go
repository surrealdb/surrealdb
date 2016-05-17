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

package rand

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

type tester struct {
	str string
	len int
}

func TestNew(t *testing.T) {

	var tests = []tester{
		{
			str: New(0),
			len: 0,
		},
		{
			str: New(10),
			len: 10,
		},
		{
			str: New(20),
			len: 20,
		},
		{
			str: New(30),
			len: 30,
		},
		{
			str: New(99),
			len: 99,
		},
	}

	for _, test := range tests {

		Convey(test.str, t, func() {
			Convey("Should be a string", func() {
				So(test.str, ShouldHaveSameTypeAs, "")
			})
			Convey("Should be of correct length", func() {
				So(test.str, ShouldHaveLength, test.len)
			})
		})

	}

}
