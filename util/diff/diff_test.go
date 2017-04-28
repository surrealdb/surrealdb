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

package diff

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMain(t *testing.T) {

	Convey("Main", t, func() {
		So(nil, ShouldBeNil)
	})

}

func BenchmarkFib1(b *testing.B) {

	old := map[string]interface{}{
		"age": 18,
		"name": map[string]interface{}{
			"first": "T",
			"last":  "M H",
		},
		"chainging": true,
	}

	now := map[string]interface{}{
		"age": 29,
		"name": map[string]interface{}{
			"first": "Tobie",
			"last":  "Morgan Hitchcock",
		},
		"changing": "This is a string",
	}

	for n := 0; n < b.N; n++ {
		Diff(old, now)
	}

}
