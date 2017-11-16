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

package fncs

import (
	"context"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSets(t *testing.T) {

	Convey("difference(a, b, c) works properly", t, func() {
		res, _ := Run(context.Background(), "difference", []interface{}{"one"}, []interface{}{"one", "two"}, []interface{}{"one", "two", "tre"})
		So(res, ShouldHaveLength, 1)
		So(res, ShouldContain, "tre")
	})

	Convey("distinct(a) works properly", t, func() {
		res, _ := Run(context.Background(), "distinct", []interface{}{"one", "two", "two", "tre", "tre", "tre"})
		So(res, ShouldHaveLength, 3)
		So(res, ShouldContain, "one")
		So(res, ShouldContain, "two")
		So(res, ShouldContain, "tre")
	})

	Convey("distinct(a, b, c) works properly", t, func() {
		res, _ := Run(context.Background(), "distinct", []interface{}{"one"}, []interface{}{"one", "two"}, []interface{}{"one", "two", "tre"})
		So(res, ShouldHaveLength, 3)
		So(res, ShouldContain, "one")
		So(res, ShouldContain, "two")
		So(res, ShouldContain, "tre")
	})

	Convey("intersect(a, b, c) works properly", t, func() {
		res, _ := Run(context.Background(), "intersect", []interface{}{"one"}, []interface{}{"one", "two"}, []interface{}{"one", "two", "tre"})
		So(res, ShouldHaveLength, 1)
		So(res, ShouldContain, "one")
	})

	Convey("union(a, b, c) works properly", t, func() {
		res, _ := Run(context.Background(), "union", []interface{}{"one"}, []interface{}{"one", "two"}, []interface{}{"one", "two", "tre"})
		So(res, ShouldHaveLength, 3)
		So(res, ShouldContain, "one")
		So(res, ShouldContain, "two")
		So(res, ShouldContain, "tre")
	})

}
