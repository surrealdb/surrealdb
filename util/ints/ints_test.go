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

package ints

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDiff(t *testing.T) {

	Convey("Check minimum", t, func() {
		So(Min(15, 37, 10, 23), ShouldEqual, 10)
		So(Min(15, 37, 16, 23), ShouldEqual, 15)
	})

	Convey("Check maximum", t, func() {
		So(Max(15, 37, 10, 23), ShouldEqual, 37)
		So(Max(40, 37, 16, 23), ShouldEqual, 40)
	})

	Convey("Check below", t, func() {
		So(Below(20, 10), ShouldEqual, 10)
		So(Below(20, 20), ShouldEqual, 20)
		So(Below(20, 30), ShouldEqual, 20)
	})

	Convey("Check above", t, func() {
		So(Above(20, 10), ShouldEqual, 20)
		So(Above(20, 20), ShouldEqual, 20)
		So(Above(20, 30), ShouldEqual, 30)
	})

	Convey("Check between", t, func() {
		So(Between(1, 1, 0), ShouldEqual, 1)
		So(Between(1, 1, 1), ShouldEqual, 1)
		So(Between(1, 1, 2), ShouldEqual, 1)
		So(Between(1, 10, 0), ShouldEqual, 1)
		So(Between(1, 10, 1), ShouldEqual, 1)
		So(Between(1, 10, 5), ShouldEqual, 5)
		So(Between(1, 10, 10), ShouldEqual, 10)
		So(Between(1, 10, 15), ShouldEqual, 10)
	})

}
