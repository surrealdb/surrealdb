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

package guid

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestNew(t *testing.T) {

	var str *GUID

	str = New()

	Convey(str.String(), t, func() {
		Convey("Should be a GUID", func() {
			So(str, ShouldHaveSameTypeAs, &GUID{})
		})
		Convey("Should not be nil", func() {
			So(str, ShouldNotBeNil)
		})
		Convey("Should be of length 20", func() {
			So(str.String(), ShouldHaveLength, 20)
		})
	})

}

func TestParsing(t *testing.T) {

	var str *GUID

	str = Parse("thiswillnotbeok5n4g")

	Convey("Parse thiswillnotbeok5n4g", t, func() {
		Convey("Should be nil", func() {
			So(str, ShouldBeNil)
		})
	})

	str = Parse("9m4e2mr0ui3e8a215n4g")

	Convey("Parse 9m4e2mr0ui3e8a215n4g", t, func() {
		Convey("Should be a GUID", func() {
			So(str, ShouldHaveSameTypeAs, &GUID{})
		})
		Convey("Should not be nil", func() {
			So(str, ShouldNotBeNil)
		})
		Convey("Should be of length 20", func() {
			So(str.String(), ShouldHaveLength, 20)
		})
		Convey("Should be exactly `9m4e2mr0ui3e8a215n4g`", func() {
			So(str.String(), ShouldEqual, "9m4e2mr0ui3e8a215n4g")
		})
	})

}
