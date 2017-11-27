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

package uuid

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestNew(t *testing.T) {

	var str *UUID

	str = New()

	Convey(str.String(), t, func() {
		Convey("Should be a UUID", func() {
			So(str, ShouldHaveSameTypeAs, &UUID{})
		})
		Convey("Should not be nil", func() {
			So(str, ShouldNotBeNil)
		})
		Convey("Should be of length 36", func() {
			So(str.String(), ShouldHaveLength, 36)
		})
	})

}

func TestParsing(t *testing.T) {

	var str *UUID

	str = Parse("thiswill-notbe-parsed-as-successful")

	Convey("Parse thiswill-notbe-parsed-as-successful", t, func() {
		Convey("Should be nil", func() {
			So(str, ShouldBeNil)
		})
	})

	str = Parse("1400A118-2749-4605-833C-E7437488BCBF")

	Convey("Parse 1400A118-2749-4605-833C-E7437488BCBF", t, func() {
		Convey("Should be a UUID", func() {
			So(str, ShouldHaveSameTypeAs, &UUID{})
		})
		Convey("Should not be nil", func() {
			So(str, ShouldNotBeNil)
		})
		Convey("Should be of length 36", func() {
			So(str.String(), ShouldHaveLength, 36)
		})
		Convey("Should be exactly `1400a118-2749-4605-833c-e7437488bcbf`", func() {
			So(str.String(), ShouldEqual, "1400a118-2749-4605-833c-e7437488bcbf")
		})
	})

}
