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

func TestIs(t *testing.T) {

	var res interface{}

	Convey("is.alpha(a) works properly", t, func() {
		res, _ = Run(context.Background(), "is.alpha", nil)
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.alpha", "test-©")
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.alpha", "aBcDe")
		So(res, ShouldEqual, true)
	})

	Convey("is.alphanum(a) works properly", t, func() {
		res, _ = Run(context.Background(), "is.alphanum", nil)
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.alphanum", "test-©")
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.alphanum", "aB3De")
		So(res, ShouldEqual, true)
	})

	Convey("is.ascii(a) works properly", t, func() {
		res, _ = Run(context.Background(), "is.ascii", nil)
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.ascii", "testing®")
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.ascii", "aB3De")
		So(res, ShouldEqual, true)
	})

	Convey("is.domain(a) works properly", t, func() {
		res, _ = Run(context.Background(), "is.domain", nil)
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.domain", "test-©")
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.domain", "abcum.com")
		So(res, ShouldEqual, true)
	})

	Convey("is.email(a) works properly", t, func() {
		res, _ = Run(context.Background(), "is.email", nil)
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.email", "test-©")
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.email", "info@abcum.com")
		So(res, ShouldEqual, true)
	})

	Convey("is.hexadecimal(a) works properly", t, func() {
		res, _ = Run(context.Background(), "is.hexadecimal", nil)
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.hexadecimal", "test-©")
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.hexadecimal", "00bfff")
		So(res, ShouldEqual, true)
	})

	Convey("is.latitude(a) works properly", t, func() {
		res, _ = Run(context.Background(), "is.latitude", nil)
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.latitude", "test-©")
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.latitude", 0)
		So(res, ShouldEqual, true)
		res, _ = Run(context.Background(), "is.latitude", -90)
		So(res, ShouldEqual, true)
		res, _ = Run(context.Background(), "is.latitude", +90)
		So(res, ShouldEqual, true)
		res, _ = Run(context.Background(), "is.latitude", "-90")
		So(res, ShouldEqual, true)
		res, _ = Run(context.Background(), "is.latitude", "+90")
		So(res, ShouldEqual, true)
		res, _ = Run(context.Background(), "is.latitude", -95)
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.latitude", +95)
		So(res, ShouldEqual, false)
	})

	Convey("is.longitude(a) works properly", t, func() {
		res, _ = Run(context.Background(), "is.longitude", nil)
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.longitude", "test-©")
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.longitude", 0)
		So(res, ShouldEqual, true)
		res, _ = Run(context.Background(), "is.longitude", -180)
		So(res, ShouldEqual, true)
		res, _ = Run(context.Background(), "is.longitude", +180)
		So(res, ShouldEqual, true)
		res, _ = Run(context.Background(), "is.longitude", "-180")
		So(res, ShouldEqual, true)
		res, _ = Run(context.Background(), "is.longitude", "+180")
		So(res, ShouldEqual, true)
		res, _ = Run(context.Background(), "is.longitude", -185)
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.longitude", +185)
		So(res, ShouldEqual, false)
	})

	Convey("is.numeric(a) works properly", t, func() {
		res, _ = Run(context.Background(), "is.numeric", nil)
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.numeric", "test-©")
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.numeric", "123456")
		So(res, ShouldEqual, true)
	})

	Convey("is.semver(a) works properly", t, func() {
		res, _ = Run(context.Background(), "is.semver", nil)
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.semver", "test-©")
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.semver", "1.0.32")
		So(res, ShouldEqual, true)
	})

	Convey("is.uuid(a) works properly", t, func() {
		res, _ = Run(context.Background(), "is.uuid", nil)
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.uuid", "test-©")
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "is.uuid", "8ddb11e8-755f-47cf-a84f-8033d1cfa1b9")
		So(res, ShouldEqual, true)
	})

}
