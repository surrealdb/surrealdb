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

package fncs

import (
	"context"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestUrl(t *testing.T) {

	Convey("url.domain(a) works properly", t, func() {
		res, _ := Run(context.Background(), "url.domain", "https://abcum.com:8000/path/to/file")
		So(res, ShouldHaveSameTypeAs, "test")
		So(res, ShouldEqual, "abcum.com")
	})

	Convey("url.domain(a) errors properly", t, func() {
		res, _ := Run(context.Background(), "url.domain", "test")
		So(res, ShouldHaveSameTypeAs, "test")
		So(res, ShouldEqual, "")
	})

	Convey("url.host(a) works properly", t, func() {
		res, _ := Run(context.Background(), "url.host", "https://abcum.com:8000/path/to/file")
		So(res, ShouldHaveSameTypeAs, "test")
		So(res, ShouldEqual, "abcum.com")
	})

	Convey("url.host(a) errors properly", t, func() {
		res, _ := Run(context.Background(), "url.host", "test")
		So(res, ShouldHaveSameTypeAs, "test")
		So(res, ShouldEqual, "")
	})

	Convey("url.port(a) works properly", t, func() {
		res, _ := Run(context.Background(), "url.port", "https://abcum.com:8000/path/to/file")
		So(res, ShouldHaveSameTypeAs, "test")
		So(res, ShouldEqual, "8000")
	})

	Convey("url.port(a) errors properly", t, func() {
		res, _ := Run(context.Background(), "url.port", "test")
		So(res, ShouldHaveSameTypeAs, "test")
		So(res, ShouldEqual, "")
	})

	Convey("url.path(a) works properly", t, func() {
		res, _ := Run(context.Background(), "url.path", "https://abcum.com:8000/path/to/file")
		So(res, ShouldHaveSameTypeAs, "test")
		So(res, ShouldEqual, "/path/to/file")
	})

	Convey("url.path(a) errors properly", t, func() {
		res, _ := Run(context.Background(), "url.path", "test")
		So(res, ShouldHaveSameTypeAs, "test")
		So(res, ShouldEqual, "")
	})

}
