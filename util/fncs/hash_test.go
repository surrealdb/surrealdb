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

func TestHash(t *testing.T) {

	Convey("hash.md5(a) works properly", t, func() {
		res, _ := Run(context.Background(), "hash.md5", "test")
		So(res, ShouldHaveSameTypeAs, "test")
		So(res, ShouldEqual, "098f6bcd4621d373cade4e832627b4f6")
	})

	Convey("hash.sha1(a) works properly", t, func() {
		res, _ := Run(context.Background(), "hash.sha1", "test")
		So(res, ShouldHaveSameTypeAs, "test")
		So(res, ShouldEqual, "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3")
	})

	Convey("hash.sha256(a) works properly", t, func() {
		res, _ := Run(context.Background(), "hash.sha256", "test")
		So(res, ShouldHaveSameTypeAs, "test")
		So(res, ShouldEqual, "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08")
	})

	Convey("hash.sha512(a) works properly", t, func() {
		res, _ := Run(context.Background(), "hash.sha512", "test")
		So(res, ShouldHaveSameTypeAs, "test")
		So(res, ShouldEqual, "ee26b0dd4af7e749aa1a8ee3c10ae9923f618980772e473f8819a5d4940e0db27ac185f8a0e1d5f84f88bc887fd67b143732c304cc5fa9ad8e6f57f50028a8ff")
	})

}
