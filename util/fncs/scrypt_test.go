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

func TestScrypt(t *testing.T) {

	Convey("scrypt.compare(a, b) works properly", t, func() {
		res, _ := Run(context.Background(), "scrypt.compare", "16384$8$1$8065a3ec98903c86d950840721ef945b$1f23d0d2ff5528f033161fd21ce84911f9332ac9878953139ad30b6a8c2959f2", "test")
		So(res, ShouldEqual, true)
	})

	Convey("scrypt.compare(a, b) errors properly", t, func() {
		res, _ := Run(context.Background(), "scrypt.compare", "16384$8$1$8065a3ec98903c86d950840721ef945b$1f23d0d2ff5528f033161fd21ce84911f9332ac9878953139ad30b6a8c2959f2", "wrong")
		So(res, ShouldEqual, false)
	})

	Convey("scrypt.generate(a) works properly", t, func() {
		res, _ := Run(context.Background(), "scrypt.generate", "test")
		So(res, ShouldHaveSameTypeAs, []byte("test"))
	})

}
