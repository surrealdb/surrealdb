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

func TestBcrypt(t *testing.T) {

	Convey("bcrypt.compare(a, b) works properly", t, func() {
		res, _ := Run(context.Background(), "bcrypt.compare", "$2y$10$XPMT7nWucHJK113jjomfJ.xa64/jhH7sYrRJ9/0Q2CjzBTGwejUx.", "test")
		So(res, ShouldEqual, true)
	})

	Convey("bcrypt.compare(a, b) errors properly", t, func() {
		res, _ := Run(context.Background(), "bcrypt.compare", "$2y$10$XPMT7nWucHJK113jjomfJ.xa64/jhH7sYrRJ9/0Q2CjzBTGwejUx.", "wrong")
		So(res, ShouldEqual, false)
	})

	Convey("bcrypt.generate(a) works properly", t, func() {
		res, _ := Run(context.Background(), "bcrypt.generate", "test")
		So(res, ShouldHaveSameTypeAs, []byte("test"))
	})

}
