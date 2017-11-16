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

func TestCount(t *testing.T) {

	var res interface{}

	var test = []interface{}{int(1), int(2), nil, float64(3.5), float64(4.5), float64(3.5)}

	Convey("count() works properly", t, func() {
		res, _ = Run(context.Background(), "count", "test")
		So(res, ShouldEqual, 1)
		res, _ = Run(context.Background(), "count", 10)
		So(res, ShouldEqual, 1)
		res, _ = Run(context.Background(), "count", test)
		So(res, ShouldEqual, 5)
	})

	Convey("count.if() works properly", t, func() {
		res, _ = Run(context.Background(), "count.if", "test", 3.5)
		So(res, ShouldEqual, 0)
		res, _ = Run(context.Background(), "count.if", 10, 3.5)
		So(res, ShouldEqual, 0)
		res, _ = Run(context.Background(), "count.if", test, 3.5)
		So(res, ShouldEqual, 2)
	})

	Convey("count.not() works properly", t, func() {
		res, _ = Run(context.Background(), "count.not", "test", 3.5)
		So(res, ShouldEqual, 1)
		res, _ = Run(context.Background(), "count.not", 10, 3.5)
		So(res, ShouldEqual, 1)
		res, _ = Run(context.Background(), "count.not", test, 3.5)
		So(res, ShouldEqual, 4)
	})

}
