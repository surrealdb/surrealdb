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

func TestPurge(t *testing.T) {

	var res interface{}

	var test = []interface{}{int(1), int(2), nil, float64(3.5), "testing string"}

	Convey("purge() works properly", t, func() {
		res, _ = Run(context.Background(), "purge", nil)
		So(res, ShouldResemble, nil)
		res, _ = Run(context.Background(), "purge", "test")
		So(res, ShouldResemble, "test")
		res, _ = Run(context.Background(), "purge", test)
		So(res, ShouldResemble, []interface{}{int(1), int(2), float64(3.5), "testing string"})
	})

	Convey("purge.if() works properly", t, func() {
		res, _ = Run(context.Background(), "purge.if", "test", nil)
		So(res, ShouldResemble, "test")
		res, _ = Run(context.Background(), "purge.if", "test", "none")
		So(res, ShouldResemble, "test")
		res, _ = Run(context.Background(), "purge.if", "test", "test")
		So(res, ShouldResemble, nil)
		res, _ = Run(context.Background(), "purge.if", test, "testing string")
		So(res, ShouldResemble, []interface{}{int(1), int(2), nil, float64(3.5)})
		res, _ = Run(context.Background(), "purge.if", test, 3.5)
		So(res, ShouldResemble, []interface{}{int(1), int(2), nil, "testing string"})
		res, _ = Run(context.Background(), "purge.if", test, nil)
		So(res, ShouldResemble, []interface{}{int(1), int(2), float64(3.5), "testing string"})
	})

	Convey("purge.not() works properly", t, func() {
		res, _ = Run(context.Background(), "purge.not", "test", nil)
		So(res, ShouldResemble, nil)
		res, _ = Run(context.Background(), "purge.not", "test", "none")
		So(res, ShouldResemble, nil)
		res, _ = Run(context.Background(), "purge.not", "test", "test")
		So(res, ShouldResemble, "test")
		res, _ = Run(context.Background(), "purge.not", test, "testing string")
		So(res, ShouldResemble, []interface{}{"testing string"})
		res, _ = Run(context.Background(), "purge.not", test, 3.5)
		So(res, ShouldResemble, []interface{}{float64(3.5)})
		res, _ = Run(context.Background(), "purge.not", test, nil)
		So(res, ShouldResemble, []interface{}{nil})
	})

}
