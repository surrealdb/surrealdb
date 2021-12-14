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

func TestEither(t *testing.T) {

	var res interface{}

	Convey("either() works properly", t, func() {

		res, _ = Run(context.Background(), "either", nil, true)
		So(res, ShouldResemble, true)

		res, _ = Run(context.Background(), "either", int64(0), "two")
		So(res, ShouldResemble, "two")
		res, _ = Run(context.Background(), "either", int64(1), "two")
		So(res, ShouldResemble, int64(1))
		res, _ = Run(context.Background(), "either", int64(-1), "two")
		So(res, ShouldResemble, int64(-1))

		res, _ = Run(context.Background(), "either", float64(0), "two")
		So(res, ShouldResemble, "two")
		res, _ = Run(context.Background(), "either", float64(1), "two")
		So(res, ShouldResemble, float64(1))
		res, _ = Run(context.Background(), "either", float64(-1), "two")
		So(res, ShouldResemble, float64(-1))

		res, _ = Run(context.Background(), "either", "", "two")
		So(res, ShouldResemble, "two")
		res, _ = Run(context.Background(), "either", "one", "two")
		So(res, ShouldResemble, "one")

		res, _ = Run(context.Background(), "either", []interface{}{}, []interface{}{"two"})
		So(res, ShouldResemble, []interface{}{"two"})
		res, _ = Run(context.Background(), "either", []interface{}{"one"}, []interface{}{"two"})
		So(res, ShouldResemble, []interface{}{"one"})

		res, _ = Run(context.Background(), "either", map[string]interface{}{}, map[string]interface{}{"two": true})
		So(res, ShouldResemble, map[string]interface{}{"two": true})
		res, _ = Run(context.Background(), "either", map[string]interface{}{"one": true}, map[string]interface{}{"two": true})
		So(res, ShouldResemble, map[string]interface{}{"one": true})

	})

}
