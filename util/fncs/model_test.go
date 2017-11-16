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

	"github.com/abcum/surreal/sql"
	. "github.com/smartystreets/goconvey/convey"
)

func TestModel(t *testing.T) {

	var res interface{}

	Convey("model(a) errors properly", t, func() {
		res, _ = Run(context.Background(), "model", "test")
		So(res, ShouldBeNil)
	})

	Convey("model(a, b) works properly", t, func() {
		res, _ = Run(context.Background(), "model", 1, 10)
		So(res, ShouldResemble, sql.NewModel("1", 0, 0, 10))
		res, _ = Run(context.Background(), "model", "test", 10)
		So(res, ShouldResemble, sql.NewModel("test", 0, 0, 10))
	})

	Convey("model(a, b, c) works properly", t, func() {
		res, _ = Run(context.Background(), "model", 1, 10, 20)
		So(res, ShouldResemble, sql.NewModel("1", 10, 1, 20))
		res, _ = Run(context.Background(), "model", "test", 10, 20)
		So(res, ShouldResemble, sql.NewModel("test", 10, 1, 20))
	})

	Convey("model(a, b, c, d) works properly", t, func() {
		res, _ = Run(context.Background(), "model", 1, 10, 0.5, 20)
		So(res, ShouldResemble, sql.NewModel("1", 10, 0.5, 20))
		res, _ = Run(context.Background(), "model", "test", 10, 0.5, 20)
		So(res, ShouldResemble, sql.NewModel("test", 10, 0.5, 20))
	})

}
