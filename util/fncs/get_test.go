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

	"github.com/surrealdb/surrealdb/sql"

	. "github.com/smartystreets/goconvey/convey"
)

func TestGet(t *testing.T) {

	var res interface{}

	var doc = map[string]interface{}{
		"object": map[string]interface{}{
			"test": "ok",
		},
		"string": "test",
		"bool":   true,
		"int":    13,
	}

	Convey("get(a, b) works properly", t, func() {
		res, _ = Run(context.Background(), "get", doc, &sql.Value{"object.test"})
		So(res, ShouldEqual, "ok")
		res, _ = Run(context.Background(), "get", doc, "object.test")
		So(res, ShouldEqual, "ok")
		res, _ = Run(context.Background(), "get", doc, "string")
		So(res, ShouldEqual, "test")
		res, _ = Run(context.Background(), "get", doc, "bool")
		So(res, ShouldEqual, true)
		res, _ = Run(context.Background(), "get", doc, "int")
		So(res, ShouldEqual, 13)
		res, _ = Run(context.Background(), "get", doc, "err")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "get", doc, nil)
		So(res, ShouldEqual, nil)
	})

}
