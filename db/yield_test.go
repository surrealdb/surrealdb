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

package db

import (
	"testing"

	"github.com/surrealdb/surrealdb/util/data"
	. "github.com/smartystreets/goconvey/convey"
)

func TestYield(t *testing.T) {

	Convey("Yield different responses when modifying a record", t, func() {

		setupDB(1)

		txt := `
		USE NS test DB test;
		CREATE person:test SET test=1 RETURN AFTER;
		UPDATE person:test SET test=2 RETURN BEFORE;
		UPDATE person:test SET test=3 RETURN NONE;
		DELETE person:test RETURN BEFORE;
		`

		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 5)
		So(res[1].Result, ShouldHaveLength, 1)
		So(data.Consume(res[1].Result[0]).Get("test").Data(), ShouldEqual, 1)
		So(res[2].Result, ShouldHaveLength, 1)
		So(data.Consume(res[2].Result[0]).Get("test").Data(), ShouldEqual, 1)
		So(res[3].Result, ShouldHaveLength, 0)
		So(res[4].Result, ShouldHaveLength, 1)
		So(data.Consume(res[4].Result[0]).Get("test").Data(), ShouldEqual, 3)

	})

}
