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
	"testing"
	"time"

	"github.com/surrealdb/surrealdb/sql"

	. "github.com/smartystreets/goconvey/convey"
)

func TestArgs(t *testing.T) {

	now := time.Now()

	args := []interface{}{
		int(1),
		int64(1),
		uint(1),
		uint64(1),
		float32(1.5),
		float64(1.5),
		string("1"),
		string("1.5"),
		string("test"),
		[]byte("test"),
		map[string]interface{}{"test": true},
	}

	good := args[0:8]

	oops := args[8:11]

	Convey("An argument will convert to string if possible", t, func() {
		for _, arg := range args {
			val, ok := ensureString(arg)
			So(ok, ShouldEqual, true)
			So(val, ShouldHaveSameTypeAs, string(""))
		}
	})

	Convey("An argument will convert to []byte if possible", t, func() {
		for _, arg := range args {
			val, ok := ensureBytes(arg)
			So(ok, ShouldEqual, true)
			So(val, ShouldHaveSameTypeAs, []byte(""))
		}
	})

	Convey("An argument will convert to int64 if possible", t, func() {
		for _, arg := range good {
			val, ok := ensureInt(arg)
			So(ok, ShouldEqual, true)
			So(val, ShouldHaveSameTypeAs, int64(0))
		}
		for _, arg := range oops {
			val, ok := ensureInt(arg)
			So(ok, ShouldEqual, false)
			So(val, ShouldHaveSameTypeAs, int64(0))
		}
	})

	Convey("An argument will convert to float64 if possible", t, func() {
		for _, arg := range good {
			val, ok := ensureFloat(arg)
			So(ok, ShouldEqual, true)
			So(val, ShouldHaveSameTypeAs, float64(0))
		}
		for _, arg := range oops {
			val, ok := ensureFloat(arg)
			So(ok, ShouldEqual, false)
			So(val, ShouldHaveSameTypeAs, float64(0))
		}
	})

	Convey("An argument will convert to time.Time if possible", t, func() {
		res, ok := ensureTime("test")
		So(ok, ShouldEqual, false)
		So(res, ShouldEqual, time.Unix(0, 0))
		one, ok := ensureTime(now)
		So(ok, ShouldEqual, true)
		So(one, ShouldEqual, now)
	})

	Convey("An argument will convert to *sql.Point if possible", t, func() {
		res, ok := ensurePoint("test")
		So(ok, ShouldEqual, false)
		So(res, ShouldEqual, nil)
		one, ok := ensurePoint(&sql.Point{})
		So(ok, ShouldEqual, true)
		So(one, ShouldResemble, &sql.Point{})
	})

	Convey("An argument will convert to *sql.Circle if possible", t, func() {
		res, ok := ensureCircle("test")
		So(ok, ShouldEqual, false)
		So(res, ShouldEqual, nil)
		one, ok := ensureCircle(&sql.Circle{})
		So(ok, ShouldEqual, true)
		So(one, ShouldResemble, &sql.Circle{})
	})

	Convey("An argument will convert to *sql.Polygon if possible", t, func() {
		res, ok := ensurePolygon("test")
		So(ok, ShouldEqual, false)
		So(res, ShouldEqual, nil)
		one, ok := ensurePolygon(&sql.Polygon{})
		So(ok, ShouldEqual, true)
		So(one, ShouldResemble, &sql.Polygon{})
	})

	Convey("An argument will convert to *sql.Polygon if possible", t, func() {
		res, ok := ensureGeometry("test")
		So(ok, ShouldEqual, false)
		So(res, ShouldEqual, nil)
		one, ok := ensureGeometry(&sql.Point{})
		So(ok, ShouldEqual, true)
		So(one, ShouldResemble, &sql.Point{})
		two, ok := ensureGeometry(&sql.Circle{})
		So(ok, ShouldEqual, true)
		So(two, ShouldResemble, &sql.Circle{})
		tre, ok := ensureGeometry(&sql.Polygon{})
		So(ok, ShouldEqual, true)
		So(tre, ShouldResemble, &sql.Polygon{})
	})

	Convey("Arguments are converted to []int64 if possible", t, func() {
		res := ensureInts(args)
		So(res, ShouldHaveSameTypeAs, []int64{})
		So(len(res), ShouldEqual, 8)
	})

	Convey("Arguments are converted to []float64 if possible", t, func() {
		res := ensureFloats(args)
		So(res, ShouldHaveSameTypeAs, []float64{})
		So(len(res), ShouldEqual, 8)
	})

	Convey("Arguments are converted to []interface{} if possible", t, func() {
		for _, arg := range args {
			res, _ := ensureSlice(arg)
			So(res, ShouldHaveSameTypeAs, []interface{}{})
		}
	})

	Convey("Arguments are converted to map[string]interface{} if possible", t, func() {
		for _, arg := range args {
			res, _ := ensureObject(arg)
			So(res, ShouldHaveSameTypeAs, map[string]interface{}{})
		}
	})

}
