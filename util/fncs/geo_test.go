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

func TestGeo(t *testing.T) {

	var res interface{}

	Convey("geo.point(a, b) works properly", t, func() {
		res, _ = Run(context.Background(), "geo.point", "test", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "geo.point", &sql.Point{38.898556, -77.037852})
		So(res, ShouldResemble, &sql.Point{38.898556, -77.037852})
		res, _ = Run(context.Background(), "geo.point", []interface{}{38.898556, -77.037852})
		So(res, ShouldResemble, &sql.Point{38.898556, -77.037852})
		res, _ = Run(context.Background(), "geo.point", 38.898556, -77.037852)
		So(res, ShouldResemble, &sql.Point{38.898556, -77.037852})
	})

	Convey("geo.circle(a, b) works properly", t, func() {
		res, _ = Run(context.Background(), "geo.circle", "test", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "geo.circle", &sql.Point{38.898556, -77.037852}, 100)
		So(res, ShouldResemble, &sql.Circle{&sql.Point{38.898556, -77.037852}, 100})
		res, _ = Run(context.Background(), "geo.circle", []interface{}{38.898556, -77.037852}, 100)
		So(res, ShouldResemble, &sql.Circle{&sql.Point{38.898556, -77.037852}, 100})
	})

	Convey("geo.polygon(a, b) works properly", t, func() {
		res, _ = Run(context.Background(), "geo.polygon", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "geo.polygon", "test", "test", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "geo.polygon", "test", &sql.Point{}, &sql.Point{}, &sql.Point{})
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "geo.polygon", &sql.Point{}, &sql.Point{}, &sql.Point{}, &sql.Point{})
		So(res, ShouldResemble, &sql.Polygon{sql.Points{&sql.Point{}, &sql.Point{}, &sql.Point{}, &sql.Point{}}})
	})

	Convey("geo.distance(a, b, c, d) works properly", t, func() {
		res, _ = Run(context.Background(), "geo.distance", &sql.Point{38.898556, -77.037852}, &sql.Point{38.897147, -77.043934})
		So(res, ShouldEqual, 549.1557912048178)
	})

	Convey("geo.distance(a, b, c, d) errors properly", t, func() {
		res, _ = Run(context.Background(), "geo.distance", &sql.Point{38.898556, -77.037852}, "null")
		So(res, ShouldEqual, -1)
	})

	Convey("geo.hash.decode(a) works properly", t, func() {
		res, _ = Run(context.Background(), "geo.hash.decode", "dqcjq")
		So(res, ShouldResemble, &sql.Point{38.91357421875, -77.05810546875})
		res, _ = Run(context.Background(), "geo.hash.decode", "dqcjqcq8x")
		So(res, ShouldResemble, &sql.Point{38.9230709412368, -77.06750950572314})
	})

	Convey("geo.hash.encode(a, b, c) works properly", t, func() {
		res, _ = Run(context.Background(), "geo.hash.encode", &sql.Point{38.898556, -77.037852}, 5)
		So(res, ShouldEqual, "dqcjq")
		res, _ = Run(context.Background(), "geo.hash.encode", &sql.Point{38.898556, -77.037852}, 9)
		So(res, ShouldEqual, "dqcjqcq8x")
	})

	Convey("geo.hash.encode(a, b, c) errors properly", t, func() {
		res, _ = Run(context.Background(), "geo.hash.encode", 0, 0, "null")
		So(res, ShouldEqual, "")
	})

}
