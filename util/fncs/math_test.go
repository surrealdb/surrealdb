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

func TestMath(t *testing.T) {

	var res interface{}

	var test = []interface{}{int(1), int64(3), float32(4.5), float64(3.5)}
	var testA = []interface{}{int(1), int64(3), float32(4.5), float64(3.5)}
	var testB = []interface{}{int(5), int64(9), float32(2.5), float64(6.5)}
	var testC = []interface{}{int(5), int64(5), float32(2.5), float64(6.5)}

	Convey("math.abs() works properly", t, func() {
		res, _ = Run(context.Background(), "math.abs", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.abs", 10)
		So(res, ShouldEqual, 10)
		res, _ = Run(context.Background(), "math.abs", -1.5)
		So(res, ShouldEqual, 1.5)
	})

	Convey("math.bottom() works properly", t, func() {
		res, _ = Run(context.Background(), "math.bottom", "test", 2)
		So(res, ShouldHaveLength, 0)
		res, _ = Run(context.Background(), "math.bottom", testC, "oops")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.bottom", testC, 2)
		So(res, ShouldHaveSameTypeAs, []float64{})
		So(res, ShouldContain, 2.5)
		So(res, ShouldContain, 5.0)
		res, _ = Run(context.Background(), "math.bottom", 13, 2)
		So(res, ShouldHaveSameTypeAs, []float64{})
		So(res, ShouldContain, 13.0)
	})

	Convey("math.ceil() works properly", t, func() {
		res, _ = Run(context.Background(), "math.ceil", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.ceil", 10)
		So(res, ShouldEqual, 10)
		res, _ = Run(context.Background(), "math.ceil", 1.5)
		So(res, ShouldEqual, 2)
	})

	Convey("math.correlation() works properly", t, func() {
		res, _ = Run(context.Background(), "math.correlation", "test", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.correlation", 1234, 1234)
		So(res, ShouldEqual, 0)
		res, _ = Run(context.Background(), "math.correlation", testA, testB)
		So(res, ShouldEqual, -0.24945922497781908)
	})

	Convey("math.covariance() works properly", t, func() {
		res, _ = Run(context.Background(), "math.covariance", "test", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.covariance", 1234, 1234)
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.covariance", testA, testB)
		So(res, ShouldEqual, -1)
	})

	Convey("math.fixed() works properly", t, func() {
		res, _ = Run(context.Background(), "math.fixed", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.fixed", 10, 2)
		So(res, ShouldEqual, 10)
		res, _ = Run(context.Background(), "math.fixed", 1.51837461, 2)
		So(res, ShouldEqual, 1.52)
	})

	Convey("math.floor() works properly", t, func() {
		res, _ = Run(context.Background(), "math.floor", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.floor", 10)
		So(res, ShouldEqual, 10)
		res, _ = Run(context.Background(), "math.floor", 1.5)
		So(res, ShouldEqual, 1)
	})

	Convey("math.geometricmean() works properly", t, func() {
		res, _ = Run(context.Background(), "math.geometricmean", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.geometricmean", 10)
		So(res, ShouldEqual, 10)
		res, _ = Run(context.Background(), "math.geometricmean", test)
		So(res, ShouldEqual, 2.6218053975140414)
	})

	Convey("math.harmonicmean() works properly", t, func() {
		res, _ = Run(context.Background(), "math.harmonicmean", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.harmonicmean", 10)
		So(res, ShouldEqual, 10)
		res, _ = Run(context.Background(), "math.harmonicmean", test)
		So(res, ShouldEqual, 2.172413793103449)
	})

	Convey("math.interquartile() works properly", t, func() {
		res, _ = Run(context.Background(), "math.interquartile", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.interquartile", 10)
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.interquartile", test)
		So(res, ShouldEqual, 2)
	})

	Convey("math.max() works properly", t, func() {
		res, _ = Run(context.Background(), "math.max", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.max", 10)
		So(res, ShouldEqual, 10)
		res, _ = Run(context.Background(), "math.max", test)
		So(res, ShouldEqual, 4.5)
	})

	Convey("math.mean() works properly", t, func() {
		res, _ = Run(context.Background(), "math.mean", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.mean", 10)
		So(res, ShouldEqual, 10)
		res, _ = Run(context.Background(), "math.mean", testA, testB)
		So(res, ShouldEqual, 3)
	})

	Convey("math.median() works properly", t, func() {
		res, _ = Run(context.Background(), "math.median", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.median", 10)
		So(res, ShouldEqual, 10)
		res, _ = Run(context.Background(), "math.median", testA, testB)
		So(res, ShouldEqual, 3.25)
	})

	Convey("math.midhinge() works properly", t, func() {
		res, _ = Run(context.Background(), "math.midhinge", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.midhinge", 10)
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.midhinge", test)
		So(res, ShouldEqual, 3)
	})

	Convey("math.min() works properly", t, func() {
		res, _ = Run(context.Background(), "math.min", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.min", 10)
		So(res, ShouldEqual, 10)
		res, _ = Run(context.Background(), "math.min", test)
		So(res, ShouldEqual, 1)
	})

	Convey("math.mode() works properly", t, func() {
		res, _ = Run(context.Background(), "math.mode", "test")
		So(res, ShouldHaveLength, 0)
		res, _ = Run(context.Background(), "math.mode", testC)
		So(res, ShouldResemble, []float64{5})
		res, _ = Run(context.Background(), "math.mode", 1)
		So(res, ShouldResemble, []float64{1})
	})

	Convey("math.nearestrank() works properly", t, func() {
		res, _ = Run(context.Background(), "math.nearestrank", "test", 90)
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.nearestrank", 10, "oops")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.nearestrank", 10, 90)
		So(res, ShouldEqual, 10)
		res, _ = Run(context.Background(), "math.nearestrank", test, 90)
		So(res, ShouldEqual, 4.5)
	})

	Convey("math.percentile() works properly", t, func() {
		res, _ = Run(context.Background(), "math.percentile", "test", 90)
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.percentile", 10, "oops")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.percentile", 10, 90)
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.percentile", test, 90)
		So(res, ShouldEqual, 4)
	})

	Convey("math.round() works properly", t, func() {
		res, _ = Run(context.Background(), "math.round", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.round", 1.4)
		So(res, ShouldEqual, 1)
		res, _ = Run(context.Background(), "math.round", 1.5)
		So(res, ShouldEqual, 2)
	})

	Convey("math.sample() works properly", t, func() {
		res, _ = Run(context.Background(), "math.sample", "test", 3)
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.sample", 10, "oops")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.sample", 10, 3)
		So(res, ShouldResemble, []float64{10})
		res, _ = Run(context.Background(), "math.sample", test, 3)
		So(res, ShouldHaveLength, 3)
	})

	Convey("math.spread() works properly", t, func() {
		res, _ = Run(context.Background(), "math.spread", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.spread", 10)
		So(res, ShouldEqual, 0)
		res, _ = Run(context.Background(), "math.spread", test)
		So(res, ShouldEqual, 3.5)
	})

	Convey("math.sqrt() works properly", t, func() {
		res, _ = Run(context.Background(), "math.sqrt", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.sqrt", test)
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.sqrt", 10)
		So(res, ShouldEqual, 3.1622776601683795)
	})

	Convey("math.stddev() works properly", t, func() {
		res, _ = Run(context.Background(), "math.stddev", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.stddev", 10)
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.stddev", test)
		So(res, ShouldEqual, 1.4719601443879744)
	})

	Convey("math.sum() works properly", t, func() {
		res, _ = Run(context.Background(), "math.sum", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.sum", 1234)
		So(res, ShouldEqual, 1234)
		res, _ = Run(context.Background(), "math.sum", []interface{}{int(1), int64(3), float32(4.5), float64(3.5)})
		So(res, ShouldEqual, 12)
	})

	Convey("math.top() works properly", t, func() {
		res, _ = Run(context.Background(), "math.top", "test", 2)
		So(res, ShouldHaveSameTypeAs, []float64{})
		So(res, ShouldHaveLength, 0)
		res, _ = Run(context.Background(), "math.top", testC, "oops")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.top", testC, 2)
		So(res, ShouldHaveSameTypeAs, []float64{})
		So(res, ShouldContain, 6.5)
		So(res, ShouldContain, 5.0)
		res, _ = Run(context.Background(), "math.top", 13, 2)
		So(res, ShouldHaveSameTypeAs, []float64{})
		So(res, ShouldContain, 13.0)
	})

	Convey("math.trimean() works properly", t, func() {
		res, _ = Run(context.Background(), "math.trimean", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.trimean", 10)
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.trimean", test)
		So(res, ShouldEqual, 3.125)
	})

	Convey("math.variance() works properly", t, func() {
		res, _ = Run(context.Background(), "math.variance", "test")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.variance", 10)
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "math.variance", test)
		So(res, ShouldEqual, 2.1666666666666665)
	})

}
