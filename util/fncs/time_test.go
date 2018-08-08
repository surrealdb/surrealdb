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
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestTime(t *testing.T) {

	now := time.Now()
	org := time.Unix(0, 0)
	dur := 24 * time.Hour
	old, _ := time.Parse(time.RFC822Z, time.RFC822Z)
	old = old.UTC()
	rnd, _ := time.Parse("2006-01-02", "2006-01-03")
	rnd = rnd.UTC()
	trc, _ := time.Parse("2006-01-02", "2006-01-02")
	trc = trc.UTC()

	Convey("time.now() works properly", t, func() {
		res, _ := Run(context.Background(), "time.now")
		So(res, ShouldHaveSameTypeAs, now)
	})

	Convey("time.add(a, b) works properly", t, func() {
		dur, _ := time.ParseDuration("1h")
		res, _ := Run(context.Background(), "time.add", now, dur)
		So(res, ShouldHaveSameTypeAs, now)
		So(res, ShouldHappenAfter, now)
	})

	Convey("time.add(a, b) errors properly", t, func() {
		res, _ := Run(context.Background(), "time.add", now, nil)
		So(res, ShouldHaveSameTypeAs, nil)
		So(res, ShouldEqual, nil)
	})

	Convey("time.age(a, b) works properly", t, func() {
		dur, _ := time.ParseDuration("1h")
		res, _ := Run(context.Background(), "time.age", now, dur)
		So(res, ShouldHaveSameTypeAs, now)
		So(res, ShouldHappenBefore, now)
	})

	Convey("time.age(a, b) errors properly", t, func() {
		res, _ := Run(context.Background(), "time.age", now, nil)
		So(res, ShouldHaveSameTypeAs, nil)
		So(res, ShouldEqual, nil)
	})

	Convey("time.floor(a,b) works properly", t, func() {
		res, _ := Run(context.Background(), "time.floor", old, dur)
		So(res, ShouldHaveSameTypeAs, org)
		So(res, ShouldEqual, trc)
	})

	Convey("time.floor(a,b) errors properly", t, func() {
		res, _ := Run(context.Background(), "time.floor", "one", "two")
		So(res, ShouldEqual, nil)
	})

	Convey("time.round(a,b) works properly", t, func() {
		res, _ := Run(context.Background(), "time.round", old, dur)
		So(res, ShouldHaveSameTypeAs, org)
		So(res, ShouldEqual, rnd)
	})

	Convey("time.round(a,b) errors properly", t, func() {
		res, _ := Run(context.Background(), "time.round", "one", "two")
		So(res, ShouldEqual, nil)
	})

	Convey("time.day() works properly", t, func() {
		res, _ := Run(context.Background(), "time.day")
		So(res, ShouldHaveSameTypeAs, float64(0))
		So(res, ShouldEqual, now.Day())
	})

	Convey("time.day(a) works properly", t, func() {
		res, _ := Run(context.Background(), "time.day", old)
		So(res, ShouldHaveSameTypeAs, float64(0))
		So(res, ShouldEqual, 2)
	})

	Convey("time.day(a,b,c) errors properly", t, func() {
		res, _ := Run(context.Background(), "time.day", "one", "two")
		So(res, ShouldHaveSameTypeAs, nil)
		So(res, ShouldEqual, nil)
	})

	Convey("time.hour() works properly", t, func() {
		res, _ := Run(context.Background(), "time.hour")
		So(res, ShouldHaveSameTypeAs, float64(0))
		So(res, ShouldEqual, now.Hour())
	})

	Convey("time.hour(a) works properly", t, func() {
		res, _ := Run(context.Background(), "time.hour", old)
		So(res, ShouldHaveSameTypeAs, float64(0))
		So(res, ShouldEqual, 22)
	})

	Convey("time.hour(a,b,c) errors properly", t, func() {
		res, _ := Run(context.Background(), "time.hour", "one", "two")
		So(res, ShouldHaveSameTypeAs, nil)
		So(res, ShouldEqual, nil)
	})

	Convey("time.mins() works properly", t, func() {
		res, _ := Run(context.Background(), "time.mins")
		So(res, ShouldHaveSameTypeAs, float64(0))
		So(res, ShouldEqual, now.Minute())
	})

	Convey("time.mins(a) works properly", t, func() {
		res, _ := Run(context.Background(), "time.mins", old)
		So(res, ShouldHaveSameTypeAs, float64(0))
		So(res, ShouldEqual, 4)
	})

	Convey("time.mins(a,b,c) errors properly", t, func() {
		res, _ := Run(context.Background(), "time.mins", "one", "two")
		So(res, ShouldHaveSameTypeAs, nil)
		So(res, ShouldEqual, nil)
	})

	Convey("time.month() works properly", t, func() {
		res, _ := Run(context.Background(), "time.month")
		So(res, ShouldHaveSameTypeAs, float64(0))
		So(res, ShouldEqual, now.Month())
	})

	Convey("time.month(a) works properly", t, func() {
		res, _ := Run(context.Background(), "time.month", old)
		So(res, ShouldHaveSameTypeAs, float64(0))
		So(res, ShouldEqual, 1)
	})

	Convey("time.month(a,b,c) errors properly", t, func() {
		res, _ := Run(context.Background(), "time.month", "one", "two")
		So(res, ShouldHaveSameTypeAs, nil)
		So(res, ShouldEqual, nil)
	})

	Convey("time.nano() works properly", t, func() {
		res, _ := Run(context.Background(), "time.nano")
		So(res, ShouldHaveSameTypeAs, float64(0))
		So(res, ShouldBeGreaterThanOrEqualTo, now.UnixNano())
	})

	Convey("time.nano(a) works properly", t, func() {
		res, _ := Run(context.Background(), "time.nano", old)
		So(res, ShouldHaveSameTypeAs, float64(0))
		So(res, ShouldEqual, 1136239440000000000)
	})

	Convey("time.nano(a,b,c) errors properly", t, func() {
		res, _ := Run(context.Background(), "time.nano", "one", "two")
		So(res, ShouldHaveSameTypeAs, nil)
		So(res, ShouldEqual, nil)
	})

	Convey("time.secs() works properly", t, func() {
		res, _ := Run(context.Background(), "time.secs")
		So(res, ShouldHaveSameTypeAs, float64(0))
		So(res, ShouldEqual, now.Second())
	})

	Convey("time.secs(a) works properly", t, func() {
		res, _ := Run(context.Background(), "time.secs", old)
		So(res, ShouldHaveSameTypeAs, float64(0))
		So(res, ShouldEqual, 0)
	})

	Convey("time.secs(a,b,c) errors properly", t, func() {
		res, _ := Run(context.Background(), "time.secs", "one", "two")
		So(res, ShouldHaveSameTypeAs, nil)
		So(res, ShouldEqual, nil)
	})

	Convey("time.unix() works properly", t, func() {
		res, _ := Run(context.Background(), "time.unix")
		So(res, ShouldHaveSameTypeAs, float64(0))
		So(res, ShouldBeGreaterThanOrEqualTo, now.Unix())
	})

	Convey("time.unix(a) works properly", t, func() {
		res, _ := Run(context.Background(), "time.unix", old)
		So(res, ShouldHaveSameTypeAs, float64(0))
		So(res, ShouldEqual, 1136239440)
	})

	Convey("time.unix(a,b,c) errors properly", t, func() {
		res, _ := Run(context.Background(), "time.unix", "one", "two")
		So(res, ShouldHaveSameTypeAs, nil)
		So(res, ShouldEqual, nil)
	})

	Convey("time.year() works properly", t, func() {
		res, _ := Run(context.Background(), "time.year")
		So(res, ShouldHaveSameTypeAs, float64(0))
		So(res, ShouldEqual, now.Year())
	})

	Convey("time.year(a) works properly", t, func() {
		res, _ := Run(context.Background(), "time.year", old)
		So(res, ShouldHaveSameTypeAs, float64(0))
		So(res, ShouldEqual, 2006)
	})

	Convey("time.year(a,b,c) errors properly", t, func() {
		res, _ := Run(context.Background(), "time.year", "one", "two")
		So(res, ShouldHaveSameTypeAs, nil)
		So(res, ShouldEqual, nil)
	})

}
