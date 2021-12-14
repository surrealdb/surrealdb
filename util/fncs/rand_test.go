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
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestRand(t *testing.T) {

	Convey("rand() works properly", t, func() {
		res, _ := Run(context.Background(), "rand")
		So(res, ShouldHaveSameTypeAs, 36.0)
	})

	Convey("uuid() works properly", t, func() {
		res, _ := Run(context.Background(), "uuid")
		So(res, ShouldHaveSameTypeAs, "test")
		So(res, ShouldHaveLength, 36)
	})

	Convey("rand.bool() works properly", t, func() {
		res, _ := Run(context.Background(), "rand.bool")
		So(res, ShouldHaveSameTypeAs, true)
	})

	Convey("rand.uuid() works properly", t, func() {
		res, _ := Run(context.Background(), "rand.uuid")
		So(res, ShouldHaveSameTypeAs, "test")
		So(res, ShouldHaveLength, 36)
	})

	Convey("rand.enum() works properly", t, func() {
		res, _ := Run(context.Background(), "rand.enum")
		So(res, ShouldHaveSameTypeAs, nil)
	})

	Convey("rand.enum(a,b,c) works properly", t, func() {
		res, _ := Run(context.Background(), "rand.enum", "one", "two", "tre")
		So(res, ShouldHaveSameTypeAs, "test")
		So(res, ShouldBeIn, []interface{}{"one", "two", "tre"})
	})

	Convey("rand.time() works properly", t, func() {
		res, _ := Run(context.Background(), "rand.time")
		So(res, ShouldHaveSameTypeAs, time.Now())
	})

	Convey("rand.time(a,b) works properly", t, func() {
		d, _ := time.ParseDuration("24h")
		now := time.Now()
		res, _ := Run(context.Background(), "rand.time", now, now.Add(d))
		So(res, ShouldHaveSameTypeAs, time.Now())
		So(res.(time.Time).UnixNano(), ShouldBeBetween, now.UnixNano(), now.Add(d).UnixNano())
	})

	Convey("rand.string() works properly", t, func() {
		res, _ := Run(context.Background(), "rand.string")
		So(res, ShouldHaveSameTypeAs, "test")
	})

	Convey("rand.string(a) works properly", t, func() {
		res, _ := Run(context.Background(), "rand.string", int64(12))
		So(res, ShouldHaveSameTypeAs, "test")
		So(res, ShouldHaveLength, 12)
	})

	Convey("rand.string(a,b) works properly", t, func() {
		res, _ := Run(context.Background(), "rand.string", int64(12), int64(16))
		So(res, ShouldHaveSameTypeAs, "test")
	})

	Convey("rand.integer() works properly", t, func() {
		res, _ := Run(context.Background(), "rand.integer")
		So(res, ShouldHaveSameTypeAs, float64(0))
	})

	Convey("rand.integer(a,b) works properly", t, func() {
		res, _ := Run(context.Background(), "rand.integer", int64(12), int64(16))
		So(res, ShouldHaveSameTypeAs, float64(0))
		So(res, ShouldBeBetween, 11, 17)
	})

	Convey("rand.decimal() works properly", t, func() {
		res, _ := Run(context.Background(), "rand.decimal")
		So(res, ShouldHaveSameTypeAs, float64(0))
	})

	Convey("rand.decimal(a,b) works properly", t, func() {
		res, _ := Run(context.Background(), "rand.decimal", int64(12), int64(16))
		So(res, ShouldHaveSameTypeAs, float64(0))
		So(res, ShouldBeBetween, 11, 17)
	})

	Convey("rand.word() works properly", t, func() {
		res, _ := Run(context.Background(), "rand.word")
		So(res, ShouldHaveSameTypeAs, "test")
	})

	Convey("rand.sentence() works properly", t, func() {
		res, _ := Run(context.Background(), "rand.sentence")
		So(res, ShouldHaveSameTypeAs, "test")
	})

	Convey("rand.sentence(a,b) works properly", t, func() {
		res, _ := Run(context.Background(), "rand.sentence", int64(12), int64(16))
		So(res, ShouldHaveSameTypeAs, "test")
	})

	Convey("rand.paragraph() works properly", t, func() {
		res, _ := Run(context.Background(), "rand.paragraph")
		So(res, ShouldHaveSameTypeAs, "test")
	})

	Convey("rand.paragraph(a,b) works properly", t, func() {
		res, _ := Run(context.Background(), "rand.paragraph", int64(12), int64(16))
		So(res, ShouldHaveSameTypeAs, "test")
	})

	Convey("rand.person.email works properly", t, func() {
		res, _ := Run(context.Background(), "rand.person.email")
		So(res, ShouldHaveSameTypeAs, "test")
		So(res, ShouldContainSubstring, "@")
	})

	Convey("rand.person.phone works properly", t, func() {
		res, _ := Run(context.Background(), "rand.person.phone")
		So(res, ShouldHaveSameTypeAs, "test")
		So(res, ShouldContainSubstring, " ")
	})

	Convey("rand.person.fullname works properly", t, func() {
		res, _ := Run(context.Background(), "rand.person.fullname")
		So(res, ShouldHaveSameTypeAs, "test")
	})

	Convey("rand.person.firstname works properly", t, func() {
		res, _ := Run(context.Background(), "rand.person.firstname")
		So(res, ShouldHaveSameTypeAs, "test")
	})

	Convey("rand.person.lastname works properly", t, func() {
		res, _ := Run(context.Background(), "rand.person.lastname")
		So(res, ShouldHaveSameTypeAs, "test")
	})

	Convey("rand.person.username works properly", t, func() {
		res, _ := Run(context.Background(), "rand.person.username")
		So(res, ShouldHaveSameTypeAs, "test")
	})

	Convey("rand.person.jobtitle works properly", t, func() {
		res, _ := Run(context.Background(), "rand.person.jobtitle")
		So(res, ShouldHaveSameTypeAs, "test")
	})

	Convey("rand.company.name works properly", t, func() {
		res, _ := Run(context.Background(), "rand.company.name")
		So(res, ShouldHaveSameTypeAs, "test")
	})

	Convey("rand.company.industry works properly", t, func() {
		res, _ := Run(context.Background(), "rand.company.industry")
		So(res, ShouldHaveSameTypeAs, "test")
	})

	Convey("rand.location.name works properly", t, func() {
		res, _ := Run(context.Background(), "rand.location.name")
		So(res, ShouldHaveSameTypeAs, "test")
	})

	Convey("rand.location.address works properly", t, func() {
		res, _ := Run(context.Background(), "rand.location.address")
		So(res, ShouldHaveSameTypeAs, "test")
	})

	Convey("rand.location.street works properly", t, func() {
		res, _ := Run(context.Background(), "rand.location.street")
		So(res, ShouldHaveSameTypeAs, "test")
	})

	Convey("rand.location.city works properly", t, func() {
		res, _ := Run(context.Background(), "rand.location.city")
		So(res, ShouldHaveSameTypeAs, "test")
	})

	Convey("rand.location.state works properly", t, func() {
		res, _ := Run(context.Background(), "rand.location.state")
		So(res, ShouldHaveSameTypeAs, "test")
	})

	Convey("rand.location.county works properly", t, func() {
		res, _ := Run(context.Background(), "rand.location.county")
		So(res, ShouldHaveSameTypeAs, "test")
	})

	Convey("rand.location.zipcode works properly", t, func() {
		res, _ := Run(context.Background(), "rand.location.zipcode")
		So(res, ShouldHaveSameTypeAs, "test")
	})

	Convey("rand.location.postcode works properly", t, func() {
		res, _ := Run(context.Background(), "rand.location.postcode")
		So(res, ShouldHaveSameTypeAs, "test")
	})

	Convey("rand.location.country works properly", t, func() {
		res, _ := Run(context.Background(), "rand.location.country")
		So(res, ShouldHaveSameTypeAs, "test")
	})

	Convey("rand.location.altitude works properly", t, func() {
		res, _ := Run(context.Background(), "rand.location.altitude")
		So(res, ShouldHaveSameTypeAs, float64(0))
	})

	Convey("rand.location.latitude works properly", t, func() {
		res, _ := Run(context.Background(), "rand.location.latitude")
		So(res, ShouldHaveSameTypeAs, float64(0))
	})

	Convey("rand.location.longitude works properly", t, func() {
		res, _ := Run(context.Background(), "rand.location.longitude")
		So(res, ShouldHaveSameTypeAs, float64(0))
	})

}
