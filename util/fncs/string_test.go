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

func TestString(t *testing.T) {

	var res interface{}

	var test = "This IS a test"
	var spac = "   This IS a test   "

	Convey("string.concat() works properly", t, func() {
		res, _ = Run(context.Background(), "string.concat", nil, 1, 1.5, "2", true, false)
		So(res, ShouldEqual, "<nil>11.52truefalse")
	})

	Convey("string.contains() works properly", t, func() {
		res, _ = Run(context.Background(), "string.contains", test, "done")
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "string.contains", test, "test")
		So(res, ShouldEqual, true)
	})

	Convey("string.endsWith() works properly", t, func() {
		res, _ = Run(context.Background(), "string.endsWith", test, "done")
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "string.endsWith", test, "test")
		So(res, ShouldEqual, true)
	})

	Convey("string.format() works properly", t, func() {
		res, _ = Run(context.Background(), "string.format", "%.9d", 1)
		So(res, ShouldEqual, "000000001")
	})

	Convey("string.format() errors properly", t, func() {
		res, _ = Run(context.Background(), "string.format", "%.9d")
		So(res, ShouldEqual, nil)
	})

	Convey("string.includes() works properly", t, func() {
		res, _ = Run(context.Background(), "string.includes", test, "done")
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "string.includes", test, "test")
		So(res, ShouldEqual, true)
	})

	Convey("string.join() works properly", t, func() {
		res, _ = Run(context.Background(), "string.join")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "string.join", ",")
		So(res, ShouldEqual, nil)
		res, _ = Run(context.Background(), "string.join", ",", nil, 1, 1.5, "2", true, false)
		So(res, ShouldEqual, "1,1.5,2,true,false")
	})

	Convey("string.length() works properly", t, func() {
		res, _ = Run(context.Background(), "string.length", test)
		So(res, ShouldEqual, 14)
	})

	Convey("string.levenshtein() works properly", t, func() {
		res, _ = Run(context.Background(), "string.levenshtein", "test", "test")
		So(res, ShouldEqual, 0)
		res, _ = Run(context.Background(), "string.levenshtein", "lawn", "flaw")
		So(res, ShouldEqual, 2)
		res, _ = Run(context.Background(), "string.levenshtein", "test", "done")
		So(res, ShouldEqual, 4)
	})

	Convey("string.lowercase() works properly", t, func() {
		res, _ = Run(context.Background(), "string.lowercase", test)
		So(res, ShouldEqual, "this is a test")
	})

	Convey("string.repeat(a, b) works properly", t, func() {
		res, _ = Run(context.Background(), "string.repeat", test, 2)
		So(res, ShouldEqual, test+test)
	})

	Convey("string.repeat(a, b) errors properly", t, func() {
		res, _ = Run(context.Background(), "string.repeat", test, "test")
		So(res, ShouldEqual, test)
	})

	Convey("string.replace() works properly", t, func() {
		res, _ = Run(context.Background(), "string.replace", test, "test", "note")
		So(res, ShouldEqual, "This IS a note")
	})

	Convey("string.reverse() works properly", t, func() {
		res, _ = Run(context.Background(), "string.reverse", test, "test")
		So(res, ShouldEqual, "tset a SI sihT")
	})

	Convey("string.search() works properly", t, func() {
		res, _ = Run(context.Background(), "string.search", test, "done")
		So(res, ShouldEqual, -1)
		res, _ = Run(context.Background(), "string.search", test, "test")
		So(res, ShouldEqual, 10)
	})

	Convey("string.slice() works properly", t, func() {
		res, _ = Run(context.Background(), "string.slice", test, "a", "b")
		So(res, ShouldEqual, test)
		res, _ = Run(context.Background(), "string.slice", test, "2", "b")
		So(res, ShouldEqual, test[2:])
		res, _ = Run(context.Background(), "string.slice", test, "a", "2")
		So(res, ShouldEqual, test[:2])
		res, _ = Run(context.Background(), "string.slice", test, "2", "4")
		So(res, ShouldEqual, test[2:4+2])
	})

	Convey("string.split() works properly", t, func() {
		res, _ = Run(context.Background(), "string.split", test, " ")
		So(res, ShouldResemble, []string{"This", "IS", "a", "test"})
	})

	Convey("string.startsWith() works properly", t, func() {
		res, _ = Run(context.Background(), "string.startsWith", test, "this")
		So(res, ShouldEqual, false)
		res, _ = Run(context.Background(), "string.startsWith", test, "This")
		So(res, ShouldEqual, true)
	})

	Convey("string.substr() works properly", t, func() {
		res, _ = Run(context.Background(), "string.substr", test, "a", "b")
		So(res, ShouldEqual, test)
		res, _ = Run(context.Background(), "string.substr", test, "2", "b")
		So(res, ShouldEqual, test[2:])
		res, _ = Run(context.Background(), "string.substr", test, "a", "2")
		So(res, ShouldEqual, test[:2])
		res, _ = Run(context.Background(), "string.substr", test, "2", "4")
		So(res, ShouldEqual, test[2:4])
	})

	Convey("string.trim() works properly", t, func() {
		res, _ = Run(context.Background(), "string.trim", spac)
		So(res, ShouldEqual, test)
	})

	Convey("string.uppercase() works properly", t, func() {
		res, _ = Run(context.Background(), "string.uppercase", test)
		So(res, ShouldEqual, "THIS IS A TEST")
	})

	Convey("string.words() works properly", t, func() {
		res, _ = Run(context.Background(), "string.words", test)
		So(res, ShouldResemble, []string{"This", "IS", "a", "test"})
	})

}
