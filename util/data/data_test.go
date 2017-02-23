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

package data

import (
	"errors"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestConversion(t *testing.T) {

	Convey("Can encode and decode", t, func() {
		doc := Consume(map[string]interface{}{
			"bool": true,
			"time": time.Now().UTC(),
		})
		enc := doc.Encode()
		dec := doc.Decode(enc)
		So(doc, ShouldResemble, dec)
	})

}

func TestOperations(t *testing.T) {

	// ----------------------------------------------------------------------
	// Ability to set and del nil
	// ----------------------------------------------------------------------

	Convey("Can get nil", t, func() {
		doc := Consume(nil)
		So(doc, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Exists("nil"), ShouldBeFalse)
		So(doc.Get("nil").Data(), ShouldEqual, nil)
	})

	Convey("Can set nil", t, func() {
		doc := Consume(nil)
		set, err := doc.Set("OK", "nil")
		So(err, ShouldBeNil)
		So(doc, ShouldHaveSameTypeAs, &Doc{})
		So(set.Data(), ShouldResemble, "OK")
		So(doc.Get("nil").Data(), ShouldEqual, "OK")
	})

	Convey("Can't del nil", t, func() {
		doc := Consume(nil)
		err := doc.Del("nil")
		So(err, ShouldNotBeNil)
	})

	Convey("Can't each nil", t, func() {
		doc := Consume(nil)
		var i int
		doc.Each(func(key string, val interface{}) error {
			i++
			return nil
		})
		So(i, ShouldEqual, 0)
	})

	// ----------------------------------------------------------------------
	// Ability to attempt new()
	// ----------------------------------------------------------------------

	Convey("Can attempt to use New()", t, func() {
		doc := New()
		one, err := doc.New("OK", "item")
		So(err, ShouldBeNil)
		So(one.Data(), ShouldEqual, "OK")
		So(doc.Exists("item"), ShouldBeTrue)
		So(doc.Get("item").Data(), ShouldEqual, "OK")
		two, err := doc.New("NOT OK", "item")
		So(err, ShouldBeNil)
		So(two.Data(), ShouldEqual, "OK")
		So(doc.Exists("item"), ShouldBeTrue)
		So(doc.Get("item").Data(), ShouldEqual, "OK")
	})

	// ----------------------------------------------------------------------
	// Ability to attempt iff()
	// ----------------------------------------------------------------------

	Convey("Can attempt to use Iff()", t, func() {
		doc := New()
		one, err := doc.Iff("OK", "item")
		So(err, ShouldBeNil)
		So(one.Data(), ShouldEqual, "OK")
		So(doc.Exists("item"), ShouldBeTrue)
		So(doc.Get("item").Data(), ShouldEqual, "OK")
		two, err := doc.Iff(nil, "item")
		So(err, ShouldBeNil)
		So(two.Data(), ShouldEqual, nil)
		So(doc.Exists("item"), ShouldBeFalse)
		So(doc.Get("item").Data(), ShouldEqual, nil)
	})

	// ----------------------------------------------------------------------
	// Ability to set and get array
	// ----------------------------------------------------------------------

	Convey("Can set base array", t, func() {
		doc := New()
		obj, err := doc.Array("array")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("array").Data(), ShouldResemble, []interface{}{})
	})

	// ----------------------------------------------------------------------
	// Ability to set and get object
	// ----------------------------------------------------------------------

	Convey("Can set base object", t, func() {
		doc := New()
		obj, err := doc.Object("object")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("object").Data(), ShouldResemble, map[string]interface{}{})
	})

	// ----------------------------------------------------------------------
	// Ability to set and get basic types
	// ----------------------------------------------------------------------

	Convey("Can set and get basic number", t, func() {
		doc := New()
		obj, err := doc.Set(1, "number")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("number").Data(), ShouldResemble, 1)
	})

	Convey("Can set and get basic string", t, func() {
		doc := New()
		obj, err := doc.Set("a", "string")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("string").Data(), ShouldResemble, "a")
	})

	Convey("Can set and get basic array", t, func() {
		doc := New()
		obj, err := doc.Set([]interface{}{1, 2, 3}, "array")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("array").Data(), ShouldResemble, []interface{}{1, 2, 3})
	})

	Convey("Can set and get basic object", t, func() {
		doc := New()
		obj, err := doc.Set(map[string]interface{}{"test": true}, "object")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("object").Data(), ShouldResemble, map[string]interface{}{"test": true})
	})

	// ----------------------------------------------------------------------
	// Ability to set and get basic embedded types
	// ----------------------------------------------------------------------

	Convey("Can set and get basic embedded number", t, func() {
		doc := New()
		obj, err := doc.Set(1, "sub.number")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("sub.number").Data(), ShouldResemble, 1)
	})

	Convey("Can set and get basic embedded string", t, func() {
		doc := New()
		obj, err := doc.Set("a", "sub.string")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("sub.string").Data(), ShouldResemble, "a")
	})

	Convey("Can set and get basic embedded array", t, func() {
		doc := New()
		obj, err := doc.Set([]interface{}{1, 2, 3}, "sub.array")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("sub.array").Data(), ShouldResemble, []interface{}{1, 2, 3})
	})

	Convey("Can set and get basic embedded object", t, func() {
		doc := New()
		obj, err := doc.Set(map[string]interface{}{"test": true}, "sub.object")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("sub.object").Data(), ShouldResemble, map[string]interface{}{"test": true})
	})

	// ----------------------------------------------------------------------
	// Ability to inc and dec basic types
	// ----------------------------------------------------------------------

	Convey("Can inc basic number", t, func() {
		doc := New()
		obj, err := doc.Inc(int64(100), "number")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("number").Data(), ShouldResemble, int64(100))
	})

	Convey("Can dec basic number", t, func() {
		doc := New()
		obj, err := doc.Dec(int64(100), "number")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("number").Data(), ShouldResemble, int64(-100))
	})

	Convey("Can inc basic double", t, func() {
		doc := New()
		obj, err := doc.Inc(float64(100), "double")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("double").Data(), ShouldResemble, float64(100))
	})

	Convey("Can dec basic double", t, func() {
		doc := New()
		obj, err := doc.Dec(float64(100), "double")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("double").Data(), ShouldResemble, float64(-100))
	})

	// ----------------------------------------------------------------------------------------------------

	doc := New()

	alt := map[string]interface{}{
		"bool":   true,
		"number": 12,
		"string": "s",
		"tags": []interface{}{
			"Hot",
		},
	}

	obj := map[string]interface{}{
		"bool":   true,
		"number": 10,
		"string": "s",
		"tags": []interface{}{
			"Hot",
		},
		"object": map[string]interface{}{
			"enabled": false,
		},
		"emptys": []interface{}{},
		"arrays": []interface{}{
			map[string]interface{}{
				"id":  1,
				"one": "one",
				"selected": map[string]interface{}{
					"city": "London",
				},
				"addresses": []interface{}{
					map[string]interface{}{
						"city": "London",
					},
					map[string]interface{}{
						"city": "New York",
					},
				},
			},
			map[string]interface{}{
				"id":  2,
				"two": "two",
				"selected": map[string]interface{}{
					"city": "Tonbridge",
				},
				"addresses": []interface{}{
					map[string]interface{}{
						"city": "Paris",
					},
					map[string]interface{}{
						"city": "Tonbridge",
					},
				},
			},
		},
	}

	// ----------------------------------------------------------------------------------------------------

	Convey("Can't del undefined", t, func() {
		err := doc.Del("the.item")
		So(err, ShouldNotBeNil)
	})

	Convey("Can set object", t, func() {
		def, err := doc.Set(obj, "the.item")
		So(err, ShouldBeNil)
		So(def, ShouldHaveSameTypeAs, &Doc{})
	})

	// ----------------------------------------------------------------------------------------------------

	Convey("Can iterate using each", t, func() {
		var i int
		doc.Each(func(key string, val interface{}) error {
			i++
			return nil
		})
		So(i, ShouldEqual, 32)
	})

	// ----------------------------------------------------------------------------------------------------

	Convey("Can diff two different docs", t, func() {
		dif := doc.Diff(doc)
		So(len(dif), ShouldEqual, 0)
	})

	Convey("Can diff two different docs", t, func() {
		obj := New()
		obj.Set(alt, "the.item")
		dif := doc.Diff(obj)
		So(len(dif), ShouldEqual, 25)
	})

	Convey("Can diff two different docs", t, func() {
		obj := New()
		obj.Set(alt, "the.item")
		dif := obj.Diff(doc)
		So(len(dif), ShouldEqual, 25)
	})

	// ----------------------------------------------------------------------------------------------------

	Convey("Does unset item exist", t, func() {
		So(doc.Exists("the.none"), ShouldBeFalse)
	})

	Convey("Does item exist", t, func() {
		So(doc.Exists("the.item"), ShouldBeTrue)
	})

	Convey("Does unset length of item exist", t, func() {
		So(doc.Exists("the.item.emptys.length"), ShouldBeFalse)
	})

	Convey("Does unset array item exist", t, func() {
		So(doc.Exists("the.item.emptys.0.id"), ShouldBeFalse)
	})

	Convey("Does unset multi array item exist", t, func() {
		So(doc.Exists("the.item.emptys.*.id"), ShouldBeFalse)
	})

	Convey("Does length of item exist", t, func() {
		So(doc.Exists("the.item.arrays.length"), ShouldBeTrue)
	})

	Convey("Does array item exist", t, func() {
		So(doc.Exists("the.item.arrays.0.id"), ShouldBeTrue)
		So(doc.Exists("the.item.arrays[0].id"), ShouldBeTrue)
		So(doc.Exists("the.item.arrays.[0].id"), ShouldBeTrue)
		So(doc.Exists("the.item.arrays.first.id"), ShouldBeTrue)
		So(doc.Exists("the.item.arrays[first].id"), ShouldBeTrue)
		So(doc.Exists("the.item.arrays.[first].id"), ShouldBeTrue)
	})

	Convey("Does array item exist", t, func() {
		So(doc.Exists("the.item.arrays.1.id"), ShouldBeTrue)
		So(doc.Exists("the.item.arrays[1].id"), ShouldBeTrue)
		So(doc.Exists("the.item.arrays.[1].id"), ShouldBeTrue)
		So(doc.Exists("the.item.arrays.last.id"), ShouldBeTrue)
		So(doc.Exists("the.item.arrays[last].id"), ShouldBeTrue)
		So(doc.Exists("the.item.arrays.[last].id"), ShouldBeTrue)
	})

	Convey("Does out of bounds array item exist", t, func() {
		So(doc.Exists("the.item.arrays.5.id"), ShouldBeFalse)
		So(doc.Exists("the.item.arrays[5].id"), ShouldBeFalse)
	})

	Convey("Does unset array item exist", t, func() {
		So(doc.Exists("the.item.arrays.0.none"), ShouldBeFalse)
		So(doc.Exists("the.item.arrays[0].none"), ShouldBeFalse)
	})

	Convey("Does incorrectly embedded array item exist", t, func() {
		So(doc.Exists("the.item.arrays.0.id.arggghh"), ShouldBeFalse)
		So(doc.Exists("the.item.arrays[0].id.arggghh"), ShouldBeFalse)
	})

	Convey("Does incorrectly embedded object item exist", t, func() {
		So(doc.Exists("the.item.object.enabled.arggghh"), ShouldBeFalse)
	})

	Convey("Does multi array item exist", t, func() {
		So(doc.Exists("the.item.arrays.*.id"), ShouldBeTrue)
		So(doc.Exists("the.item.arrays[*].id"), ShouldBeTrue)
		So(doc.Exists("the.item.arrays[:].id"), ShouldBeTrue)
	})

	Convey("Does sparse multi array item exist", t, func() {
		So(doc.Exists("the.item.arrays.*.one"), ShouldBeFalse)
		So(doc.Exists("the.item.arrays[*].one"), ShouldBeFalse)
		So(doc.Exists("the.item.arrays[:].one"), ShouldBeFalse)
	})

	// ----------------------------------------------------------------------------------------------------

	Convey("Is unset item valid", t, func() {
		So(doc.Valid("the.none"), ShouldBeFalse)
	})

	Convey("Is item valid", t, func() {
		So(doc.Valid("the.item"), ShouldBeTrue)
	})

	// ----------------------------------------------------------------------------------------------------

	Convey("Can get object keys and vals", t, func() {
		So(doc.Keys("the.item.object").Data(), ShouldResemble, []interface{}{"enabled"})
		So(doc.Vals("the.item.object").Data(), ShouldResemble, []interface{}{false})
	})

	// ----------------------------------------------------------------------------------------------------

	Convey("Can get unset item", t, func() {
		So(doc.Get("the.item.none").Data(), ShouldResemble, nil)
		So(doc.Get("the.item.none.arggghh").Data(), ShouldResemble, nil)
	})

	Convey("Can set unset item", t, func() {
		set, err := doc.Set("OK", "the.item.none")
		So(err, ShouldBeNil)
		So(set.Data(), ShouldResemble, "OK")
		So(doc.Get("the.item.none").Data(), ShouldResemble, "OK")
	})

	Convey("Can del unset item", t, func() {
		err := doc.Del("the.item.none")
		So(err, ShouldBeNil)
		So(doc.Get("the.item.none").Data(), ShouldResemble, nil)
	})

	// ----------------------------------------------------------------------------------------------------

	Convey("Can get basic bool", t, func() {
		So(doc.Get("the.item.bool").Data(), ShouldBeTrue)
	})

	Convey("Can set basic bool", t, func() {
		set, err := doc.Set(false, "the.item.bool")
		So(err, ShouldBeNil)
		So(set.Data(), ShouldBeFalse)
		So(doc.Get("the.item.bool").Data(), ShouldBeFalse)
	})

	// ----------------------------------------------------------------------------------------------------

	Convey("Can get basic number", t, func() {
		So(doc.Get("the.item.number").Data(), ShouldResemble, 10)
	})

	Convey("Can set basic number", t, func() {
		set, err := doc.Set(20, "the.item.number")
		So(err, ShouldBeNil)
		So(set.Data(), ShouldResemble, 20)
		So(doc.Get("the.item.number").Data(), ShouldResemble, 20)
	})

	// ----------------------------------------------------------------------------------------------------

	Convey("Can get basic string", t, func() {
		So(doc.Get("the.item.string").Data(), ShouldResemble, "s")
	})

	Convey("Can set basic string", t, func() {
		set, err := doc.Set("t", "the.item.string")
		So(err, ShouldBeNil)
		So(set.Data(), ShouldResemble, "t")
		So(doc.Get("the.item.string").Data(), ShouldResemble, "t")
	})

	// ----------------------------------------------------------------------------------------------------

	Convey("Can inc += 1", t, func() {
		obj, err := doc.Inc(int64(1), "the.item.tester")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("the.item.tester").Data(), ShouldResemble, int64(1))
	})

	Convey("Can inc += 4", t, func() {
		obj, err := doc.Inc(int64(4), "the.item.tester")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("the.item.tester").Data(), ShouldResemble, int64(5))
	})

	Convey("Can inc += 3.87659", t, func() {
		obj, err := doc.Inc(float64(3.87659), "the.item.tester")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("the.item.tester").Data(), ShouldResemble, float64(8.87659))
	})

	Convey("Can inc += 1.12341", t, func() {
		obj, err := doc.Inc(float64(1.12341), "the.item.tester")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("the.item.tester").Data(), ShouldResemble, float64(10))
	})

	Convey("Can inc += 5", t, func() {
		obj, err := doc.Inc(int64(5), "the.item.tester")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("the.item.tester").Data(), ShouldResemble, float64(15))
	})

	// ----------------------------------------------------------------------------------------------------

	Convey("Can reset tester", t, func() {
		obj, err := doc.Set(int64(15), "the.item.tester")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("the.item.tester").Data(), ShouldResemble, int64(15))
	})

	// ----------------------------------------------------------------------------------------------------

	Convey("Can dec -= 5", t, func() {
		obj, err := doc.Dec(int64(5), "the.item.tester")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("the.item.tester").Data(), ShouldResemble, int64(10))
	})

	Convey("Can dec -= 1.12341", t, func() {
		obj, err := doc.Dec(float64(1.12341), "the.item.tester")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("the.item.tester").Data(), ShouldResemble, float64(8.87659))
	})

	Convey("Can dec -= 3.87659", t, func() {
		obj, err := doc.Dec(float64(3.87659), "the.item.tester")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("the.item.tester").Data(), ShouldResemble, float64(5))
	})

	Convey("Can dec -= 4", t, func() {
		obj, err := doc.Dec(int64(4), "the.item.tester")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("the.item.tester").Data(), ShouldResemble, float64(1))
	})

	Convey("Can dec -= 1", t, func() {
		obj, err := doc.Dec(int64(1), "the.item.tester")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("the.item.tester").Data(), ShouldResemble, float64(0))
	})

	// ----------------------------------------------------------------------------------------------------

	Convey("Can reset bool", t, func() {
		obj, err := doc.Set(true, "the.item.bool")
		So(err, ShouldBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("the.item.bool").Data(), ShouldBeTrue)
	})

	Convey("Can't inc non incable item", t, func() {
		obj, err := doc.Inc(int64(1), "the.item.bool")
		So(err, ShouldNotBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("the.item.bool").Data(), ShouldBeTrue)
	})

	Convey("Can't dec non decable item", t, func() {
		obj, err := doc.Dec(int64(1), "the.item.bool")
		So(err, ShouldNotBeNil)
		So(obj, ShouldHaveSameTypeAs, &Doc{})
		So(doc.Get("the.item.bool").Data(), ShouldBeTrue)
	})

	// ----------------------------------------------------------------------------------------------------

	Convey("Can get array", t, func() {
		So(doc.Get("the.item.tags").Data(), ShouldResemble, []interface{}{"Hot"})
		So(doc.Get("the.item.tags.length").Data(), ShouldResemble, 1)
	})

	Convey("Can set array", t, func() {
		set, err := doc.Set([]interface{}{"Hot", "Humid", "Sticky", "Warm"}, "the.item.tags")
		So(err, ShouldBeNil)
		So(set.Data(), ShouldResemble, []interface{}{"Hot", "Humid", "Sticky", "Warm"})
		So(doc.Get("the.item.tags").Data(), ShouldResemble, []interface{}{"Hot", "Humid", "Sticky", "Warm"})
		So(doc.Get("the.item.tags.length").Data(), ShouldResemble, 4)
	})

	Convey("Can see if array contains valid", t, func() {
		So(doc.Contains("Hot", "the.item.tags"), ShouldBeTrue)
	})

	Convey("Can see if array contains invalid", t, func() {
		So(doc.Contains("Cold", "the.item.tags"), ShouldBeFalse)
	})

	Convey("Can get range queries from array", t, func() {
		So(doc.Get("the.item.tags[0:]").Data(), ShouldResemble, []interface{}{"Hot", "Humid", "Sticky", "Warm"})
		So(doc.Get("the.item.tags[:$]").Data(), ShouldResemble, []interface{}{"Hot", "Humid", "Sticky", "Warm"})
		So(doc.Get("the.item.tags[0:$]").Data(), ShouldResemble, []interface{}{"Hot", "Humid", "Sticky", "Warm"})
		So(doc.Get("the.item.tags[first:last]").Data(), ShouldResemble, []interface{}{"Hot", "Humid", "Sticky", "Warm"})
		So(doc.Get("the.item.tags[0:1]").Data(), ShouldResemble, []interface{}{"Hot"})
		So(doc.Get("the.item.tags[2:3]").Data(), ShouldResemble, []interface{}{"Sticky"})
		So(doc.Get("the.item.tags[2:4]").Data(), ShouldResemble, []interface{}{"Sticky", "Warm"})
		So(doc.Get("the.item.tags[2:5]").Data(), ShouldResemble, []interface{}{"Sticky", "Warm"})
		So(doc.Get("the.item.tags[2:9]").Data(), ShouldResemble, []interface{}{"Sticky", "Warm"})
		So(doc.Get("the.item.tags[4:5]").Data(), ShouldResemble, nil)
		So(doc.Get("the.item.tags[8:9]").Data(), ShouldResemble, nil)
		So(doc.Get("the.item.tags[0:none]").Data(), ShouldResemble, nil)
		So(doc.Get("the.item.tags[0:none:some]").Data(), ShouldResemble, nil)
	})

	Convey("Can add single to array", t, func() {
		_, err := doc.Inc("Sunny", "the.item.tags")
		So(err, ShouldBeNil)
		So(doc.Get("the.item.tags").Data(), ShouldResemble, []interface{}{"Hot", "Humid", "Sticky", "Warm", "Sunny"})
		So(doc.Get("the.item.tags.length").Data(), ShouldResemble, 5)
	})

	Convey("Can add duplicate to array", t, func() {
		_, err := doc.Inc("Sunny", "the.item.tags")
		So(err, ShouldBeNil)
		So(doc.Get("the.item.tags").Data(), ShouldResemble, []interface{}{"Hot", "Humid", "Sticky", "Warm", "Sunny"})
		So(doc.Get("the.item.tags.length").Data(), ShouldResemble, 5)
	})

	Convey("Can add multiple to array", t, func() {
		_, err := doc.Inc([]interface{}{"Sunny", "Snowy", "Icy"}, "the.item.tags")
		So(err, ShouldBeNil)
		So(doc.Get("the.item.tags").Data(), ShouldResemble, []interface{}{"Hot", "Humid", "Sticky", "Warm", "Sunny", "Snowy", "Icy"})
		So(doc.Get("the.item.tags.length").Data(), ShouldResemble, 7)
	})

	Convey("Can del single from array", t, func() {
		_, err := doc.Dec("Sunny", "the.item.tags")
		So(err, ShouldBeNil)
		So(doc.Get("the.item.tags").Data(), ShouldResemble, []interface{}{"Hot", "Humid", "Sticky", "Warm", "Snowy", "Icy"})
		So(doc.Get("the.item.tags.length").Data(), ShouldResemble, 6)
	})

	Convey("Can del multiple from array", t, func() {
		_, err := doc.Dec([]interface{}{"Snowy", "Icy"}, "the.item.tags")
		So(err, ShouldBeNil)
		So(doc.Get("the.item.tags").Data(), ShouldResemble, []interface{}{"Hot", "Humid", "Sticky", "Warm"})
		So(doc.Get("the.item.tags.length").Data(), ShouldResemble, 4)
	})

	Convey("Can get array → *", t, func() {
		So(doc.Get("the.item.tags.*").Data(), ShouldResemble, []interface{}{"Hot", "Humid", "Sticky", "Warm"})
		So(doc.Get("the.item.tags.length").Data(), ShouldResemble, 4)
	})

	Convey("Can del array → 2", t, func() {
		err := doc.Del("the.item.tags.2")
		So(err, ShouldBeNil)
		So(doc.Get("the.item.tags").Data(), ShouldResemble, []interface{}{"Hot", "Humid", "Warm"})
		So(doc.Get("the.item.tags.length").Data(), ShouldResemble, 3)
	})

	Convey("Can't del array → 5", t, func() {
		err := doc.Del("the.item.tags.5")
		So(err, ShouldBeNil)
		So(doc.Get("the.item.tags").Data(), ShouldResemble, []interface{}{"Hot", "Humid", "Warm"})
		So(doc.Get("the.item.tags.length").Data(), ShouldResemble, 3)
	})

	Convey("Can set array → 0", t, func() {
		set, err := doc.Set("Tepid", "the.item.tags.0")
		So(err, ShouldBeNil)
		So(set.Data(), ShouldResemble, "Tepid")
		So(doc.Get("the.item.tags").Data(), ShouldResemble, []interface{}{"Tepid", "Humid", "Warm"})
		So(doc.Get("the.item.tags.length").Data(), ShouldResemble, 3)
	})

	Convey("Can't set array → 5", t, func() {
		set, err := doc.Set("Other", "the.item.tags.5")
		So(err, ShouldBeNil)
		So(set.Data(), ShouldResemble, nil)
		So(doc.Get("the.item.tags").Data(), ShouldResemble, []interface{}{"Tepid", "Humid", "Warm"})
		So(doc.Get("the.item.tags.length").Data(), ShouldResemble, 3)
	})

	Convey("Can set array → first", t, func() {
		set, err := doc.Set("Test1", "the.item.tags.first")
		So(err, ShouldBeNil)
		So(set.Data(), ShouldResemble, "Test1")
		So(doc.Get("the.item.tags").Data(), ShouldResemble, []interface{}{"Test1", "Humid", "Warm"})
		So(doc.Get("the.item.tags.0").Data(), ShouldResemble, doc.Get("the.item.tags.first").Data())
		So(doc.Get("the.item.tags.length").Data(), ShouldResemble, 3)
	})

	Convey("Can set array → last", t, func() {
		set, err := doc.Set("Test2", "the.item.tags.last")
		So(err, ShouldBeNil)
		So(set.Data(), ShouldResemble, "Test2")
		So(doc.Get("the.item.tags").Data(), ShouldResemble, []interface{}{"Test1", "Humid", "Test2"})
		So(doc.Get("the.item.tags.2").Data(), ShouldResemble, doc.Get("the.item.tags.last").Data())
		So(doc.Get("the.item.tags.length").Data(), ShouldResemble, 3)
	})

	Convey("Can del array → first", t, func() {
		err := doc.Del("the.item.tags.first")
		So(err, ShouldBeNil)
		So(doc.Get("the.item.tags").Data(), ShouldResemble, []interface{}{"Humid", "Test2"})
		So(doc.Get("the.item.tags.0").Data(), ShouldResemble, doc.Get("the.item.tags.first").Data())
		So(doc.Get("the.item.tags.length").Data(), ShouldResemble, 2)
	})

	Convey("Can del array → last", t, func() {
		err := doc.Del("the.item.tags.last")
		So(err, ShouldBeNil)
		So(doc.Get("the.item.tags").Data(), ShouldResemble, []interface{}{"Humid"})
		So(doc.Get("the.item.tags.0").Data(), ShouldResemble, doc.Get("the.item.tags.last").Data())
		So(doc.Get("the.item.tags.length").Data(), ShouldResemble, 1)
	})

	Convey("Can set array → *", t, func() {
		set, err := doc.Set("Unknown", "the.item.tags.*")
		So(err, ShouldBeNil)
		So(set.Data(), ShouldResemble, []interface{}{"Unknown"})
		So(doc.Get("the.item.tags").Data(), ShouldResemble, []interface{}{"Unknown"})
		So(doc.Get("the.item.tags.length").Data(), ShouldResemble, 1)
	})

	Convey("Can del array → *", t, func() {
		err := doc.Del("the.item.tags.*")
		So(err, ShouldBeNil)
		So(doc.Get("the.item.tags").Data(), ShouldResemble, []interface{}{})
		So(doc.Get("the.item.tags.length").Data(), ShouldResemble, 0)
	})

	Convey("Can del single from array", t, func() {
		_, err := doc.Inc([]interface{}{"Hot", "Humid", "Sticky", "Warm", "Snowy", "Icy"}, "the.item.tags")
		So(err, ShouldBeNil)
		So(doc.Get("the.item.tags").Data(), ShouldResemble, []interface{}{"Hot", "Humid", "Sticky", "Warm", "Snowy", "Icy"})
		So(doc.Get("the.item.tags.length").Data(), ShouldResemble, 6)
	})

	Convey("Can del single from array", t, func() {
		err := doc.Del("the.item.tags[0:3]")
		So(err, ShouldBeNil)
		So(doc.Get("the.item.tags").Data(), ShouldResemble, []interface{}{"Snowy", "Icy"})
		So(doc.Get("the.item.tags.length").Data(), ShouldResemble, 2)
	})

	Convey("Can del array", t, func() {
		err := doc.Del("the.item.tags")
		So(err, ShouldBeNil)
	})

	Convey("Can't add to not array", t, func() {
		_, err := doc.ArrayAdd("None", "the.item.tags")
		So(err, ShouldNotBeNil)
	})

	Convey("Can't del from not array", t, func() {
		_, err := doc.ArrayDel("None", "the.item.tags")
		So(err, ShouldNotBeNil)
	})

	Convey("Can't see if array contains", t, func() {
		So(doc.Contains("None", "the.item.tags"), ShouldBeFalse)
	})

	// ----------------------------------------------------------------------------------------------------

	Convey("Can get object → key", t, func() {
		So(doc.Get("the.item.object.enabled").Data(), ShouldBeFalse)
	})

	Convey("Can set object → key", t, func() {
		set, err := doc.Set(true, "the.item.object.enabled")
		So(err, ShouldBeNil)
		So(set.Data(), ShouldBeTrue)
		So(doc.Get("the.item.object.enabled").Data(), ShouldBeTrue)
	})

	Convey("Can del object → key", t, func() {
		err := doc.Del("the.item.object.enabled")
		So(err, ShouldBeNil)
		So(doc.Exists("the.item.object.enabled"), ShouldBeFalse)
		So(doc.Get("the.item.object.enabled").Data(), ShouldResemble, nil)
	})

	// ----------------------------------------------------------------------------------------------------

	Convey("Can get array → * → key", t, func() {
		So(doc.Get("the.item.arrays.*.id").Data(), ShouldResemble, []interface{}{1, 2})
	})

	Convey("Can't get array → 5 → key", t, func() {
		So(doc.Get("the.item.arrays.5.id").Data(), ShouldResemble, nil)
	})

	Convey("Can set array → * → key", t, func() {
		set, err := doc.Set("ID", "the.item.arrays.*.id")
		So(err, ShouldBeNil)
		So(set.Data(), ShouldResemble, []interface{}{"ID", "ID"})
		So(doc.Get("the.item.arrays.*.id").Data(), ShouldResemble, []interface{}{"ID", "ID"})
	})

	Convey("Can set array → 0 → key", t, func() {
		set, err := doc.Set("ID1", "the.item.arrays.0.id")
		So(err, ShouldBeNil)
		So(set.Data(), ShouldResemble, "ID1")
		So(doc.Get("the.item.arrays.0.id").Data(), ShouldResemble, "ID1")
		So(doc.Get("the.item.arrays.*.id").Data(), ShouldResemble, []interface{}{"ID1", "ID"})
	})

	Convey("Can set array → 1 → key", t, func() {
		set, err := doc.Set("ID2", "the.item.arrays.1.id")
		So(err, ShouldBeNil)
		So(set.Data(), ShouldResemble, "ID2")
		So(doc.Get("the.item.arrays.1.id").Data(), ShouldResemble, "ID2")
		So(doc.Get("the.item.arrays.*.id").Data(), ShouldResemble, []interface{}{"ID1", "ID2"})
	})

	Convey("Can del array → 0 → key", t, func() {
		err := doc.Del("the.item.arrays.0.id")
		So(err, ShouldBeNil)
		So(doc.Get("the.item.arrays.0.id").Data(), ShouldResemble, nil)
		So(doc.Get("the.item.arrays.*.id").Data(), ShouldResemble, []interface{}{"ID2"})
	})

	Convey("Can't del array → 5 → key", t, func() {
		err := doc.Del("the.item.arrays.5.id")
		So(err, ShouldBeNil)
		So(doc.Get("the.item.arrays.*.id").Data(), ShouldResemble, []interface{}{"ID2"})
	})

	Convey("Can del array → * → key", t, func() {
		err := doc.Del("the.item.arrays.*.id")
		So(err, ShouldBeNil)
		So(doc.Get("the.item.arrays.0.id").Data(), ShouldResemble, nil)
		So(doc.Get("the.item.arrays.*.id").Data(), ShouldResemble, []interface{}{})
	})

	// ----------------------------------------------------------------------------------------------------

	Convey("Can get object → key", t, func() {
		So(doc.Get("the.item.arrays.*.one").Data(), ShouldResemble, []interface{}{"one"})
	})

	Convey("Can get object → key", t, func() {
		So(doc.Get("the.item.arrays.*.two").Data(), ShouldResemble, []interface{}{"two"})
	})

	// ----------------------------------------------------------------------------------------------------

	Convey("Can get array → * → object → !", t, func() {
		So(doc.Get("the.item.arrays.*.selected.none").Data(), ShouldResemble, []interface{}{})
	})

	Convey("Can set array → * → object → !", t, func() {
		set, err := doc.Set("OK", "the.item.arrays.*.selected.none")
		So(err, ShouldBeNil)
		So(set.Data(), ShouldResemble, []interface{}{"OK", "OK"})
		So(doc.Get("the.item.arrays.*.selected.none").Data(), ShouldResemble, []interface{}{"OK", "OK"})
	})

	Convey("Can get array → * → object → key", t, func() {
		So(doc.Get("the.item.arrays[*].selected.city").Data(), ShouldResemble, []interface{}{"London", "Tonbridge"})
		So(doc.Get("the.item.arrays[:].selected.city").Data(), ShouldResemble, []interface{}{"London", "Tonbridge"})
	})

	Convey("Can get array → 0 → arrays → 0 → key", t, func() {
		So(doc.Get("the.item.arrays.0.addresses.0.city").Data(), ShouldResemble, "London")
		So(doc.Get("the.item.arrays.0.addresses.0.city").Data(), ShouldResemble, "London")
	})

	Convey("Can get array → * → arrays → 0 → key", t, func() {
		So(doc.Get("the.item.arrays.*.addresses.0.city").Data(), ShouldResemble, []interface{}{"London", "Paris"})
	})

	Convey("Can get array → * → arrays → * → key", t, func() {
		So(doc.Get("the.item.arrays.*.addresses.*.city").Data(), ShouldResemble, []interface{}{[]interface{}{"London", "New York"}, []interface{}{"Paris", "Tonbridge"}})
	})

	Convey("Can get array → ! → arrays → 0 → key", t, func() {
		So(doc.Get("the.item.arrays.5.addresses.0.city").Data(), ShouldResemble, nil)
	})

	// ----------------------------------------------------------------------------------------------------

	tmp := []interface{}{
		map[string]interface{}{
			"test": "one",
		},
		map[string]interface{}{
			"test": "two",
		},
		map[string]interface{}{
			"test": "tre",
		},
	}

	Convey("Can del array", t, func() {
		err := doc.Del("the.item.arrays")
		So(err, ShouldBeNil)
	})

	Convey("Can walk nil", t, func() {
		doc.Walk(func(key string, val interface{}) error {
			doc.Set(tmp, "none")
			return nil
		})
		So(doc.Exists("none"), ShouldBeFalse)
	})

	Convey("Can walk array", t, func() {
		doc.Walk(func(key string, val interface{}) error {
			So(key, ShouldResemble, "the.item.arrays")
			doc.Set(tmp, key)
			return nil
		}, "the.item.arrays")
		So(doc.Get("the.item.arrays").Data(), ShouldResemble, tmp)
	})

	Convey("Can walk array → *", t, func() {
		doc.Walk(func(key string, val interface{}) error {
			So(key, ShouldBeIn, "the.item.arrays.[0]", "the.item.arrays.[1]", "the.item.arrays.[2]")
			So(val, ShouldBeIn, tmp[0], tmp[1], tmp[2])
			return nil
		}, "the.item.arrays.*")
	})

	Convey("Can walk array → * → object", t, func() {
		doc.Walk(func(key string, val interface{}) error {
			So(key, ShouldBeIn, "the.item.arrays.[0].test", "the.item.arrays.[1].test", "the.item.arrays.[2].test")
			So(val, ShouldBeIn, "one", "two", "tre")
			return nil
		}, "the.item.arrays.*.test")
	})

	Convey("Can walk array → first → object", t, func() {
		doc.Walk(func(key string, val interface{}) error {
			So(key, ShouldResemble, "the.item.arrays.[0].test")
			So(val, ShouldResemble, "one")
			return nil
		}, "the.item.arrays.first.test")
	})

	Convey("Can walk array → last → object", t, func() {
		doc.Walk(func(key string, val interface{}) error {
			So(key, ShouldResemble, "the.item.arrays.[2].test")
			So(val, ShouldResemble, "tre")
			return nil
		}, "the.item.arrays.last.test")
	})

	Convey("Can walk array → 0 → value", t, func() {
		doc.Walk(func(key string, val interface{}) error {
			So(key, ShouldResemble, "the.item.arrays.[0]")
			So(val, ShouldResemble, map[string]interface{}{"test": "one"})
			return nil
		}, "the.item.arrays.0")
	})

	Convey("Can walk array → 1 → value", t, func() {
		doc.Walk(func(key string, val interface{}) error {
			So(key, ShouldResemble, "the.item.arrays.[1]")
			So(val, ShouldResemble, map[string]interface{}{"test": "two"})
			return nil
		}, "the.item.arrays.1")
	})

	Convey("Can walk array → 2 → value", t, func() {
		doc.Walk(func(key string, val interface{}) error {
			So(key, ShouldResemble, "the.item.arrays.[2]")
			So(val, ShouldResemble, map[string]interface{}{"test": "tre"})
			return nil
		}, "the.item.arrays.2")
	})

	Convey("Can walk array → 3 → value", t, func() {
		err := doc.Walk(func(key string, val interface{}) error {
			return nil
		}, "the.item.arrays.3")
		So(err, ShouldNotBeNil)
	})

	Convey("Can walk array → 0 → value → value", t, func() {
		err := doc.Walk(func(key string, val interface{}) error {
			return nil
		}, "the.item.arrays.0.test.value")
		So(err, ShouldNotBeNil)
	})

	Convey("Can walk array → 0 → value → value → value", t, func() {
		err := doc.Walk(func(key string, val interface{}) error {
			return nil
		}, "the.item.arrays.0.test.value.value")
		So(err, ShouldNotBeNil)
	})

	Convey("Can force error from walk", t, func() {
		err := doc.Walk(func(key string, val interface{}) error {
			return errors.New("Testing")
		}, "the.item.something")
		So(err, ShouldNotBeNil)
	})

	Convey("Can force error from walk array → *", t, func() {
		err := doc.Walk(func(key string, val interface{}) error {
			return errors.New("Testing")
		}, "the.item.arrays.*")
		So(err, ShouldNotBeNil)
	})

	Convey("Can force error from walk array → * → value", t, func() {
		err := doc.Walk(func(key string, val interface{}) error {
			return errors.New("Testing")
		}, "the.item.arrays.*.test")
		So(err, ShouldNotBeNil)
	})

	// ----------------------------------------------------------------------------------------------------

	Convey("Can copy object", t, func() {
		So(doc.Copy(), ShouldResemble, doc.Data())
	})

	// ----------------------------------------------------------------------------------------------------

	Convey("Can reset object", t, func() {
		_, err := doc.Reset()
		So(err, ShouldBeNil)
	})

}
