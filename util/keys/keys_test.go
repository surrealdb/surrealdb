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

package keys

import (
	"bytes"
	"fmt"
	"math"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

var sorts []Key

var tests []struct {
	str string
	obj Key
	new Key
}

var prefs []struct {
	obj Key
	yes []Key
	nos []Key
}

func ShouldPrefix(actual interface{}, expected ...interface{}) string {
	if bytes.HasPrefix(expected[0].([]byte), actual.([]byte)) {
		return ""
	} else {
		return fmt.Sprintf("%v was not prefixed by \n%v\n%s\n%s", expected[0], actual, expected[0], actual)
	}
}

func ShouldNotPrefix(actual interface{}, expected ...interface{}) string {
	if bytes.HasPrefix(expected[0].([]byte), actual.([]byte)) {
		return fmt.Sprintf("%v was prefixed by \n%v\n%s\n%s", expected[0], actual, expected[0], actual)
	} else {
		return ""
	}
}

func TestMain(t *testing.T) {

	clock, _ := time.Parse(time.RFC3339, "1987-06-22T08:00:00.123456789Z")

	tests = []struct {
		str string
		obj Key
		new Key
	}{
		{
			str: "/surreal/!/¥",
			obj: &CK{KV: "surreal"},
			new: &CK{},
		},
		{
			str: "/surreal/!/n/abcum",
			obj: &NS{KV: "surreal", NS: "abcum"},
			new: &NS{},
		},
		{
			str: "/surreal/!/d/abcum/database",
			obj: &DB{KV: "surreal", NS: "abcum", DB: "database"},
			new: &DB{},
		},
		{
			str: "/surreal/!/t/abcum/database/person",
			obj: &TB{KV: "surreal", NS: "abcum", DB: "database", TB: "person"},
			new: &TB{},
		},
		{
			str: "/surreal/!/f/abcum/database/person/fullname",
			obj: &FD{KV: "surreal", NS: "abcum", DB: "database", TB: "person", FD: "fullname"},
			new: &FD{},
		},
		{
			str: "/surreal/!/r/abcum/database/person/select",
			obj: &RU{KV: "surreal", NS: "abcum", DB: "database", TB: "person", RU: "select"},
			new: &RU{},
		},
		{
			str: "/surreal/!/i/abcum/database/person/teenagers",
			obj: &IX{KV: "surreal", NS: "abcum", DB: "database", TB: "person", IX: "teenagers"},
			new: &IX{},
		},
		{
			str: "/surreal/abcum/database/person/*/\x00",
			obj: &Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: Prefix},
			new: &Thing{},
		},
		{
			str: "/surreal/abcum/database/person/*/873c2f37-ea03-4c5e-843e-cf393af44155",
			obj: &Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "873c2f37-ea03-4c5e-843e-cf393af44155"},
			new: &Thing{},
		},
		{
			str: "/surreal/abcum/database/person",
			obj: &Table{KV: "surreal", NS: "abcum", DB: "database", TB: "person"},
			new: &Table{},
		},
		{
			str: "/surreal/abcum/database/person/*/\xff",
			obj: &Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: Suffix},
			new: &Thing{},
		},
		{
			str: "/surreal/abcum/database/person/~/873c2f37-ea03-4c5e-843e-cf393af44155/1987-06-22T08:00:00.123456789Z",
			obj: &Patch{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "873c2f37-ea03-4c5e-843e-cf393af44155", AT: clock},
			new: &Patch{},
		},
		{
			str: "/surreal/abcum/database/person/~/test/1987-06-22T08:00:00.123456789Z",
			obj: &Patch{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "test", AT: clock},
			new: &Patch{},
		},
		{
			str: "/surreal/abcum/database/person/•/873c2f37-ea03-4c5e-843e-cf393af44155/friend/1987-06-22T08:00:00.123456789Z",
			obj: &Event{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "873c2f37-ea03-4c5e-843e-cf393af44155", TP: "friend", AT: clock},
			new: &Event{},
		},
		{
			str: "/surreal/abcum/database/person/«»/873c2f37-ea03-4c5e-843e-cf393af44155/clicked/b38d7aa1-60d6-4f2d-8702-46bd0fa961fe",
			obj: &Edge{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "873c2f37-ea03-4c5e-843e-cf393af44155", TP: "clicked", FK: "b38d7aa1-60d6-4f2d-8702-46bd0fa961fe"},
			new: &Edge{},
		},
		{
			str: "/surreal/abcum/database/person/«/873c2f37-ea03-4c5e-843e-cf393af44155/clicked/b38d7aa1-60d6-4f2d-8702-46bd0fa961fe",
			obj: &Edge{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "873c2f37-ea03-4c5e-843e-cf393af44155", TK: "«", TP: "clicked", FK: "b38d7aa1-60d6-4f2d-8702-46bd0fa961fe"},
			new: &Edge{},
		},
		{
			str: "/surreal/abcum/database/person/»/873c2f37-ea03-4c5e-843e-cf393af44155/clicked/b38d7aa1-60d6-4f2d-8702-46bd0fa961fe",
			obj: &Edge{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "873c2f37-ea03-4c5e-843e-cf393af44155", TK: "»", TP: "clicked", FK: "b38d7aa1-60d6-4f2d-8702-46bd0fa961fe"},
			new: &Edge{},
		},
		{
			str: "/surreal/abcum/database/person/«»/873c2f37-ea03-4c5e-843e-cf393af44155/clicked/b38d7aa1-60d6-4f2d-8702-46bd0fa961fe",
			obj: &Edge{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "873c2f37-ea03-4c5e-843e-cf393af44155", TK: "«»", TP: "clicked", FK: "b38d7aa1-60d6-4f2d-8702-46bd0fa961fe"},
			new: &Edge{},
		},
		{
			str: "/surreal/abcum/database/person/∆/names/[lastname firstname]",
			obj: &Index{KV: "surreal", NS: "abcum", DB: "database", TB: "person", IX: "names", FD: []interface{}{"lastname", "firstname"}},
			new: &Index{},
		},
		{
			str: "/surreal/abcum/database/person/∆/names/[false account:1 lastname <nil> firstname]",
			obj: &Index{KV: "surreal", NS: "abcum", DB: "database", TB: "person", IX: "names", FD: []interface{}{false, "account:1", "lastname", nil, "firstname"}},
			new: &Index{},
		},
		{
			str: "/surreal/abcum/database/person/∆/uniqs/[lastname firstname]/873c2f37-ea03-4c5e-843e-cf393af44155",
			obj: &Point{KV: "surreal", NS: "abcum", DB: "database", TB: "person", IX: "uniqs", FD: []interface{}{"lastname", "firstname"}, ID: "873c2f37-ea03-4c5e-843e-cf393af44155"},
			new: &Point{},
		},
		{
			str: "/surreal/abcum/database/person/∆/uniqs/[false account:1 lastname <nil> firstname]/873c2f37-ea03-4c5e-843e-cf393af44155",
			obj: &Point{KV: "surreal", NS: "abcum", DB: "database", TB: "person", IX: "uniqs", FD: []interface{}{false, "account:1", "lastname", nil, "firstname"}, ID: "873c2f37-ea03-4c5e-843e-cf393af44155"},
			new: &Point{},
		},
		{
			str: "Test key",
			new: &Full{},
			obj: &Full{
				N:     nil,
				B:     true,
				F:     false,
				S:     "Test",
				T:     clock,
				NI64:  int64(MinNumber),
				NI32:  math.MinInt32,
				NI16:  math.MinInt16,
				NI8:   math.MinInt8,
				NI:    -1,
				I:     1,
				I8:    math.MaxInt8,
				I16:   math.MaxInt16,
				I32:   math.MaxInt32,
				I64:   int64(MaxNumber),
				UI:    1,
				UI8:   math.MaxUint8,
				UI16:  math.MaxUint16,
				UI32:  math.MaxUint32,
				UI64:  uint64(MaxNumber),
				NF64:  -math.MaxFloat64,
				NF32:  -math.MaxFloat32,
				F32:   math.MaxFloat32,
				F64:   math.MaxFloat64,
				AB:    []bool{true, false},
				AS:    []string{"A", "B", "C"},
				AT:    []time.Time{clock, clock, clock},
				AI:    []int{1},
				AI8:   []int8{math.MaxInt8},
				AI16:  []int16{math.MaxInt16},
				AI32:  []int32{math.MaxInt32},
				AI64:  []int64{int64(MaxNumber)},
				AUI:   []uint{1},
				AUI8:  []uint8{math.MaxUint8},
				AUI16: []uint16{math.MaxUint16},
				AUI32: []uint32{math.MaxUint32},
				AUI64: []uint64{uint64(MaxNumber)},
				AF32:  []float32{1.1, 1.2, 1.3},
				AF64:  []float64{1.1, 1.2, 1.3},
				IN:    "Test",
				IB:    true,
				IF:    false,
				IT:    clock,
				II:    float64(19387.1),
				ID:    float64(183784.13413),
				INA:   []interface{}{true, false, nil, "Test", clock, int64(192), 1.1, 1.2, 1.3},
				AIN:   []interface{}{true, false, nil, "Test", clock, int64(192), 1.1, 1.2, 1.3, []interface{}{"Test"}},
			},
		},
	}

	sorts = []Key{

		&Table{KV: "kv", NS: "ns", DB: "db", TB: "person"},

		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: Prefix},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: nil},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: false},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: true},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: -9223372036854775807},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: -2147483647},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: -32767},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: -12},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: -2},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: -1},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: 0},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: 1},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: 2},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: 12},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: 127},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: int8(math.MaxInt8)},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: 32767},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: int16(math.MaxInt16)},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: 2147483647},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: int32(math.MaxInt32)},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: 9223372036854775807},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: int64(math.MaxInt64)},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: "A"},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: "B"},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: "Bb"},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: "C"},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: "a"},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: "b"},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: "bB"},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: "c"},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: "z"},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: "Â"},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: "Ä"},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: "ß"},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: "â"},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: "ä"},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: "①"},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: "会"},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: "😀😀😀"},
		&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: Suffix},

		&Patch{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: int8(1), AT: time.Now()},
		&Patch{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: int8(1), AT: time.Now()},

		&Edge{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: int8(1), TP: "friend", FK: int8(2)},
		&Edge{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: int8(1), TP: "friend", FK: int8(3)},
		&Edge{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: int8(2), TP: "friend", FK: int8(1)},

		&Index{KV: "kv", NS: "ns", DB: "db", TB: "person", IX: "names", FD: Prefix},

		&Index{KV: "kv", NS: "ns", DB: "db", TB: "person", IX: "names", FD: []interface{}{"account:abcum", false, "Smith", nil, "Zoe"}},
		&Index{KV: "kv", NS: "ns", DB: "db", TB: "person", IX: "names", FD: []interface{}{"account:abcum", true, "Morgan Hitchcock", nil, "Tobie"}},
		&Index{KV: "kv", NS: "ns", DB: "db", TB: "person", IX: "names", FD: []interface{}{"account:abcum", true, "Rutherford", nil, "Sam"}},

		&Index{KV: "kv", NS: "ns", DB: "db", TB: "person", IX: "names", FD: []interface{}{"account:tests", false, "Smith", nil, "Zoe"}},
		&Index{KV: "kv", NS: "ns", DB: "db", TB: "person", IX: "names", FD: []interface{}{"account:tests", true, "Morgan Hitchcock", nil, "Tobie"}},
		&Index{KV: "kv", NS: "ns", DB: "db", TB: "person", IX: "names", FD: []interface{}{"account:tests", true, "Rutherford", nil, "Sam"}},

		&Index{KV: "kv", NS: "ns", DB: "db", TB: "person", IX: "names", FD: []interface{}{"account:zymba", 0, 127}},
		&Index{KV: "kv", NS: "ns", DB: "db", TB: "person", IX: "names", FD: []interface{}{"account:zymba", 0, 127}},
		&Index{KV: "kv", NS: "ns", DB: "db", TB: "person", IX: "names", FD: []interface{}{"account:zymba", 1, math.MaxInt8}},
		&Index{KV: "kv", NS: "ns", DB: "db", TB: "person", IX: "names", FD: []interface{}{"account:zymba", 2, math.MaxInt16}},
		&Index{KV: "kv", NS: "ns", DB: "db", TB: "person", IX: "names", FD: []interface{}{"account:zymba", 2, math.MaxInt32}},
		&Index{KV: "kv", NS: "ns", DB: "db", TB: "person", IX: "names", FD: []interface{}{"account:zymba", 2, MaxNumber}},
		&Index{KV: "kv", NS: "ns", DB: "db", TB: "person", IX: "names", FD: []interface{}{"account:zymba", 2, MaxNumber}},

		&Index{KV: "kv", NS: "ns", DB: "db", TB: "person", IX: "names", FD: Suffix},
	}

	prefs = []struct {
		obj Key
		yes []Key
		nos []Key
	}{
		{
			obj: &Table{KV: "kv", NS: "ns", DB: "db", TB: "person"},
			yes: []Key{
				&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: Prefix},
				&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: "test"},
				&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: Suffix},
				&Patch{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: "test", AT: clock},
				&Index{KV: "kv", NS: "ns", DB: "db", TB: "person", IX: "names", FD: []interface{}{"1", "2"}},
				&Index{KV: "kv", NS: "ns", DB: "db", TB: "person", IX: "names", FD: []interface{}{"3", "4"}},
			},
			nos: []Key{
				&Thing{KV: "kv", NS: "ns", DB: "db", TB: "other", ID: "test"},
				&Thing{KV: "kv", NS: "ns", DB: "other", TB: "person", ID: "test"},
				&Thing{KV: "kv", NS: "other", DB: "db", TB: "person", ID: "test"},
				&Thing{KV: "other", NS: "ns", DB: "db", TB: "person", ID: "test"},
			},
		},
		{
			obj: &Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: Ignore},
			yes: []Key{
				&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: Prefix},
				&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: "test"},
				&Thing{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: Suffix},
			},
			nos: []Key{
				&Patch{KV: "kv", NS: "ns", DB: "db", TB: "person", ID: "test", AT: clock},
				&Index{KV: "kv", NS: "ns", DB: "db", TB: "person", IX: "names", FD: []interface{}{"1", "2"}},
				&Index{KV: "kv", NS: "ns", DB: "db", TB: "person", IX: "names", FD: []interface{}{"3", "4"}},
				&Thing{KV: "kv", NS: "ns", DB: "db", TB: "other", ID: "test"},
				&Thing{KV: "kv", NS: "ns", DB: "other", TB: "person", ID: "test"},
				&Thing{KV: "kv", NS: "other", DB: "db", TB: "person", ID: "test"},
				&Thing{KV: "other", NS: "ns", DB: "db", TB: "person", ID: "test"},
			},
		},
		{
			obj: &Index{KV: "kv", NS: "ns", DB: "db", TB: "person", IX: "names", FD: Ignore},
			yes: []Key{
				&Index{KV: "kv", NS: "ns", DB: "db", TB: "person", IX: "names", FD: []interface{}{"1", "2"}},
				&Index{KV: "kv", NS: "ns", DB: "db", TB: "person", IX: "names", FD: []interface{}{"3", "4"}},
			},
			nos: []Key{
				&Index{KV: "kv", NS: "ns", DB: "db", TB: "person", IX: "other", FD: []interface{}{}},
				&Index{KV: "kv", NS: "ns", DB: "db", TB: "other", IX: "names", FD: []interface{}{}},
				&Index{KV: "kv", NS: "ns", DB: "other", TB: "person", IX: "names", FD: []interface{}{}},
				&Index{KV: "kv", NS: "other", DB: "db", TB: "person", IX: "names", FD: []interface{}{}},
				&Index{KV: "other", NS: "ns", DB: "db", TB: "person", IX: "names", FD: []interface{}{}},
			},
		},
	}

}

func TestDisplaying(t *testing.T) {

	for _, test := range tests {

		Convey(test.str, t, func() {

			Convey("String should match", func() {
				So(test.obj.String(), ShouldEqual, test.str)
			})

		})

	}

}

func TestEncoding(t *testing.T) {

	for _, test := range tests {

		Convey(test.str, t, func() {

			enc := test.obj.Encode()
			test.new.Decode(enc)

			Convey("Key should encode and decode", func() {
				Printf("%s\n\n%#q\n\n%v\n\n", test.str, enc, enc)
				So(test.new, ShouldResemble, test.obj)
			})

		})

	}

}

func TestPrefixing(t *testing.T) {

	for _, test := range prefs {

		Convey(test.obj.String(), t, func() {

			for _, key := range test.yes {
				Convey("Key "+test.obj.String()+" should prefix "+key.String(), func() {
					So(test.obj.Encode(), ShouldPrefix, key.Encode())
				})
			}

			for _, key := range test.nos {
				Convey("Key "+test.obj.String()+" should not prefix "+key.String(), func() {
					So(test.obj.Encode(), ShouldNotPrefix, key.Encode())
				})
			}

		})

	}

}

func TestSorting(t *testing.T) {

	for i := 1; i < len(sorts); i++ {

		txt := fmt.Sprintf("%#v", sorts[i-1])

		Convey(txt, t, func() {

			one := sorts[i-1].Encode()
			two := sorts[i].Encode()

			Convey("Key should sort before next key", func() {
				Printf("%#v\n%#v\n------\n%#v\n%#v\n------\n%#q\n%#q", sorts[i-1], sorts[i], one, two, one, two)
				So(string(one), ShouldBeLessThanOrEqualTo, string(two))
			})
		})

	}

}
