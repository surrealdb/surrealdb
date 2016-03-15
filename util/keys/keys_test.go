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

package keys_test

import (
	"testing"
	"time"

	"golang.org/x/text/collate"
	"golang.org/x/text/language"

	"github.com/abcum/surreal/util/keys"
	. "github.com/smartystreets/goconvey/convey"
)

func TestEncodeDecode(t *testing.T) {

	timer, _ := time.Parse(time.RFC3339, "1987-06-22T08:00:00.123456789Z")

	tests := []struct {
		str string
		obj keys.Key
		new keys.Key
	}{
		{
			str: "/surreal/!/n/abcum",
			obj: &keys.NS{KV: "surreal", NS: "abcum"},
			new: &keys.NS{},
		},
		{
			str: "/surreal/!/d/abcum/database",
			obj: &keys.DB{KV: "surreal", NS: "abcum", DB: "database"},
			new: &keys.DB{},
		},
		{
			str: "/surreal/!/t/abcum/database/person",
			obj: &keys.TB{KV: "surreal", NS: "abcum", DB: "database", TB: "person"},
			new: &keys.TB{},
		},
		{
			str: "/surreal/!/f/abcum/database/person/fullname",
			obj: &keys.FD{KV: "surreal", NS: "abcum", DB: "database", TB: "person", FD: "fullname"},
			new: &keys.FD{},
		},
		{
			str: "/surreal/!/i/abcum/database/person/teenagers",
			obj: &keys.IX{KV: "surreal", NS: "abcum", DB: "database", TB: "person", IX: "teenagers"},
			new: &keys.IX{},
		},
		{
			str: "/surreal/abcum/database/person/¤/[firstname,lastname]",
			obj: &keys.Index{KV: "surreal", NS: "abcum", DB: "database", TB: "person", What: []string{"firstname", "lastname"}},
			new: &keys.Index{},
		},
		{
			str: "/surreal/abcum/database/person/873c2f37-ea03-4c5e-843e-cf393af44155",
			obj: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "873c2f37-ea03-4c5e-843e-cf393af44155"},
			new: &keys.Thing{},
		},
		{
			str: "/surreal/abcum/database/person/•/873c2f37-ea03-4c5e-843e-cf393af44155/1987-06-22T08:00:00.123456789Z",
			obj: &keys.Trail{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "873c2f37-ea03-4c5e-843e-cf393af44155", Time: timer},
			new: &keys.Trail{},
		},
		{
			str: "/surreal/abcum/database/person/‡/873c2f37-ea03-4c5e-843e-cf393af44155/friend/1987-06-22T08:00:00.123456789Z",
			obj: &keys.Event{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "873c2f37-ea03-4c5e-843e-cf393af44155", Type: "friend", Time: timer},
			new: &keys.Event{},
		},
		{
			str: "/surreal/abcum/database/person/«»/873c2f37-ea03-4c5e-843e-cf393af44155/clicked/b38d7aa1-60d6-4f2d-8702-46bd0fa961fe",
			obj: &keys.Edge{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "873c2f37-ea03-4c5e-843e-cf393af44155", Type: "clicked", Edge: "b38d7aa1-60d6-4f2d-8702-46bd0fa961fe"},
			new: &keys.Edge{},
		},
		{
			str: "/surreal/abcum/database/person/«/873c2f37-ea03-4c5e-843e-cf393af44155/clicked/b38d7aa1-60d6-4f2d-8702-46bd0fa961fe",
			obj: &keys.Edge{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "873c2f37-ea03-4c5e-843e-cf393af44155", TK: "«", Type: "clicked", Edge: "b38d7aa1-60d6-4f2d-8702-46bd0fa961fe"},
			new: &keys.Edge{},
		},
		{
			str: "/surreal/abcum/database/person/»/873c2f37-ea03-4c5e-843e-cf393af44155/clicked/b38d7aa1-60d6-4f2d-8702-46bd0fa961fe",
			obj: &keys.Edge{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "873c2f37-ea03-4c5e-843e-cf393af44155", TK: "»", Type: "clicked", Edge: "b38d7aa1-60d6-4f2d-8702-46bd0fa961fe"},
			new: &keys.Edge{},
		},
		{
			str: "/surreal/abcum/database/person/«»/873c2f37-ea03-4c5e-843e-cf393af44155/clicked/b38d7aa1-60d6-4f2d-8702-46bd0fa961fe",
			obj: &keys.Edge{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "873c2f37-ea03-4c5e-843e-cf393af44155", TK: "«»", Type: "clicked", Edge: "b38d7aa1-60d6-4f2d-8702-46bd0fa961fe"},
			new: &keys.Edge{},
		},
	}

	for _, test := range tests {

		Convey(test.str, t, func() {

			Convey("String should match", func() {
				So(test.obj.String(), ShouldEqual, test.str)
			})

			Convey("String should be a string", func() {
				So(test.obj.String(), ShouldHaveSameTypeAs, "")
			})

			Convey("Encode should be a byte slice", func() {
				So(test.obj.Encode(), ShouldHaveSameTypeAs, []byte{})
			})

			Convey("Key should encode and decode", func() {
				enc := test.obj.Encode()
				test.new.Decode(enc)
				// So(test.new.String(), ShouldEqual, test.str)
			})

		})

	}

}

func TestSorting(t *testing.T) {

	c := collate.New(language.English, collate.Force, collate.Numeric)

	tests := []struct {
		res int
		one keys.Key
		two keys.Key
	}{
		{
			res: -1,
			one: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "1"},
			two: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "2"},
		},
		{
			res: -1,
			one: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "2"},
			two: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "12"},
		},
		{
			res: 1,
			one: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "12"},
			two: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "2"},
		},
		{
			res: -1, // Fullwidth is sorted as usual.
			one: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "２"},
			two: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "１２"},
		},
		{
			res: 1, // Circled is not sorted as numbers.
			one: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "②"},
			two: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "①②"},
		},
		{
			res: 1, // Subscript is not sorted as numbers.
			one: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "₂"},
			two: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "₁₂"},
		},
		{
			res: -1,
			one: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "abc"},
			two: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "ABC"},
		},
		{
			res: -1,
			one: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "ABC"},
			two: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "zbc"},
		},
		{
			res: -1,
			one: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "ÄBC"},
			two: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "zbc"},
		},
		{
			res: -1,
			one: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "ÁBC"},
			two: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "zbc"},
		},
		{
			res: -1,
			one: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "zbc"},
			two: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "ZBC"},
		},
		{
			res: -1,
			one: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: keys.Prefix},
			two: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: keys.Suffix},
		},
		{
			res: -1,
			one: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: keys.Prefix},
			two: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "a"},
		},
		{
			res: -1,
			one: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: keys.Prefix},
			two: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "①"},
		},
		{
			res: -1,
			one: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: keys.Prefix},
			two: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "会"},
		},
		{
			res: -1,
			one: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: keys.Prefix},
			two: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "😀"},
		},
		{
			res: -1,
			one: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "a"},
			two: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: keys.Suffix},
		},
		{
			res: -1,
			one: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "①"},
			two: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: keys.Suffix},
		},
		{
			res: -1,
			one: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "会"},
			two: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: keys.Suffix},
		},
		{
			res: -1,
			one: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: "😀"},
			two: &keys.Thing{KV: "surreal", NS: "abcum", DB: "database", TB: "person", ID: keys.Suffix},
		},
	}

	for _, test := range tests {

		Convey("Strings should sort correctly", t, func() {
			one := string(test.one.Encode())
			two := string(test.two.Encode())
			res := c.CompareString(one, two)
			So(res, ShouldEqual, test.res)
		})

	}

}
