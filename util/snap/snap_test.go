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

package snap

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestEncodeDecode(t *testing.T) {

	dec := []byte("Hello World")
	enc := []byte{11, 20, 72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100}

	Convey("String should encode and decode", t, func() {
		Convey("Should encode", func() {
			res, _ := Encode(dec)
			So(res, ShouldResemble, enc)
		})
		Convey("Should decode", func() {
			res, _ := Decode(enc)
			So(res, ShouldResemble, dec)
		})
	})

}
