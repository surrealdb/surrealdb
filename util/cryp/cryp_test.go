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

package cryp

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestEmpty(t *testing.T) {

	key := []byte("")
	str := []byte("Hello World")

	Convey("Cryptography should fail", t, func() {
		enc, _ := Encrypt(key, str)
		dec, _ := Decrypt(key, enc)
		Convey("Encrypt", func() {
			So(enc, ShouldResemble, str)
		})
		Convey("Decrypt", func() {
			So(dec, ShouldResemble, str)
		})
	})

}

func TestInvalid(t *testing.T) {

	key := []byte("invalidkey")
	str := []byte("Hello World")

	Convey("Cryptography should fail", t, func() {
		enc, _ := Encrypt(key, str)
		dec, _ := Decrypt(key, enc)
		Convey("Encrypt", func() {
			So(enc, ShouldResemble, []byte(nil))
		})
		Convey("Decrypt", func() {
			So(dec, ShouldResemble, []byte(nil))
		})
	})

}

func TestAES128(t *testing.T) {

	key := []byte("1hg7dbrma8ghe547")
	str := []byte("Hello World")

	Convey("AES-128 should encrypt and decrypt", t, func() {
		enc, _ := Encrypt(key, str)
		dec, _ := Decrypt(key, enc)
		Convey("Encrypt", func() {
			So(enc, ShouldNotResemble, str)
		})
		Convey("Decrypt", func() {
			So(dec, ShouldResemble, str)
		})
	})

}

func TestAES192(t *testing.T) {

	key := []byte("1hg7dbrma8ghe5473kghvie6")
	str := []byte("Hello World")

	Convey("AES-192 should encrypt and decrypt", t, func() {
		enc, _ := Encrypt(key, str)
		dec, _ := Decrypt(key, enc)
		Convey("Encrypt", func() {
			So(enc, ShouldNotResemble, str)
		})
		Convey("Decrypt", func() {
			So(dec, ShouldResemble, str)
		})
	})

}

func TestAES256(t *testing.T) {

	key := []byte("1hg7dbrma8ghe5473kghvie64jgi3ph4")
	str := []byte("Hello World")

	Convey("AES-256 should encrypt and decrypt", t, func() {
		enc, _ := Encrypt(key, str)
		dec, _ := Decrypt(key, enc)
		Convey("Encrypt", func() {
			So(enc, ShouldNotResemble, str)
		})
		Convey("Decrypt", func() {
			So(dec, ShouldResemble, str)
		})
	})

}
