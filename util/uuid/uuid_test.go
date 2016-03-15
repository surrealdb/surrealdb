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

package uuid_test

import (
	"testing"

	"github.com/abcum/surreal/util/uuid"
	. "github.com/smartystreets/goconvey/convey"
)

func TestNewV1(t *testing.T) {

	var str *uuid.UUID

	str = uuid.NewV1()

	Convey(str.String(), t, func() {
		Convey("Should be a UUID", func() {
			So(str, ShouldHaveSameTypeAs, &uuid.UUID{})
		})
		Convey("Should not be nil", func() {
			So(str, ShouldNotBeNil)
		})
		Convey("Should be version 1", func() {
			So(str.Version(), ShouldEqual, 1)
		})
		Convey("Should be of length 36", func() {
			So(str.String(), ShouldHaveLength, 36)
		})
	})

}

func TestNewV2(t *testing.T) {

	var str *uuid.UUID

	str = uuid.NewV2(0)

	Convey(str.String(), t, func() {
		Convey("Should be a UUID", func() {
			So(str, ShouldHaveSameTypeAs, &uuid.UUID{})
		})
		Convey("Should not be nil", func() {
			So(str, ShouldNotBeNil)
		})
		Convey("Should be version 2", func() {
			So(str.Version(), ShouldEqual, 2)
		})
		Convey("Should be of length 36", func() {
			So(str.String(), ShouldHaveLength, 36)
		})
	})

}

func TestNewV3(t *testing.T) {

	var str *uuid.UUID

	str = uuid.NewV3(uuid.NamespaceDNS, "abcum.com")

	Convey(str.String(), t, func() {
		Convey("Should be a UUID", func() {
			So(str, ShouldHaveSameTypeAs, &uuid.UUID{})
		})
		Convey("Should not be nil", func() {
			So(str, ShouldNotBeNil)
		})
		Convey("Should be version 3", func() {
			So(str.Version(), ShouldEqual, 3)
		})
		Convey("Should be of length 36", func() {
			So(str.String(), ShouldHaveLength, 36)
		})
	})

	str = uuid.NewV3(uuid.NamespaceURL, "https://abcum.com")

	Convey(str.String(), t, func() {
		Convey("Should be a UUID", func() {
			So(str, ShouldHaveSameTypeAs, &uuid.UUID{})
		})
		Convey("Should not be nil", func() {
			So(str, ShouldNotBeNil)
		})
		Convey("Should be version 3", func() {
			So(str.Version(), ShouldEqual, 3)
		})
		Convey("Should be of length 36", func() {
			So(str.String(), ShouldHaveLength, 36)
		})
	})

}

func TestNewV4(t *testing.T) {

	var str *uuid.UUID

	str = uuid.NewV4()

	Convey(str.String(), t, func() {
		Convey("Should be a UUID", func() {
			So(str, ShouldHaveSameTypeAs, &uuid.UUID{})
		})
		Convey("Should not be nil", func() {
			So(str, ShouldNotBeNil)
		})
		Convey("Should be version 4", func() {
			So(str.Version(), ShouldEqual, 4)
		})
		Convey("Should be of length 36", func() {
			So(str.String(), ShouldHaveLength, 36)
		})
	})

}

func TestNewV5(t *testing.T) {

	var str *uuid.UUID

	str = uuid.NewV5(uuid.NamespaceDNS, "abcum.com")

	Convey(str.String(), t, func() {
		Convey("Should be a UUID", func() {
			So(str, ShouldHaveSameTypeAs, &uuid.UUID{})
		})
		Convey("Should not be nil", func() {
			So(str, ShouldNotBeNil)
		})
		Convey("Should be version 5", func() {
			So(str.Version(), ShouldEqual, 5)
		})
		Convey("Should be of length 36", func() {
			So(str.String(), ShouldHaveLength, 36)
		})
	})

	str = uuid.NewV5(uuid.NamespaceURL, "https://abcum.com")

	Convey(str.String(), t, func() {
		Convey("Should be a UUID", func() {
			So(str, ShouldHaveSameTypeAs, &uuid.UUID{})
		})
		Convey("Should not be nil", func() {
			So(str, ShouldNotBeNil)
		})
		Convey("Should be version 5", func() {
			So(str.Version(), ShouldEqual, 5)
		})
		Convey("Should be of length 36", func() {
			So(str.String(), ShouldHaveLength, 36)
		})
	})

}

func TestParsing(t *testing.T) {

	var str *uuid.UUID

	str = uuid.GetUUID("thiswill-notbe-parsed-as-successful")

	Convey(str.String(), t, func() {
		Convey("Should be a UUID", func() {
			So(str, ShouldHaveSameTypeAs, &uuid.UUID{})
		})
		Convey("Should not be nil", func() {
			So(str, ShouldNotBeNil)
		})
		Convey("Should be of length 36", func() {
			So(str.String(), ShouldHaveLength, 36)
		})
		Convey("Should be exactly `00000000-0000-0000-0000-000000000000`", func() {
			So(str.String(), ShouldEqual, "00000000-0000-0000-0000-000000000000")
		})
	})

	str = uuid.GetUUID("1400A118-2749-4605-833C-E7437488BCBF")

	Convey(str.String(), t, func() {
		Convey("Should be a UUID", func() {
			So(str, ShouldHaveSameTypeAs, &uuid.UUID{})
		})
		Convey("Should not be nil", func() {
			So(str, ShouldNotBeNil)
		})
		Convey("Should be version 4", func() {
			So(str.Version(), ShouldEqual, 4)
		})
		Convey("Should be of length 36", func() {
			So(str.String(), ShouldHaveLength, 36)
		})
		Convey("Should be exactly `1400a118-2749-4605-833c-e7437488bcbf`", func() {
			So(str.String(), ShouldEqual, "1400a118-2749-4605-833c-e7437488bcbf")
		})
	})

}
