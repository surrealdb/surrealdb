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

package db

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func inc(val *int) int {
	*val++
	return *val
}

func TestFetch(t *testing.T) {

	Convey("Check calc-as-bool expressions", t, func() {

		setupDB(1)

		txt := `

		USE NS test DB test;

		SELECT * FROM "test" WHERE gone;
		SELECT * FROM "test" WHERE true;
		SELECT * FROM "test" WHERE 0;
		SELECT * FROM "test" WHERE -1;
		SELECT * FROM "test" WHERE +1;
		SELECT * FROM "test" WHERE "test";
		SELECT * FROM "test" WHERE time.now();

		SELECT * FROM "test" WHERE [];
		SELECT * FROM "test" WHERE [1,2,3];

		SELECT * FROM "test" WHERE {};
		SELECT * FROM "test" WHERE {test:true};

		SELECT * FROM "test" WHERE gone OR gone;
		SELECT * FROM "test" WHERE true OR true;
		SELECT * FROM "test" WHERE gone OR true;

		SELECT * FROM "test" WHERE gone AND gone;
		SELECT * FROM "test" WHERE true AND true;
		SELECT * FROM "test" WHERE gone AND true;

		`

		i := 0
		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 18)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)

	})

	Convey("Check calc-as-math expressions", t, func() {

		setupDB(1)

		txt := `

		USE NS test DB test;

		SELECT * FROM "test" WHERE 0 + false;
		SELECT * FROM "test" WHERE 1 + false;

		SELECT * FROM "test" WHERE 0 + true;
		SELECT * FROM "test" WHERE 1 + true;

		SELECT * FROM "test" WHERE time.now() + 1;

		SELECT * FROM "test" WHERE [] + [];
		SELECT * FROM "test" WHERE {} + {};

		`

		i := 0
		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 8)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)

	})

	Convey("Check binary math comparisons", t, func() {

		setupDB(1)

		txt := `

		USE NS test DB test;

		SELECT * FROM "test" WHERE 1 = 1;
		SELECT * FROM "test" WHERE 1 != 1;

		SELECT * FROM "test" WHERE 1 < 2;
		SELECT * FROM "test" WHERE 1 > 2;
		SELECT * FROM "test" WHERE 1 <= 2;
		SELECT * FROM "test" WHERE 1 >= 2;

		SELECT * FROM "test" WHERE 10-10 = 0;
		SELECT * FROM "test" WHERE 10+10 = 20;
		SELECT * FROM "test" WHERE 10*10 = 100;
		SELECT * FROM "test" WHERE 10/10 = 1;

		`

		i := 0
		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 11)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

	})

	Convey("Check binary NULL comparisons", t, func() {

		setupDB(1)

		txt := `

		USE NS test DB test;

		LET var = NULL;

		SELECT * FROM "test" WHERE NULL = "";
		SELECT * FROM "test" WHERE NULL = [];
		SELECT * FROM "test" WHERE NULL = {};
		SELECT * FROM "test" WHERE NULL = $var;
		SELECT * FROM "test" WHERE NULL = VOID;
		SELECT * FROM "test" WHERE NULL = NULL;
		SELECT * FROM "test" WHERE NULL = EMPTY;
		SELECT * FROM "test" WHERE NULL = something;
		SELECT * FROM { test: [] } WHERE NULL = test;
		SELECT * FROM { test: {} } WHERE NULL = test;

		SELECT * FROM "test" WHERE "" = NULL;
		SELECT * FROM "test" WHERE [] = NULL;
		SELECT * FROM "test" WHERE {} = NULL;
		SELECT * FROM "test" WHERE $var = NULL;
		SELECT * FROM "test" WHERE VOID = NULL;
		SELECT * FROM "test" WHERE NULL = NULL;
		SELECT * FROM "test" WHERE EMPTY = NULL;
		SELECT * FROM "test" WHERE something = NULL;
		SELECT * FROM { test: [] } WHERE test = NULL;
		SELECT * FROM { test: {} } WHERE test = NULL;

		SELECT * FROM "test" WHERE NULL != "";
		SELECT * FROM "test" WHERE NULL != [];
		SELECT * FROM "test" WHERE NULL != {};
		SELECT * FROM "test" WHERE NULL != $var;
		SELECT * FROM "test" WHERE NULL != VOID;
		SELECT * FROM "test" WHERE NULL != NULL;
		SELECT * FROM "test" WHERE NULL != EMPTY;
		SELECT * FROM "test" WHERE NULL != something;
		SELECT * FROM { test: [] } WHERE NULL != test;
		SELECT * FROM { test: {} } WHERE NULL != test;

		SELECT * FROM "test" WHERE "" != NULL;
		SELECT * FROM "test" WHERE [] != NULL;
		SELECT * FROM "test" WHERE {} != NULL;
		SELECT * FROM "test" WHERE $var != NULL;
		SELECT * FROM "test" WHERE VOID != NULL;
		SELECT * FROM "test" WHERE NULL != NULL;
		SELECT * FROM "test" WHERE EMPTY != NULL;
		SELECT * FROM "test" WHERE something != NULL;
		SELECT * FROM { test: [] } WHERE test != NULL;
		SELECT * FROM { test: {} } WHERE test != NULL;

		SELECT * FROM "test" WHERE NULL ∈ [];
		SELECT * FROM "test" WHERE NULL ∉ [];
		SELECT * FROM "test" WHERE NULL ∋ [];
		SELECT * FROM "test" WHERE NULL ∌ [];

		SELECT * FROM "test" WHERE [] ∈ NULL;
		SELECT * FROM "test" WHERE [] ∉ NULL;
		SELECT * FROM "test" WHERE [] ∋ NULL;
		SELECT * FROM "test" WHERE [] ∌ NULL;

		SELECT * FROM "test" WHERE NULL ∈ [null];
		SELECT * FROM "test" WHERE NULL ∉ [null];
		SELECT * FROM "test" WHERE [null] ∋ NULL;
		SELECT * FROM "test" WHERE [null] ∌ NULL;

		`

		i := 1
		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 54)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)

	})

	Convey("Check binary VOID comparisons", t, func() {

		txt := `

		USE NS test DB test;

		LET var = NULL;

		SELECT * FROM "test" WHERE VOID = "";
		SELECT * FROM "test" WHERE VOID = [];
		SELECT * FROM "test" WHERE VOID = {};
		SELECT * FROM "test" WHERE VOID = $var;
		SELECT * FROM "test" WHERE VOID = NULL;
		SELECT * FROM "test" WHERE VOID = VOID;
		SELECT * FROM "test" WHERE VOID = EMPTY;
		SELECT * FROM "test" WHERE VOID = something;
		SELECT * FROM { test: [] } WHERE VOID = test;
		SELECT * FROM { test: {} } WHERE VOID = test;

		SELECT * FROM "test" WHERE "" = VOID;
		SELECT * FROM "test" WHERE [] = VOID;
		SELECT * FROM "test" WHERE {} = VOID;
		SELECT * FROM "test" WHERE $var = VOID;
		SELECT * FROM "test" WHERE NULL = VOID;
		SELECT * FROM "test" WHERE VOID = VOID;
		SELECT * FROM "test" WHERE EMPTY = VOID;
		SELECT * FROM "test" WHERE something = VOID;
		SELECT * FROM { test: [] } WHERE test = VOID;
		SELECT * FROM { test: {} } WHERE test = VOID;

		SELECT * FROM "test" WHERE VOID != "";
		SELECT * FROM "test" WHERE VOID != [];
		SELECT * FROM "test" WHERE VOID != {};
		SELECT * FROM "test" WHERE VOID != $var;
		SELECT * FROM "test" WHERE VOID != NULL;
		SELECT * FROM "test" WHERE VOID != VOID;
		SELECT * FROM "test" WHERE VOID != EMPTY;
		SELECT * FROM "test" WHERE VOID != something;
		SELECT * FROM { test: [] } WHERE VOID != test;
		SELECT * FROM { test: {} } WHERE VOID != test;

		SELECT * FROM "test" WHERE "" != VOID;
		SELECT * FROM "test" WHERE [] != VOID;
		SELECT * FROM "test" WHERE {} != VOID;
		SELECT * FROM "test" WHERE $var != VOID;
		SELECT * FROM "test" WHERE NULL != VOID;
		SELECT * FROM "test" WHERE VOID != VOID;
		SELECT * FROM "test" WHERE EMPTY != VOID;
		SELECT * FROM "test" WHERE something != VOID;
		SELECT * FROM { test: [] } WHERE test != VOID;
		SELECT * FROM { test: {} } WHERE test != VOID;

		SELECT * FROM "test" WHERE VOID ∈ [];
		SELECT * FROM "test" WHERE VOID ∉ [];
		SELECT * FROM "test" WHERE VOID ∋ [];
		SELECT * FROM "test" WHERE VOID ∌ [];

		SELECT * FROM "test" WHERE [] ∈ VOID;
		SELECT * FROM "test" WHERE [] ∉ VOID;
		SELECT * FROM "test" WHERE [] ∋ VOID;
		SELECT * FROM "test" WHERE [] ∌ VOID;

		SELECT * FROM "test" WHERE VOID ∈ [null];
		SELECT * FROM "test" WHERE VOID ∉ [null];
		SELECT * FROM "test" WHERE [null] ∋ VOID;
		SELECT * FROM "test" WHERE [null] ∌ VOID;

		`

		i := 1
		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 54)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

	})

	Convey("Check binary EMPTY comparisons", t, func() {

		setupDB(1)

		txt := `

		USE NS test DB test;

		LET var = NULL;

		SELECT * FROM "test" WHERE EMPTY = "";
		SELECT * FROM "test" WHERE EMPTY = [];
		SELECT * FROM "test" WHERE EMPTY = {};
		SELECT * FROM "test" WHERE EMPTY = $var;
		SELECT * FROM "test" WHERE EMPTY = NULL;
		SELECT * FROM "test" WHERE EMPTY = VOID;
		SELECT * FROM "test" WHERE EMPTY = EMPTY;
		SELECT * FROM "test" WHERE EMPTY = something;
		SELECT * FROM { test: [] } WHERE EMPTY = test;
		SELECT * FROM { test: {} } WHERE EMPTY = test;

		SELECT * FROM "test" WHERE "" = EMPTY;
		SELECT * FROM "test" WHERE [] = EMPTY;
		SELECT * FROM "test" WHERE {} = EMPTY;
		SELECT * FROM "test" WHERE $var = EMPTY;
		SELECT * FROM "test" WHERE NULL = EMPTY;
		SELECT * FROM "test" WHERE VOID = EMPTY;
		SELECT * FROM "test" WHERE EMPTY = EMPTY;
		SELECT * FROM "test" WHERE something = EMPTY;
		SELECT * FROM { test: [] } WHERE test = EMPTY;
		SELECT * FROM { test: {} } WHERE test = EMPTY;

		SELECT * FROM "test" WHERE EMPTY != "";
		SELECT * FROM "test" WHERE EMPTY != [];
		SELECT * FROM "test" WHERE EMPTY != {};
		SELECT * FROM "test" WHERE EMPTY != $var;
		SELECT * FROM "test" WHERE EMPTY != NULL;
		SELECT * FROM "test" WHERE EMPTY != VOID;
		SELECT * FROM "test" WHERE EMPTY != EMPTY;
		SELECT * FROM "test" WHERE EMPTY != something;
		SELECT * FROM { test: [] } WHERE EMPTY != test;
		SELECT * FROM { test: {} } WHERE EMPTY != test;

		SELECT * FROM "test" WHERE "" != EMPTY;
		SELECT * FROM "test" WHERE [] != EMPTY;
		SELECT * FROM "test" WHERE {} != EMPTY;
		SELECT * FROM "test" WHERE $var != EMPTY;
		SELECT * FROM "test" WHERE NULL != EMPTY;
		SELECT * FROM "test" WHERE VOID != EMPTY;
		SELECT * FROM "test" WHERE EMPTY != EMPTY;
		SELECT * FROM "test" WHERE something != EMPTY;
		SELECT * FROM { test: [] } WHERE test != EMPTY;
		SELECT * FROM { test: {} } WHERE test != EMPTY;

		SELECT * FROM "test" WHERE EMPTY ∈ [];
		SELECT * FROM "test" WHERE EMPTY ∉ [];
		SELECT * FROM "test" WHERE EMPTY ∋ [];
		SELECT * FROM "test" WHERE EMPTY ∌ [];

		SELECT * FROM "test" WHERE [] ∈ EMPTY;
		SELECT * FROM "test" WHERE [] ∉ EMPTY;
		SELECT * FROM "test" WHERE [] ∋ EMPTY;
		SELECT * FROM "test" WHERE [] ∌ EMPTY;

		SELECT * FROM "test" WHERE EMPTY ∈ [null];
		SELECT * FROM "test" WHERE EMPTY ∉ [null];
		SELECT * FROM "test" WHERE [null] ∋ EMPTY;
		SELECT * FROM "test" WHERE [null] ∌ EMPTY;

		`

		i := 1
		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 54)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

	})

	Convey("Check binary thing comparisons", t, func() {

		setupDB(1)

		txt := `

		USE NS test DB test;

		-- ------------------------------

		SELECT * FROM "test" WHERE person:test = person:test;
		SELECT * FROM "test" WHERE person:test = "person:test";
		SELECT * FROM "test" WHERE person:test ∈ array(person:test);

		-- ------------------------------

		SELECT * FROM "test" WHERE person:test = person:test;
		SELECT * FROM "test" WHERE person:test != person:test;
		SELECT * FROM "test" WHERE person:test = user:test;
		SELECT * FROM "test" WHERE person:test != user:test;

		`

		i := 0
		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 8)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

	})

	Convey("Check binary bool comparisons", t, func() {

		setupDB(1)

		txt := `

		USE NS test DB test;

		-- ------------------------------

		SELECT * FROM "test" WHERE true = true;
		SELECT * FROM "test" WHERE true = "true";
		SELECT * FROM "test" WHERE true = /\w/;
		SELECT * FROM "test" WHERE true ∈ [true,false];

		-- ------------------------------

		SELECT * FROM "test" WHERE true = true;
		SELECT * FROM "test" WHERE true != true;
		SELECT * FROM "test" WHERE false = false;
		SELECT * FROM "test" WHERE false != false;

		`

		i := 0
		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 9)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)

	})

	Convey("Check binary float comparisons", t, func() {

		setupDB(1)

		txt := `

		USE NS test DB test;

		LET timer = "1987-06-22T08:30:30.511Z";

		-- ------------------------------

		SELECT * FROM "test" WHERE 1 = "1";
		SELECT * FROM "test" WHERE 1 = /\d/;
		SELECT * FROM "test" WHERE 1 ∈ [1,2,3];
		SELECT * FROM "test" WHERE 551349030511000000 = $timer;

		-- ------------------------------

		SELECT * FROM "test" WHERE 1 = 1;
		SELECT * FROM "test" WHERE 1.1 != 1.1;

		SELECT * FROM "test" WHERE 1 < 2;
		SELECT * FROM "test" WHERE 1 > 2;
		SELECT * FROM "test" WHERE 1 <= 2;
		SELECT * FROM "test" WHERE 1 >= 2;

		`

		i := 1
		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 12)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)

	})

	Convey("Check binary string comparisons", t, func() {

		setupDB(1)

		txt := `

		USE NS test DB test;

		LET timer = "1987-06-22T08:30:30.511Z";

		-- ------------------------------

		SELECT * FROM "test" WHERE "true" = true;
		SELECT * FROM "test" WHERE "1.1" = 1.1;
		SELECT * FROM "test" WHERE "person:test" = person:test;
		SELECT * FROM "test" WHERE "test" = /\w/;
		SELECT * FROM "test" WHERE "test" ∈ ["test","some"];
		SELECT * FROM "test" WHERE "1987-06-22 08:30:30.511 +0000 UTC" = $timer;

		-- ------------------------------

		SELECT * FROM "test" WHERE "test" = "test";
		SELECT * FROM "test" WHERE "test" != "test";

		SELECT * FROM "test" WHERE "abc" < "def";
		SELECT * FROM "test" WHERE "abc" > "def";
		SELECT * FROM "test" WHERE "abc" <= "def";
		SELECT * FROM "test" WHERE "abc" >= "def";

		SELECT * FROM "test" WHERE "a true test string" ∋ "test";
		SELECT * FROM "test" WHERE "a true test string" ∌ "test";
		SELECT * FROM "test" WHERE "test" ∈ "a true test string";
		SELECT * FROM "test" WHERE "test" ∉ "a true test string";

		SELECT * FROM "test" WHERE "a true test string" ~ "test";
		SELECT * FROM "test" WHERE "a true test string" ~ "Test";
		SELECT * FROM "test" WHERE "a true test string" !~ "test";
		SELECT * FROM "test" WHERE "a true test string" !~ "Test";
		SELECT * FROM "test" WHERE "a true test string" ?~ "test";
		SELECT * FROM "test" WHERE "a true test string" ?~ "Test";

		SELECT * FROM "test" WHERE "a true test string" = /test/;
		SELECT * FROM "test" WHERE "a true test string" = /Test/;
		SELECT * FROM "test" WHERE "a true test string" = /(?i)Test/;
		SELECT * FROM "test" WHERE "a true test string" != /test/;
		SELECT * FROM "test" WHERE "a true test string" != /Test/;
		SELECT * FROM "test" WHERE "a true test string" != /(?i)Test/;
		SELECT * FROM "test" WHERE "a true test string" ?= /test/;
		SELECT * FROM "test" WHERE "a true test string" ?= /Test/;
		SELECT * FROM "test" WHERE "a true test string" ?= /(?i)Test/;

		`

		i := 1
		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 33)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

	})

	Convey("Check binary time.Time comparisons", t, func() {

		setupDB(1)

		txt := `

		USE NS test DB test;

		LET timer = "1987-06-22T08:30:30.511Z";

		-- ------------------------------

		SELECT * FROM "test" WHERE $timer = "1987-06-22 08:30:30.511 +0000 UTC";
		SELECT * FROM "test" WHERE $timer = 551349030511000000;
		SELECT * FROM "test" WHERE $timer = $timer;
		SELECT * FROM "test" WHERE $timer = /\d/;
		SELECT * FROM "test" WHERE $timer ∈ array($timer);

		-- ------------------------------

		`

		i := 1
		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 7)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

	})

	Convey("Check binary array comparisons", t, func() {

		setupDB(1)

		txt := `

		USE NS test DB test;

		LET timer = "1987-06-22T08:30:30.511Z";

		-- ------------------------------

		SELECT * FROM "test" WHERE [true] ∋ true;
		SELECT * FROM "test" WHERE ["test"] ∋ "test";
		SELECT * FROM "test" WHERE [1,2,3] ∋ 1;
		SELECT * FROM "test" WHERE array($timer) ∋ $timer;
		SELECT * FROM "test" WHERE [1,2,3] = /\d/;
		SELECT * FROM "test" WHERE [{test:true}] ∋ {test:true};

		-- ------------------------------

		SELECT * FROM "test" WHERE [] = [];
		SELECT * FROM "test" WHERE [] != [];

		SELECT * FROM "test" WHERE [1,2,3] = [1,2,3];
		SELECT * FROM "test" WHERE [1,2,3] = [4,5,6];
		SELECT * FROM "test" WHERE [1,2,3] != [1,2,3];
		SELECT * FROM "test" WHERE [1,2,3] != [4,5,6];

		SELECT * FROM "test" WHERE [1,2,3] ∈ [ [1,2,3] ];
		SELECT * FROM "test" WHERE [1,2,3] ∉ [ [1,2,3] ];
		SELECT * FROM "test" WHERE [ [1,2,3] ] ∋ [1,2,3];
		SELECT * FROM "test" WHERE [ [1,2,3] ] ∌ [1,2,3];

		SELECT * FROM "test" WHERE [1,2,3,4,5] ⊇ [1,2,3];
		SELECT * FROM "test" WHERE [1,2,3,4,5] ⊇ [2,4,6];
		SELECT * FROM "test" WHERE [1,3,5,7,9] ⊃ [1,2,3];
		SELECT * FROM "test" WHERE [1,3,5,7,9] ⊃ [2,4,6];
		SELECT * FROM "test" WHERE [1,3,5,7,9] ⊅ [1,2,3];
		SELECT * FROM "test" WHERE [1,3,5,7,9] ⊅ [2,4,6];

		SELECT * FROM "test" WHERE [1,2,3] ⊆ [1,2,3,4,5];
		SELECT * FROM "test" WHERE [2,4,6] ⊆ [1,2,3,4,5];
		SELECT * FROM "test" WHERE [1,2,3] ⊂ [1,3,5,7,9];
		SELECT * FROM "test" WHERE [2,4,6] ⊂ [1,3,5,7,9];
		SELECT * FROM "test" WHERE [1,2,3] ⊄ [1,3,5,7,9];
		SELECT * FROM "test" WHERE [2,4,6] ⊄ [1,3,5,7,9];

		SELECT * FROM "test" WHERE [] = /[0-9]/;
		SELECT * FROM "test" WHERE [1,2,3] = /[0-9]/;
		SELECT * FROM "test" WHERE [1,"2",true] = /[0-9]/;
		SELECT * FROM "test" WHERE ["a","b","c"] = /[0-9]/;

		SELECT * FROM "test" WHERE [] != /[0-9]/;
		SELECT * FROM "test" WHERE [1,2,3] != /[0-9]/;
		SELECT * FROM "test" WHERE [1,"2",true] != /[0-9]/;
		SELECT * FROM "test" WHERE ["a","b","c"] != /[0-9]/;

		SELECT * FROM "test" WHERE [] ?= /[0-9]/;
		SELECT * FROM "test" WHERE [1,2,3] ?= /[0-9]/;
		SELECT * FROM "test" WHERE [1,"2",true] ?= /[0-9]/;
		SELECT * FROM "test" WHERE ["a","b","c"] ?= /[0-9]/;

		SELECT * FROM "test" WHERE [] ~ "pro";
		SELECT * FROM "test" WHERE [1,2,3] ~ "pro";
		SELECT * FROM "test" WHERE [1,"2","pro"] ~ "pro";
		SELECT * FROM "test" WHERE ["a","b","c","gopros"] ~ "Pro";
		SELECT * FROM "test" WHERE ["gopros","gopros","gopros"] ~ "Pro";

		SELECT * FROM "test" WHERE [] !~ "pro";
		SELECT * FROM "test" WHERE [1,2,3] !~ "pro";
		SELECT * FROM "test" WHERE [1,"2","pro"] !~ "pro";
		SELECT * FROM "test" WHERE ["a","b","c","gopros"] !~ "Pro";
		SELECT * FROM "test" WHERE ["gopros","gopros","gopros"] !~ "Pro";

		SELECT * FROM "test" WHERE [] ?~ "pro";
		SELECT * FROM "test" WHERE [1,2,3] ?~ "pro";
		SELECT * FROM "test" WHERE [1,"2","pro"] ?~ "pro";
		SELECT * FROM "test" WHERE ["a","b","c","gopros"] ?~ "Pro";
		SELECT * FROM "test" WHERE ["gopros","gopros","gopros"] ?~ "Pro";

		`

		i := 1
		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 57)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)

		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

	})

	Convey("Check binary object comparisons", t, func() {

		setupDB(1)

		txt := `

		USE NS test DB test;

		-- ------------------------------

		SELECT * FROM "test" WHERE {test:true} = {test:true};
		SELECT * FROM "test" WHERE {test:true} ∈ [{test:true}];

		-- ------------------------------

		SELECT * FROM "test" WHERE {test:true} = {test:true};
		SELECT * FROM "test" WHERE {test:true} != {test:true};
		SELECT * FROM "test" WHERE {test:true} = {other:true};
		SELECT * FROM "test" WHERE {test:true} != {other:true};

		`

		i := 0
		res, err := Execute(permsKV(), txt, nil)
		So(err, ShouldBeNil)
		So(res, ShouldHaveLength, 7)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

		So(res[inc(&i)].Result, ShouldHaveLength, 1)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 0)
		So(res[inc(&i)].Result, ShouldHaveLength, 1)

	})

}
