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

package diff

import (
	"testing"

	"github.com/abcum/surreal/sql"

	. "github.com/smartystreets/goconvey/convey"
)

var old = map[string]interface{}{
	"age":  18,
	"item": sql.NewThing("test", 1),
	"name": map[string]interface{}{
		"first": "T",
		"last":  "M H",
	},
	"dates":     []interface{}{1, 2, 4},
	"changing":  true,
	"different": "true",
}

var now = map[string]interface{}{
	"age":  29,
	"item": sql.NewThing("test", 2),
	"name": map[string]interface{}{
		"first": "Tobie",
		"last":  "Morgan Hitchcock",
	},
	"changed":   "This is a string",
	"different": true,
	"dates":     []interface{}{1, 2, 3, 4, 4},
	"addedArr":  []interface{}{1, 2, 3},
	"addedMap": map[string]interface{}{
		"first": map[string]interface{}{
			"embedded": true,
		},
	},
}

var chg = []interface{}{
	map[string]interface{}{
		"op":    "add",
		"path":  "/addedArr",
		"value": []interface{}{1, 2, 3},
	},
	map[string]interface{}{
		"op":   "add",
		"path": "/addedMap",
		"value": map[string]interface{}{
			"first": map[string]interface{}{
				"embedded": true,
			},
		},
	},
	map[string]interface{}{
		"op":    "replace",
		"path":  "/age",
		"value": 29,
	},
	map[string]interface{}{
		"op":    "add",
		"path":  "/changed",
		"value": "This is a string",
	},
	map[string]interface{}{
		"op":   "remove",
		"path": "/changing",
	},
	map[string]interface{}{
		"op":    "replace",
		"path":  "/dates/2",
		"value": 3,
	},
	map[string]interface{}{
		"op":    "add",
		"path":  "/dates/3",
		"value": 4,
	},
	map[string]interface{}{
		"op":    "add",
		"path":  "/dates/4",
		"value": 4,
	},
	map[string]interface{}{
		"op":    "replace",
		"path":  "/different",
		"value": true,
	},
	map[string]interface{}{
		"op":    "replace",
		"path":  "/item",
		"value": sql.NewThing("test", 2),
	},
	map[string]interface{}{
		"op":    "change",
		"path":  "/name/first",
		"value": "=1\t+obie",
	},
	map[string]interface{}{
		"op":    "change",
		"path":  "/name/last",
		"value": "=1\t+organ\t=2\t+itchcock",
	},
}

func TestMain(t *testing.T) {

	var obj interface{}
	var dif []interface{}

	Convey("Confirm that the item can be diffed correctly", t, func() {
		dif = Diff(old, now)
		So(dif, ShouldResemble, chg)
	})

	Convey("Confirm that the item can be patched correctly", t, func() {
		obj = Patch(old, dif)
		So(obj, ShouldResemble, now)
	})

}

func BenchmarkDiff(b *testing.B) {

	for n := 0; n < b.N; n++ {
		Diff(old, now)
	}

}

func BenchmarkPatch(b *testing.B) {

	for n := 0; n < b.N; n++ {
		Patch(old, chg)
	}

}
