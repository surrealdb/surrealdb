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

package indx

import (
	"reflect"

	"github.com/surrealdb/surrealdb/sql"
	"github.com/surrealdb/surrealdb/util/data"
)

func Diff(old, now [][]interface{}) (del, add [][]interface{}) {

loopo:
	for _, ov := range old {
		for _, nv := range now {
			if reflect.DeepEqual(ov, nv) {
				continue loopo
			}
		}
		del = append(del, ov)
	}

loopn:
	for _, nv := range now {
		for _, ov := range old {
			if reflect.DeepEqual(ov, nv) {
				continue loopn
			}
		}
		add = append(add, nv)
	}

	return

}

func Build(cols sql.Idents, item *data.Doc) (out [][]interface{}) {

	if len(cols) == 0 {
		return [][]interface{}{nil}
	}

	col, cols := cols[0], cols[1:]

	sub := Build(cols, item)

	if arr, ok := item.Get(col.VA).Data().([]interface{}); ok {
		for _, s := range sub {
			for _, a := range arr {
				idx := []interface{}{}
				idx = append(idx, a)
				idx = append(idx, s...)
				out = append(out, idx)
			}
		}
	} else {
		for _, s := range sub {
			idx := []interface{}{}
			idx = append(idx, item.Get(col.VA).Data())
			idx = append(idx, s...)
			out = append(out, idx)
		}
	}

	return

}
