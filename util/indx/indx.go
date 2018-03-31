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

package indx

import (
	"reflect"

	"github.com/abcum/surreal/sql"
	"github.com/abcum/surreal/util/data"
)

func Diff(old, now [][]interface{}) (_, _ [][]interface{}) {

	for i := len(old) - 1; i >= 0; i-- {
		if reflect.DeepEqual(old[i], now[i]) {
			old = append(old[:i], old[i+1:]...)
			now = append(now[:i], now[i+1:]...)
		}
	}

	return old, now

}

func Build(cols sql.Idents, item *data.Doc) (out [][]interface{}) {

	if len(cols) == 0 {
		return [][]interface{}{nil}
	}

	col, cols := cols[0], cols[1:]

	sub := Build(cols, item)

	if arr, ok := item.Get(col.ID).Data().([]interface{}); ok {
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
			idx = append(idx, item.Get(col.ID).Data())
			idx = append(idx, s...)
			out = append(out, idx)
		}
	}

	return

}
