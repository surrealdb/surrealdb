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

package item

import (
	"github.com/abcum/surreal/util/data"
	"github.com/abcum/surreal/util/diff"
)

func (this *Doc) diff() *data.Doc {

	va := this.initial.Data().(map[string]interface{})
	vb := this.current.Data().(map[string]interface{})

	dif := diff.Diff(va, vb).Out()

	if len(dif) == 0 {
		return data.Consume(nil)
	}

	return data.Consume(dif)

}
