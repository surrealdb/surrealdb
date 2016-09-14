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
	"github.com/abcum/surreal/sql"
)

func (this *Doc) Yield(output sql.Token, fallback sql.Token) (res interface{}) {

	if output == 0 {
		output = fallback
	}

	switch output {
	case sql.ID:
		res = this.id
	case sql.DIFF:
		res = this.diff().Data()
	case sql.AFTER:
		res = this.current.Data()
	case sql.BEFORE:
		res = this.initial.Data()
	case sql.BOTH:
		res = map[string]interface{}{
			"After":  this.current.Data(),
			"Before": this.initial.Data(),
		}
	}

	return

}
