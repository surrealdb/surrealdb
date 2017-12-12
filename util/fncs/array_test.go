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

package fncs

import (
	"time"

	"context"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestArray(t *testing.T) {

	var res interface{}

	timer := time.Now()

	Convey("array() works properly", t, func() {
		res, _ = Run(context.Background(), "array", 1, 2, 3)
		So(res, ShouldResemble, []interface{}{1, 2, 3})
		res, _ = Run(context.Background(), "array", nil, true, "test", timer)
		So(res, ShouldResemble, []interface{}{nil, true, "test", timer})
	})

}
