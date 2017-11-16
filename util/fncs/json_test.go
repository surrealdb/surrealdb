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
	"context"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestJson(t *testing.T) {

	var res interface{}

	Convey("json.decode(a) works properly", t, func() {
		res, _ = Run(context.Background(), "json.decode", "true")
		So(res, ShouldResemble, true)
		res, _ = Run(context.Background(), "json.decode", "13579")
		So(res, ShouldResemble, float64(13579))
		res, _ = Run(context.Background(), "json.decode", `{"test":true}`)
		So(res, ShouldResemble, map[string]interface{}{
			"test": true,
		})
	})

	Convey("json.encode(a) works properly", t, func() {
		res, _ = Run(context.Background(), "json.encode", true)
		So(res, ShouldResemble, []byte("true"))
		res, _ = Run(context.Background(), "json.encode", 13579)
		So(res, ShouldResemble, []byte("13579"))
		res, _ = Run(context.Background(), "json.encode", map[string]interface{}{"test": true})
		So(res, ShouldResemble, []byte(`{"test":true}`))
	})

}
