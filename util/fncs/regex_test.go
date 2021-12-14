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

package fncs

import (
	"context"
	"regexp"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestRegex(t *testing.T) {

	var res interface{}

	Convey("regex() works properly", t, func() {
		res, _ = Run(context.Background(), "regex", "something")
		So(res, ShouldResemble, regexp.MustCompile("something"))
		res, _ = Run(context.Background(), "regex", `^[a-z]+\[[0-9]+\]$`)
		So(res, ShouldResemble, regexp.MustCompile(`^[a-z]+\[[0-9]+\]$`))
	})

}
