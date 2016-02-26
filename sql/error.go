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

package sql

import (
	"fmt"
	"strings"
)

// ParseError represents an error that occurred during parsing.
type ParseError struct {
	Found    string
	Expected []string
}

// Error returns the string representation of the error.
func (e *ParseError) Error() string {
	return fmt.Sprintf("found `%s` but expected `%s`", e.Found, strings.Join(e.Expected, ", "))
}
