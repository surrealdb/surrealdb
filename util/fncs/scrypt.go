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

	"github.com/elithrar/simple-scrypt"
)

func scryptCompare(ctx context.Context, args ...interface{}) (interface{}, error) {
	if h, ok := ensureString(args[0]); ok {
		if s, ok := ensureString(args[1]); ok {
			e := scrypt.CompareHashAndPassword([]byte(h), []byte(s))
			if e == nil {
				return true, nil
			}
		}
	}
	return false, nil
}

func scryptGenerate(ctx context.Context, args ...interface{}) (interface{}, error) {
	s, _ := ensureString(args[0])
	p := []byte(s)
	o := scrypt.DefaultParams
	return scrypt.GenerateFromPassword(p, o)
}
