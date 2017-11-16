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

	"golang.org/x/crypto/bcrypt"
)

func bcryptCompare(ctx context.Context, args ...interface{}) (bool, error) {
	if h, ok := ensureString(args[0]); ok {
		if s, ok := ensureString(args[1]); ok {
			e := bcrypt.CompareHashAndPassword([]byte(h), []byte(s))
			if e == nil {
				return true, nil
			}
		}
	}
	return false, nil
}

func bcryptGenerate(ctx context.Context, args ...interface{}) ([]byte, error) {
	s, _ := ensureString(args[0])
	p := []byte(s)
	o := bcrypt.DefaultCost
	return bcrypt.GenerateFromPassword(p, o)
}
