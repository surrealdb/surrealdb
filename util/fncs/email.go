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
	"strings"

	"github.com/abcum/surreal/util/chck"
)

func emailUser(ctx context.Context, args ...interface{}) (interface{}, error) {
	v, _ := ensureString(args[0])
	if p := strings.LastIndexByte(v, '@'); p > 0 {
		return v[:p], nil
	}
	return nil, nil
}

func emailDomain(ctx context.Context, args ...interface{}) (interface{}, error) {
	v, _ := ensureString(args[0])
	if p := strings.LastIndexByte(v, '@'); p > 0 {
		return v[p+1:], nil
	}
	return nil, nil
}

func emailValid(ctx context.Context, args ...interface{}) (interface{}, error) {
	v, _ := ensureString(args[0])
	return chck.IsEmail(v), nil
}
