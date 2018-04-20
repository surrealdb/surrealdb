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

package db

import (
	"context"
)

func vers(ctx context.Context) int {

	v := ctx.Value(ctxKeyDive)

	switch v {
	case nil:
		return 0
	default:
		return v.(int)
	}

}

func dive(ctx context.Context) context.Context {

	v := ctx.Value(ctxKeyDive)

	switch v {
	case nil:
		return context.WithValue(ctx, ctxKeyDive, 1)
	default:
		if v.(int) > maxRecursiveQueries {
			panic(errRecursiveOverload)
		}
		return context.WithValue(ctx, ctxKeyDive, v.(int)+1)
	}

}
