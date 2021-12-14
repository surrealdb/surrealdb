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
)

func purge(ctx context.Context, args ...interface{}) (interface{}, error) {
	if arr, ok := ensureSlice(args[0]); ok {
		arr = copySlice(arr)
		for i := len(arr) - 1; i >= 0; i-- {
			if arr[i] == nil {
				copy(arr[i:], arr[i+1:])
				arr[len(arr)-1] = nil
				arr = arr[:len(arr)-1]
			}
		}
		return arr, nil
	}
	if args[0] == nil {
		return nil, nil
	}
	return args[0], nil
}

func purgeIf(ctx context.Context, args ...interface{}) (interface{}, error) {
	if arr, ok := ensureSlice(args[0]); ok {
		arr = copySlice(arr)
		for i := len(arr) - 1; i >= 0; i-- {
			if arr[i] == args[1] {
				copy(arr[i:], arr[i+1:])
				arr[len(arr)-1] = nil
				arr = arr[:len(arr)-1]
			}
		}
		return arr, nil
	}
	if args[0] == args[1] {
		return nil, nil
	}
	return args[0], nil
}

func purgeNot(ctx context.Context, args ...interface{}) (interface{}, error) {
	if arr, ok := ensureSlice(args[0]); ok {
		arr = copySlice(arr)
		for i := len(arr) - 1; i >= 0; i-- {
			if arr[i] != args[1] {
				copy(arr[i:], arr[i+1:])
				arr[len(arr)-1] = nil
				arr = arr[:len(arr)-1]
			}
		}
		return arr, nil
	}
	if args[0] != args[1] {
		return nil, nil
	}
	return args[0], nil
}
