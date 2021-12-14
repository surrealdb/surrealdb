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

package db

import (
	"github.com/dgraph-io/ristretto"
)

var keyCache *ristretto.Cache
var valCache *ristretto.Cache

func init() {

	keyCache, _ = ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,
		MaxCost:     1 << 32,
		BufferItems: 64,
		Cost: func(i interface{}) int64 {
			switch v := i.(type) {
			case string:
				return int64(len(v))
			case []byte:
				return int64(len(v))
			default:
				return 1
			}
		},
	})

	valCache, _ = ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,
		MaxCost:     1 << 32,
		BufferItems: 64,
		Cost: func(i interface{}) int64 {
			switch v := i.(type) {
			case string:
				return int64(len(v))
			case []byte:
				return int64(len(v))
			default:
				return 1
			}
		},
	})

}
