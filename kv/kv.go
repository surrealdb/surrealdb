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

package kv

var bucket = []byte("default")

// KV represents a key:value item in the database
type KV struct {
	// Exi is true if the key exists
	Exi bool `json:"-"`
	// Key is a byte slice of the key
	Key []byte `json:"key"`
	// Val is a byte slice of the value
	Val []byte `json:"val"`
}
