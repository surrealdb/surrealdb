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

package pgsql

// KV represents a database key:value item
type KV struct {
	exi bool
	key []byte
	val []byte
}

// Exists is true if the key exists
func (kv *KV) Exists() bool {
	return kv.exi
}

// Key returns a byte slice of the key
func (kv *KV) Key() []byte {
	return kv.key
}

// Val returns a byte slice of the value
func (kv *KV) Val() []byte {
	return kv.val
}

// Str returns a string of the value
func (kv *KV) Str() string {
	return string(kv.val)
}
