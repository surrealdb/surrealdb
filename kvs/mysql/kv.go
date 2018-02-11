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

package mysql

// KV represents a row stored in the database.
type KV struct {
	ver uint64
	key []byte
	val []byte
}

// Exi returns whether this key-value item actually exists.
func (kv *KV) Exi() bool {
	return kv.val != nil
}

// Key returns the key for the underlying key-value item.
func (kv *KV) Key() []byte {
	return kv.key
}

// Val returns the value for the underlying key-value item.
func (kv *KV) Val() []byte {
	return kv.val
}

// Ver returns the version for the underlying key-value item.
func (kv *KV) Ver() uint64 {
	return kv.ver
}
