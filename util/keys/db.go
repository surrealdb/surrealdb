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

package keys

import (
	"strings"
)

// DB ...
type DB struct {
	KV string // KV
	CF string // !
	TK string // d
	NS string // NS
	DB string // DB
}

// init initialises the key
func (k *DB) init() *DB {
	k.CF = "!"
	k.TK = "d"
	return k
}

// Encode encodes the key into binary
func (k *DB) Encode() []byte {
	k.init()
	return encode(k.KV, k.CF, k.TK, k.NS, k.DB)
}

// Decode decodes the key from binary
func (k *DB) Decode(data []byte) {
	k.init()
	decode(data, &k.KV, &k.CF, &k.TK, &k.NS, &k.DB)
}

// String returns a string representation of the key
func (k *DB) String() string {
	k.init()
	return "/" + strings.Join([]string{k.KV, k.CF, k.TK, k.NS, k.DB}, "/")
}
