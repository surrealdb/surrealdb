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

// TB ...
type TB struct {
	KV string // Base
	CF string // !
	TK string // t
	NS string // Namespace
	DB string // Database
	TB string // Table
}

// init initialises the key
func (k *TB) init() *TB {
	k.CF = "!"
	k.TK = "t"
	return k
}

// Encode encodes the key into binary
func (k *TB) Encode() []byte {
	return output(k.init())
}

// Decode decodes the key from binary
func (k *TB) Decode(data []byte) {
	injest(k, data)
}

// String returns a string representation of the key
func (k *TB) String() string {
	k.init()
	return "/" + strings.Join([]string{k.KV, k.CF, k.TK, k.NS, k.DB, k.TB}, "/")
}
