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
	"fmt"
)

// FD ...
type FD struct {
	KV interface{}
	CF interface{}
	TK interface{}
	NS interface{}
	DB interface{}
	TB interface{}
	FD interface{}
}

// init initialises the key
func (k *FD) init() *FD {
	k.CF = "!"
	k.TK = "f"
	return k
}

// Encode encodes the key into binary
func (k *FD) Encode() []byte {
	k.init()
	return encode(k.KV, k.CF, k.TK, k.NS, k.DB, k.TB, k.FD)
}

// Decode decodes the key from binary
func (k *FD) Decode(data []byte) {
	k.init()
	decode(data, &k.KV, &k.CF, &k.TK, &k.NS, &k.DB, &k.TB, &k.FD)
}

// String returns a string representation of the key
func (k *FD) String() string {
	k.init()
	return fmt.Sprintf("/%s/%s/%s/%s/%s/%s/%s", k.KV, k.CF, k.TK, k.NS, k.DB, k.TB, k.FD)
}
