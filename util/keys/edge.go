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

// Edge ...
type Edge struct {
	KV interface{}
	NS interface{}
	DB interface{}
	TB interface{}
	TK interface{}
	ID interface{}
	TP interface{}
	FK interface{}
}

// init initialises the key
func (k *Edge) init() *Edge {
	if k.TK == nil {
		k.TK = "«»"
	}
	return k
}

// Encode encodes the key into binary
func (k *Edge) Encode() []byte {
	k.init()
	return encode(k.KV, k.NS, k.DB, k.TB, k.TK, k.ID, k.TP, k.FK)
}

// Decode decodes the key from binary
func (k *Edge) Decode(data []byte) {
	k.init()
	decode(data, &k.KV, &k.NS, &k.DB, &k.TB, &k.TK, &k.ID, &k.TP, &k.FK)
}

// String returns a string representation of the key
func (k *Edge) String() string {
	k.init()
	return fmt.Sprintf("/%s/%s/%s/%s/%s/%s/%s/%s", k.KV, k.NS, k.DB, k.TB, k.TK, k.ID, k.TP, k.FK)
}
