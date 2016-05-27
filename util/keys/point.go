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

// Point ...
type Point struct {
	KV interface{}
	NS interface{}
	DB interface{}
	TB interface{}
	TK interface{}
	IX interface{}
	FD interface{}
	ID interface{}
}

// init initialises the key
func (k *Point) init() *Point {
	k.TK = "∆"
	return k
}

// Encode encodes the key into binary
func (k *Point) Encode() []byte {
	k.init()
	return encode(k.KV, k.NS, k.DB, k.TB, k.TK, k.IX, k.FD, k.ID)
}

// Decode decodes the key from binary
func (k *Point) Decode(data []byte) {
	k.init()
	decode(data, &k.KV, &k.NS, &k.DB, &k.TB, &k.TK, &k.IX, &k.FD, &k.ID)
}

// String returns a string representation of the key
func (k *Point) String() string {
	k.init()
	return fmt.Sprintf("/%s/%s/%s/%s/%s/%s/%v/%s", k.KV, k.NS, k.DB, k.TB, k.TK, k.IX, k.FD, k.ID)
}
