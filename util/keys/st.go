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

// ST ...
type ST struct {
	KV string
	NS string
	DB string
	SC string
	TK string
}

// init initialises the key
func (k *ST) init() *ST {
	return k
}

// Encode encodes the key into binary
func (k *ST) Encode() []byte {
	k.init()
	return encode(k.KV, k.NS, "*", k.DB, "!", "s", k.SC, "!", "t", k.TK)
}

// Decode decodes the key from binary
func (k *ST) Decode(data []byte) {
	k.init()
	decode(data, &k.KV, &k.NS, &skip, &k.DB, &skip, &skip, &k.SC, &skip, &skip, &k.TK)
}

// String returns a string representation of the key
func (k *ST) String() string {
	k.init()
	return output(k.KV, k.NS, "*", k.DB, "!", "s", k.SC, "!", "t", k.TK)
}
