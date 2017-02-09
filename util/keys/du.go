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

// DU ...
type DU struct {
	KV interface{}
	NS interface{}
	DB interface{}
	US interface{}
}

// init initialises the key
func (k *DU) init() *DU {
	return k
}

// Encode encodes the key into binary
func (k *DU) Encode() []byte {
	k.init()
	return encode(k.KV, k.NS, "*", k.DB, "!", "u", k.US)
}

// Decode decodes the key from binary
func (k *DU) Decode(data []byte) {
	k.init()
	decode(data, &k.KV, &k.NS, &skip, &k.DB, &skip, &skip, &k.US)
}

// String returns a string representation of the key
func (k *DU) String() string {
	k.init()
	return output(k.KV, k.NS, "*", k.DB, "!", "u", k.US)
}
