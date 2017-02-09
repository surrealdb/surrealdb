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

// VW ...
type VW struct {
	KV interface{}
	NS interface{}
	DB interface{}
	VW interface{}
}

// init initialises the key
func (k *VW) init() *VW {
	return k
}

// Encode encodes the key into binary
func (k *VW) Encode() []byte {
	k.init()
	return encode(k.KV, k.NS, "*", k.DB, "!", "v", k.VW)
}

// Decode decodes the key from binary
func (k *VW) Decode(data []byte) {
	k.init()
	decode(data, &k.KV, &k.NS, &skip, &k.DB, &skip, &skip, &k.VW)
}

// String returns a string representation of the key
func (k *VW) String() string {
	k.init()
	return output(k.KV, k.NS, "*", k.DB, "!", "v", k.VW)
}
