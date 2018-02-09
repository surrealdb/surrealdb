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

// Namespace ...
type Namespace struct {
	KV string
	NS string
}

// init initialises the key
func (k *Namespace) init() *Namespace {
	return k
}

// Copy creates a copy of the key
func (k *Namespace) Copy() *Namespace {
	return &Namespace{
		KV: k.KV,
		NS: k.NS,
	}
}

// Encode encodes the key into binary
func (k *Namespace) Encode() []byte {
	k.init()
	return encode(k.KV, "*", k.NS)
}

// Decode decodes the key from binary
func (k *Namespace) Decode(data []byte) {
	k.init()
	var __ string
	decode(data, &k.KV, &__, &k.NS)
}

// String returns a string representation of the key
func (k *Namespace) String() string {
	k.init()
	return output(k.KV, "*", k.NS)
}
