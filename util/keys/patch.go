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
	"time"
)

// Patch ...
type Patch struct {
	KV string
	NS string
	DB string
	TB string
	ID interface{}
	AT time.Time
}

// init initialises the key
func (k *Patch) init() *Patch {
	if k.AT.IsZero() {
		k.AT = time.Now()
	}
	return k
}

// Copy creates a copy of the key
func (k *Patch) Copy() *Patch {
	return &Patch{
		KV: k.KV,
		NS: k.NS,
		DB: k.DB,
		TB: k.TB,
		ID: k.ID,
		AT: k.AT,
	}
}

// Encode encodes the key into binary
func (k *Patch) Encode() []byte {
	k.init()
	return encode(k.KV, k.NS, "*", k.DB, "*", k.TB, "~", k.ID, k.AT)
}

// Decode decodes the key from binary
func (k *Patch) Decode(data []byte) {
	k.init()
	decode(data, &k.KV, &k.NS, &skip, &k.DB, &skip, &k.TB, &skip, &k.ID, &k.AT)
}

// String returns a string representation of the key
func (k *Patch) String() string {
	k.init()
	return output(k.KV, k.NS, "*", k.DB, "*", k.TB, "~", k.ID, k.AT.Format(time.RFC3339Nano))
}
