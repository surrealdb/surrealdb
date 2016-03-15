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

import "github.com/abcum/surreal/util/bytes"

const (
	// Prefix is the lowest char found in a key
	Prefix = "\x00"
	// Suffix is the highest char found in a key
	Suffix = "\xff"
)

// Key ...
type Key interface {
	String() string
	Encode() []byte
	Decode(data []byte)
}

func output(k Key) []byte {
	return bytes.Encode(k)
}

func injest(k Key, d []byte) {
	bytes.Decode(d, k)
}
