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

import "time"

const (
	// Ignore specifies an ignored field
	Ignore = ignore
	// Prefix is the lowest char found in a key
	Prefix = prefix
	// Suffix is the highest char found in a key
	Suffix = suffix
	// Ignore specifies an ignored field
	ignore = "\x01"
	// Prefix is the lowest char found in a key
	prefix = "\x00"
	// Suffix is the highest char found in a key
	suffix = "\xff"
)

var (
	// StartOfTime is a datetime in the past
	StartOfTime = time.Unix(0, 0)
	// EndOfTime is a datetime in the future
	EndOfTime = time.Now().AddDate(50, 0, 0)
)

var (
	cEND = byte(0x00)
	cPRE = byte(0x01)
	cNIL = byte(0x02)
	cVAL = byte(0x03)
	cTME = byte(0x04)
	cNEG = byte(0x05)
	cPOS = byte(0x06)
	cSTR = byte(0x07)
	cARR = byte(0x08)
	cSUF = byte(0x09)
)

var (
	bEND = []byte{cEND}
	bPRE = []byte{cPRE}
	bNIL = []byte{cNIL}
	bVAL = []byte{cVAL}
	bTME = []byte{cTME}
	bNEG = []byte{cNEG}
	bPOS = []byte{cPOS}
	bSTR = []byte{cSTR}
	bARR = []byte{cARR}
	bSUF = []byte{cSUF}
)

const (
	// MinNumber is the minimum number which can be accurately serialized
	MinNumber = -1 << 53
	// MaxNumber is the maximum number which can be accurately serialized
	MaxNumber = 1<<53 - 1
)

// Key ...
type Key interface {
	String() string
	Encode() []byte
	Decode(data []byte)
}
