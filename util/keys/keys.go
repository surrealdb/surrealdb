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
	cTERM   = uint8(0x00)
	cPREFIX = uint8(0x01)
	cNILL   = uint8(0x02)
	cBOOL   = uint8(0x03)
	cTIME   = uint8(0x04)
	cNUMBER = uint8(0x05)
	cSTRING = uint8(0x06)
	cARRAY  = uint8(0x07)
	cSUFFIX = uint8(0x08)
)

var (
	bTERM   = []byte("\x00")
	bPREFIX = []byte("\x01")
	bNILL   = []byte("\x02")
	bBOOL   = []byte("\x03")
	bTIME   = []byte("\x04")
	bNUMBER = []byte("\x05")
	bSTRING = []byte("\x06")
	bARRAY  = []byte("\x07")
	bSUFFIX = []byte("\x08")
)

// Key ...
type Key interface {
	String() string
	Encode() []byte
	Decode(data []byte)
}
