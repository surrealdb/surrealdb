// Copyright © 2016 SurrealDB Ltd.
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

package cnf

const (
	// Root access
	AuthKV Kind = iota
	// Namespace access
	AuthNS
	// Database access
	AuthDB
	// Scoped user access
	AuthSC
	// No access
	AuthNO
)

type Kind int

func (k Kind) String() string {
	switch k {
	default:
		return "NO"
	case AuthKV:
		return "KV"
	case AuthNS:
		return "NS"
	case AuthDB:
		return "DB"
	case AuthSC:
		return "SC"
	}
}

func (k Kind) MarshalText() (data []byte, err error) {
	return []byte(k.String()), err
}

func (k Kind) UnmarshalText(text []byte) error {
	return nil
}
