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

package cnf

type Auth struct {
	Data     interface{} `json:"id" msgpack:"id"`
	Kind     Kind        `json:"-" msgpack:"-"`
	Scope    string      `json:"-" msgpack:"-"`
	Possible struct {
		NS string
		DB string
	} `json:"-" msgpack:"-"`
	Selected struct {
		NS string
		DB string
	} `json:"-" msgpack:"-"`
}

// Reset resets the authentication data.
func (a *Auth) Reset() *Auth {

	// Remove any saved session data
	a.Data = nil

	// Reset the authentication level
	a.Kind = AuthNO

	// Clear any authenticated scope
	a.Scope = ""

	return a

}
