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

package sql

func (p *Parser) parseType() (exp Ident, err error) {

	allowed := []string{"any", "url", "uuid", "color", "email", "phone", "array", "object", "domain", "string", "number", "custom", "boolean", "datetime", "latitude", "longitude"}

	tok, lit, err := p.shouldBe(IDENT)
	if err != nil {
		return Ident(""), err
	}

	if !contains(lit, allowed) {
		return Ident(""), &ParseError{Found: lit, Expected: allowed}
	}

	val, err := declare(tok, lit)

	return val.(Ident), err

}
