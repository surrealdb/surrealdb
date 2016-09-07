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

func (p *Parser) parseWhat() (mul []Expr, err error) {

	for {

		tok, lit, err := p.shouldBe(IDENT, THING)
		if err != nil {
			return nil, &ParseError{Found: lit, Expected: []string{"table name or record id"}}
		}

		if p.is(tok, IDENT) {
			one, _ := p.declare(TABLE, lit)
			mul = append(mul, one)
		}

		if p.is(tok, THING) {
			one, _ := p.declare(THING, lit)
			mul = append(mul, one)
		}

		// If the next token is not a comma then break the loop.
		if _, _, exi := p.mightBe(COMMA); !exi {
			break
		}

	}

	return

}
