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

func (p *parser) parseUseStatement() (stmt *UseStatement, err error) {

	stmt = &UseStatement{}

	var tok Token
	var exi bool

	tok, _, err = p.shouldBe(NAMESPACE, NS, DATABASE, DB)
	if err != nil {
		return nil, err
	}

	for {

		var ok bool
		var lit string
		var val interface{}

		if p.is(tok, NAMESPACE, NS) {

			tok, lit, err = p.shouldBe(IDENT, STRING, PARAM)
			if err != nil {
				return nil, &ParseError{Found: lit, Expected: []string{"namespace name"}}
			}

			switch tok {
			default:
				val, err = p.declare(STRING, lit)
			case PARAM:
				val, err = p.declare(PARAM, lit)
			}

			if stmt.NS, ok = val.(string); !ok || err != nil {
				return nil, &ParseError{Found: lit, Expected: []string{"namespace name as a STRING"}}
			}

			if err = p.o.ns(stmt.NS); err != nil {
				return nil, err
			}

		}

		if p.is(tok, DATABASE, DB) {

			tok, lit, err = p.shouldBe(IDENT, DATE, TIME, STRING, NUMBER, DOUBLE, PARAM)
			if err != nil {
				return nil, &ParseError{Found: lit, Expected: []string{"database name"}}
			}

			switch tok {
			default:
				val, err = p.declare(STRING, lit)
			case PARAM:
				val, err = p.declare(PARAM, lit)
			}

			if stmt.DB, ok = val.(string); !ok || err != nil {
				return nil, &ParseError{Found: lit, Expected: []string{"database name as a STRING"}}
			}

			if err = p.o.db(stmt.DB); err != nil {
				return nil, err
			}

		}

		tok, _, exi = p.mightBe(NAMESPACE, NS, DATABASE, DB)
		if !exi {
			break
		}

	}

	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return

}
