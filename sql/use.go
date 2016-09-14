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

func (p *parser) parseUseStatement(explain bool) (stmt *UseStatement, err error) {

	stmt = &UseStatement{}

	var tok Token
	var exi bool

	tok, _, err = p.shouldBe(NAMESPACE, DATABASE)
	if err != nil {
		return nil, err
	}

	for {

		if p.is(tok, NAMESPACE) {
			_, stmt.NS, err = p.shouldBe(IDENT, STRING)
			if err != nil {
				return nil, &ParseError{Found: stmt.NS, Expected: []string{"namespace name"}}
			}
			// TODO: need to make sure this user can access this NS
			p.c.Set("NS", stmt.NS)
		}

		if p.is(tok, DATABASE) {
			_, stmt.DB, err = p.shouldBe(IDENT, DATE, TIME, STRING, NUMBER, DOUBLE)
			if err != nil {
				return nil, &ParseError{Found: stmt.DB, Expected: []string{"database name"}}
			}
			// TODO: need to make sure this user can access this DB
			p.c.Set("DB", stmt.DB)
		}

		tok, _, exi = p.mightBe(NAMESPACE, DATABASE)
		if !exi {
			break
		}

	}

	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return

}
