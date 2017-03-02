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

		var val *Ident

		if p.is(tok, NAMESPACE, NS) {

			if val, err = p.parseIdent(); err != nil {
				return nil, err
			}

			stmt.NS = val.ID

			if err = p.o.ns(stmt.NS); err != nil {
				return nil, err
			}

		}

		if p.is(tok, DATABASE, DB) {

			if val, err = p.parseIdent(); err != nil {
				return nil, err
			}

			stmt.DB = val.ID

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
