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

	if p.buf.txn {
		return nil, &TransError{}
	}

	tok, _, err = p.shouldBe(NAMESPACE, DATABASE, NS, DB)
	if err != nil {
		return nil, err
	}

	for {

		if is(tok, NAMESPACE, NS) {

			_, stmt.NS, err = p.shouldBe(IDENT, STRING, NUMBER, DOUBLE, DATE, TIME)
			if err != nil {
				return
			}

			if len(stmt.NS) == 0 {
				return nil, &ParseError{Expected: []string{"namespace name"}, Found: stmt.NS}
			}

			if err = p.o.ns(stmt.NS); err != nil {
				return nil, err
			}

		}

		if is(tok, DATABASE, DB) {

			_, stmt.DB, err = p.shouldBe(IDENT, STRING, NUMBER, DOUBLE, DATE, TIME)
			if err != nil {
				return
			}

			if len(stmt.DB) == 0 {
				return nil, &ParseError{Expected: []string{"database name"}, Found: stmt.DB}
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

	return

}
