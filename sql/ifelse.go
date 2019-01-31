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

func (p *parser) parseIfelseStatement() (stmt *IfelseStatement, err error) {

	stmt = &IfelseStatement{}

	for {

		var tok Token

		if cond, err := p.parseExpr(); err != nil {
			return nil, err
		} else {
			stmt.Cond = append(stmt.Cond, cond)
		}

		if _, _, err = p.shouldBe(THEN); err != nil {
			return nil, err
		}

		if then, err := p.parseExpr(); err != nil {
			return nil, err
		} else {
			stmt.Then = append(stmt.Then, then)
		}

		// Check to see if the next token is an
		// ELSE keyword and if it is then check to
		// see if there is another if statement.

		if tok, _, err = p.shouldBe(ELSE, END); err != nil {
			return nil, err
		}

		if tok == END {
			return
		}

		if tok == ELSE {
			if _, _, exi := p.mightBe(IF); !exi {
				break
			}
		}

	}

	// Check to see if the next token is an
	// ELSE keyword and if it is then check to
	// see if there is another if statement.

	if then, err := p.parseExpr(); err != nil {
		return nil, err
	} else {
		stmt.Else = then
	}

	if _, _, err = p.shouldBe(END); err != nil {
		return nil, err
	}

	// If this query has any subqueries which
	// need to alter the database then mark
	// this query as a writeable statement.

	stmt.RW = p.buf.rw

	return

}
