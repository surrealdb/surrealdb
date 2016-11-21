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

func (p *parser) parseDefineScopeStatement() (stmt *DefineScopeStatement, err error) {

	stmt = &DefineScopeStatement{}

	if stmt.KV, stmt.NS, stmt.DB, err = p.o.get(AuthDB); err != nil {
		return nil, err
	}

	if stmt.Name, err = p.parseName(); err != nil {
		return nil, err
	}

	for {

		tok, _, exi := p.mightBe(SESSION, SIGNUP, SIGNIN)
		if !exi {
			break
		}

		if p.is(tok, SESSION) {
			if stmt.Time, err = p.parseDuration(); err != nil {
				return nil, err
			}
		}

		if p.is(tok, SIGNUP) {
			_, _, _ = p.mightBe(AS)
			if stmt.Signup, err = p.parseExpr(); err != nil {
				return nil, err
			}
		}

		if p.is(tok, SIGNIN) {
			_, _, _ = p.mightBe(AS)
			if stmt.Signin, err = p.parseExpr(); err != nil {
				return nil, err
			}
		}

	}

	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return

}

func (p *parser) parseRemoveScopeStatement() (stmt *RemoveScopeStatement, err error) {

	stmt = &RemoveScopeStatement{}

	if stmt.KV, stmt.NS, stmt.DB, err = p.o.get(AuthDB); err != nil {
		return nil, err
	}

	if stmt.Name, err = p.parseName(); err != nil {
		return nil, err
	}

	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return

}
