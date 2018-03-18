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

	if stmt.Name, err = p.parseIdent(); err != nil {
		return nil, err
	}

	for {

		tok, _, exi := p.mightBe(SESSION, SIGNUP, SIGNIN, CONNECT, ON)
		if !exi {
			break
		}

		if is(tok, SESSION) {
			if stmt.Time, err = p.parseDuration(); err != nil {
				return nil, err
			}
		}

		if is(tok, SIGNUP) {
			_, _, _ = p.mightBe(AS)
			if stmt.Signup, err = p.parseExpr(); err != nil {
				return nil, err
			}
		}

		if is(tok, SIGNIN) {
			_, _, _ = p.mightBe(AS)
			if stmt.Signin, err = p.parseExpr(); err != nil {
				return nil, err
			}
		}

		if is(tok, CONNECT) {
			_, _, _ = p.mightBe(AS)
			if stmt.Connect, err = p.parseExpr(); err != nil {
				return nil, err
			}
		}

		if is(tok, ON) {

			tok, _, err = p.shouldBe(SIGNIN, SIGNUP)
			if err != nil {
				return nil, err
			}

			switch tok {
			case SIGNUP:
				if stmt.OnSignup, err = p.parseMult(); err != nil {
					return nil, err
				}
			case SIGNIN:
				if stmt.OnSignin, err = p.parseMult(); err != nil {
					return nil, err
				}
			}

		}

	}

	return

}

func (p *parser) parseRemoveScopeStatement() (stmt *RemoveScopeStatement, err error) {

	stmt = &RemoveScopeStatement{}

	if stmt.KV, stmt.NS, stmt.DB, err = p.o.get(AuthDB); err != nil {
		return nil, err
	}

	if stmt.Name, err = p.parseIdent(); err != nil {
		return nil, err
	}

	return

}
