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

func (p *parser) parseDefineFieldStatement() (stmt *DefineFieldStatement, err error) {

	stmt = &DefineFieldStatement{}

	if stmt.KV, stmt.NS, stmt.DB, err = p.o.get(AuthDB); err != nil {
		return nil, err
	}

	if stmt.Name, err = p.parseIdent(); err != nil {
		return nil, err
	}

	if _, _, err = p.shouldBe(ON); err != nil {
		return nil, err
	}

	if stmt.What, err = p.parseTables(); err != nil {
		return nil, err
	}

	for {

		tok, _, exi := p.mightBe(MIN, MAX, TYPE, ENUM, CODE, MATCH, DEFAULT, NOTNULL, READONLY, MANDATORY, VALIDATE, PERMISSIONS)
		if !exi {
			break
		}

		if p.is(tok, MIN) {
			if stmt.Min, err = p.parseDouble(); err != nil {
				return nil, err
			}
		}

		if p.is(tok, MAX) {
			if stmt.Max, err = p.parseDouble(); err != nil {
				return nil, err
			}
		}

		if p.is(tok, TYPE) {
			if stmt.Type, err = p.parseType(); err != nil {
				return nil, err
			}
		}

		if p.is(tok, ENUM) {
			if stmt.Enum, err = p.parseArray(); err != nil {
				return nil, err
			}
		}

		if p.is(tok, CODE) {
			if stmt.Code, err = p.parseScript(); err != nil {
				return nil, err
			}
		}

		if p.is(tok, MATCH) {
			if stmt.Match, err = p.parseRegexp(); err != nil {
				return nil, err
			}
		}

		if p.is(tok, DEFAULT) {
			if stmt.Default, err = p.parseExpr(); err != nil {
				return nil, err
			}
		}

		if p.is(tok, NOTNULL) {
			stmt.Notnull = true
			if tok, _, exi := p.mightBe(TRUE, FALSE); exi {
				if tok == FALSE {
					stmt.Notnull = false
				}
			}
		}

		if p.is(tok, READONLY) {
			stmt.Readonly = true
			if tok, _, exi := p.mightBe(TRUE, FALSE); exi {
				if tok == FALSE {
					stmt.Readonly = false
				}
			}
		}

		if p.is(tok, MANDATORY) {
			stmt.Mandatory = true
			if tok, _, exi := p.mightBe(TRUE, FALSE); exi {
				if tok == FALSE {
					stmt.Mandatory = false
				}
			}
		}

		if p.is(tok, VALIDATE) {
			stmt.Validate = true
			if tok, _, exi := p.mightBe(TRUE, FALSE); exi {
				if tok == FALSE {
					stmt.Validate = false
				}
			}
		}

		if p.is(tok, PERMISSIONS) {
			if stmt.Perm, err = p.parsePerms(); err != nil {
				return nil, err
			}
		}

	}

	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return

}

func (p *parser) parseRemoveFieldStatement() (stmt *RemoveFieldStatement, err error) {

	stmt = &RemoveFieldStatement{}

	if stmt.KV, stmt.NS, stmt.DB, err = p.o.get(AuthDB); err != nil {
		return nil, err
	}

	if stmt.Name, err = p.parseIdent(); err != nil {
		return nil, err
	}

	if _, _, err = p.shouldBe(ON); err != nil {
		return nil, err
	}

	if stmt.What, err = p.parseTables(); err != nil {
		return nil, err
	}

	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return

}
