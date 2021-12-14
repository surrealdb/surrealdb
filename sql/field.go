// Copyright © 2016 SurrealDB Ltd.
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

	if stmt.Name, err = p.parseIdiom(); err != nil {
		return nil, err
	}

	if _, _, err = p.shouldBe(ON); err != nil {
		return nil, err
	}

	if stmt.What, err = p.parseTables(); err != nil {
		return nil, err
	}

	for {

		tok, _, exi := p.mightBe(TYPE, VALUE, ASSERT, PRIORITY, PERMISSIONS)
		if !exi {
			break
		}

		if is(tok, TYPE) {
			if stmt.Type, stmt.Kind, err = p.parseType(); err != nil {
				return nil, err
			}
		}

		if is(tok, VALUE) {
			if stmt.Value, err = p.parseExpr(); err != nil {
				return nil, err
			}
		}

		if is(tok, ASSERT) {
			if stmt.Assert, err = p.parseExpr(); err != nil {
				return nil, err
			}
		}

		if is(tok, PRIORITY) {
			if stmt.Priority, err = p.parsePriority(); err != nil {
				return nil, err
			}
		}

		if is(tok, PERMISSIONS) {
			if stmt.Perms, err = p.parsePerms(); err != nil {
				return nil, err
			}
		}

	}

	return

}

func (p *parser) parseRemoveFieldStatement() (stmt *RemoveFieldStatement, err error) {

	stmt = &RemoveFieldStatement{}

	if stmt.Name, err = p.parseIdiom(); err != nil {
		return nil, err
	}

	if _, _, err = p.shouldBe(ON); err != nil {
		return nil, err
	}

	if stmt.What, err = p.parseTables(); err != nil {
		return nil, err
	}

	return

}
