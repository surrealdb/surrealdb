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

func (p *parser) parseDefineTableStatement() (stmt *DefineTableStatement, err error) {

	stmt = &DefineTableStatement{}

	if stmt.What, err = p.parseTables(); err != nil {
		return nil, err
	}

	for {

		tok, _, exi := p.mightBe(DROP, SCHEMAFULL, SCHEMALESS, VERSIONED, UNVERSIONED, PERMISSIONS, AS)
		if !exi {
			break
		}

		if is(tok, DROP) {
			stmt.Drop = true
		}

		if is(tok, SCHEMAFULL) {
			stmt.Full = true
		}

		if is(tok, SCHEMALESS) {
			stmt.Full = false
		}

		if is(tok, VERSIONED) {
			stmt.Vers = true
		}

		if is(tok, UNVERSIONED) {
			stmt.Vers = false
		}

		if is(tok, PERMISSIONS) {
			if stmt.Perms, err = p.parsePerms(); err != nil {
				return nil, err
			}
		}

		if is(tok, AS) {

			stmt.Lock = true

			_, _, _ = p.mightBe(LPAREN)

			_, _, err = p.shouldBe(SELECT)
			if err != nil {
				return nil, err
			}

			if stmt.Expr, err = p.parseFields(); err != nil {
				return nil, err
			}

			_, _, err = p.shouldBe(FROM)
			if err != nil {
				return nil, err
			}

			if stmt.From, err = p.parseTables(); err != nil {
				return nil, err
			}

			if stmt.Cond, err = p.parseCond(); err != nil {
				return nil, err
			}

			if stmt.Group, err = p.parseGroup(); err != nil {
				return nil, err
			}

			_, _, _ = p.mightBe(RPAREN)

			if err = checkExpression(rolls, stmt.Expr, stmt.Group); err != nil {
				return nil, err
			}

		}

	}

	return

}

func (p *parser) parseRemoveTableStatement() (stmt *RemoveTableStatement, err error) {

	stmt = &RemoveTableStatement{}

	if stmt.What, err = p.parseTables(); err != nil {
		return nil, err
	}

	return

}
