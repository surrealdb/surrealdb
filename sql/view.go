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

func (p *parser) parseDefineViewStatement() (stmt *DefineViewStatement, err error) {

	stmt = &DefineViewStatement{}

	if stmt.KV, stmt.NS, stmt.DB, err = p.o.get(AuthDB); err != nil {
		return nil, err
	}

	if stmt.Name, err = p.parseIdent(); err != nil {
		return nil, err
	}

	_, _, err = p.shouldBe(AS)
	if err != nil {
		return nil, err
	}

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

	if stmt.What, err = p.parseWhat(); err != nil {
		return nil, err
	}

	if stmt.Cond, err = p.parseCond(); err != nil {
		return nil, err
	}

	if stmt.Group, err = p.parseGroup(); err != nil {
		return nil, err
	}

	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return

}

func (p *parser) parseRemoveViewStatement() (stmt *RemoveViewStatement, err error) {

	stmt = &RemoveViewStatement{}

	if stmt.KV, stmt.NS, stmt.DB, err = p.o.get(AuthDB); err != nil {
		return nil, err
	}

	if stmt.Name, err = p.parseIdent(); err != nil {
		return nil, err
	}

	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return

}
