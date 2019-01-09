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

func (p *parser) parseDefineEventStatement() (stmt *DefineEventStatement, err error) {

	stmt = &DefineEventStatement{}

	if stmt.KV, stmt.NS, stmt.DB, err = p.a.get(AuthDB); err != nil {
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

	if _, _, err = p.shouldBe(WHEN); err != nil {
		return nil, err
	}

	if stmt.When, err = p.parseExpr(); err != nil {
		return nil, err
	}

	if _, _, err = p.shouldBe(THEN); err != nil {
		return nil, err
	}

	if stmt.Then, err = p.parseMult(); err != nil {
		return nil, err
	}

	return

}

func (p *parser) parseRemoveEventStatement() (stmt *RemoveEventStatement, err error) {

	stmt = &RemoveEventStatement{}

	if stmt.KV, stmt.NS, stmt.DB, err = p.a.get(AuthDB); err != nil {
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

	return

}
