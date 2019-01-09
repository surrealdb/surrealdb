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

func (p *parser) parseDefineIndexStatement() (stmt *DefineIndexStatement, err error) {

	stmt = &DefineIndexStatement{}

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

	if _, _, err = p.shouldBe(COLUMNS); err != nil {
		return nil, err
	}

	if stmt.Cols, err = p.parseIdioms(); err != nil {
		return nil, err
	}

	_, _, stmt.Uniq = p.mightBe(UNIQUE)

	return

}

func (p *parser) parseRemoveIndexStatement() (stmt *RemoveIndexStatement, err error) {

	stmt = &RemoveIndexStatement{}

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
