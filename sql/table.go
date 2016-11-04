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

func (p *parser) parseDefineTableStatement() (stmt *DefineTableStatement, err error) {

	stmt = &DefineTableStatement{}

	if stmt.KV, stmt.NS, stmt.DB, err = p.o.get(AuthDB); err != nil {
		return nil, err
	}

	if stmt.What, err = p.parseNames(); err != nil {
		return nil, err
	}

	if tok, _, exi := p.mightBe(SCHEMAFULL, SCHEMALESS); exi && tok == SCHEMAFULL {
		stmt.Full = true
	}

	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return

}

func (p *parser) parseRemoveTableStatement() (stmt *RemoveTableStatement, err error) {

	stmt = &RemoveTableStatement{}

	if stmt.KV, stmt.NS, stmt.DB, err = p.o.get(AuthDB); err != nil {
		return nil, err
	}

	if stmt.What, err = p.parseNames(); err != nil {
		return nil, err
	}

	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return

}
