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

func (p *Parser) parseDefineIndexStatement() (*DefineIndexStatement, error) {

	stmt := &DefineIndexStatement{}

	var err error

	// Parse index name
	if stmt.Index, err = p.parseIdent(); err != nil {
		return nil, err
	}

	// Next token should be ON
	if _, _, err = p.shouldBe(ON); err != nil {
		return nil, err
	}

	// Parse table name
	if stmt.Table, err = p.parseTable(); err != nil {
		return nil, err
	}

	// Next token should be COLUMNS
	if _, _, err = p.shouldBe(COLUMNS); err != nil {
		return nil, err
	}

	// Parse columns
	if stmt.Fields, err = p.parseFields(); err != nil {
		return nil, err
	}

	// Parse unique
	_, _, stmt.Unique = p.mightBe(UNIQUE)

	// Next token should be EOF
	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return stmt, nil

}

func (p *Parser) parseResyncIndexStatement() (*ResyncIndexStatement, error) {

	stmt := &ResyncIndexStatement{}

	var err error

	// Parse index name
	if stmt.Index, err = p.parseIdent(); err != nil {
		return nil, err
	}

	// Next token should be ON
	if _, _, err = p.shouldBe(ON); err != nil {
		return nil, err
	}

	// Parse table name
	if stmt.Table, err = p.parseTable(); err != nil {
		return nil, err
	}

	// Next token should be EOF
	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return stmt, nil

}

func (p *Parser) parseRemoveIndexStatement() (*RemoveIndexStatement, error) {

	stmt := &RemoveIndexStatement{}

	var err error

	// Parse index name
	if stmt.Index, err = p.parseIdent(); err != nil {
		return nil, err
	}

	// Next token should be ON
	if _, _, err = p.shouldBe(ON); err != nil {
		return nil, err
	}

	// Parse table name
	if stmt.Table, err = p.parseTable(); err != nil {
		return nil, err
	}

	// Next token should be EOF
	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return stmt, nil

}
