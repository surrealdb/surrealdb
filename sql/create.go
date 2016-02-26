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

func (p *Parser) parseCreateStatement() (*CreateStatement, error) {

	stmt := &CreateStatement{}

	var err error

	// Next token might be INTO
	_, _, _ = p.mightBe(INTO)

	// Parse table name
	if stmt.What, err = p.parseTable(); err != nil {
		return nil, err
	}

	// Next token should be SET
	if _, _, err = p.shouldBe(SET); err != nil {
		return nil, err
	}

	// Parse data set
	if stmt.Data, err = p.parseSet(); err != nil {
		return nil, err
	}

	// Next token should be EOF
	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return stmt, nil

}
