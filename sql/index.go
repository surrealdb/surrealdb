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

	stmt.KV = p.c.Get("KV").(string)
	stmt.NS = p.c.Get("NS").(string)
	stmt.DB = p.c.Get("DB").(string)

	if stmt.Name, err = p.parseName(); err != nil {
		return nil, err
	}

	if _, _, err = p.shouldBe(ON); err != nil {
		return nil, err
	}

	if stmt.What, err = p.parseNames(); err != nil {
		return nil, err
	}

	if _, _, err = p.shouldBe(COLUMNS); err != nil {
		return nil, err
	}

	if stmt.Cols, err = p.parseNames(); err != nil {
		return nil, err
	}

	_, _, stmt.Uniq = p.mightBe(UNIQUE)

	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return

}

func (p *parser) parseRemoveIndexStatement() (stmt *RemoveIndexStatement, err error) {

	stmt = &RemoveIndexStatement{}

	stmt.KV = p.c.Get("KV").(string)
	stmt.NS = p.c.Get("NS").(string)
	stmt.DB = p.c.Get("DB").(string)

	if stmt.Name, err = p.parseName(); err != nil {
		return nil, err
	}

	if _, _, err = p.shouldBe(ON); err != nil {
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
