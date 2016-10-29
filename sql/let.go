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

func (p *parser) parseLetStatement() (stmt *LetStatement, err error) {

	stmt = &LetStatement{}

	stmt.KV = p.c.Get("KV").(string)
	stmt.NS = p.c.Get("NS").(string)
	stmt.DB = p.c.Get("DB").(string)

	_, stmt.Name, err = p.shouldBe(IDENT)
	if err != nil {
		return nil, err
	}

	_, _, err = p.shouldBe(EQ)
	if err != nil {
		return nil, err
	}

	tok, lit, err := p.shouldBe(NULL, NOW, DATE, TIME, TRUE, FALSE, STRING, NUMBER, DOUBLE, THING, JSON, ARRAY, PARAM)
	if err != nil {
		return nil, err
	}

	stmt.Expr, err = p.declare(tok, lit)
	if err != nil {
		return nil, err
	}

	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return

}
