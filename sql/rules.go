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

func (p *parser) parseDefineRulesStatement() (stmt *DefineRulesStatement, err error) {

	stmt = &DefineRulesStatement{}

	stmt.KV = p.c.Get("KV").(string)
	stmt.NS = p.c.Get("NS").(string)
	stmt.DB = p.c.Get("DB").(string)

	if _, _, err = p.shouldBe(ON); err != nil {
		return nil, err
	}

	if stmt.What, err = p.parseNames(); err != nil {
		return nil, err
	}

	if _, _, err = p.shouldBe(FOR); err != nil {
		return nil, err
	}

	for {

		tok, _, exi := p.mightBe(SELECT, CREATE, UPDATE, DELETE, RELATE)
		if !exi {
			break
		}

		stmt.When = append(stmt.When, tok.String())

		if _, _, exi := p.mightBe(COMMA); !exi {
			break
		}

	}

	if len(stmt.When) == 0 {
		return nil, &ParseError{Found: "", Expected: []string{"SELECT", "CREATE", "UPDATE", "DELETE", "RELATE"}}
	}

	if tok, _, err := p.shouldBe(ACCEPT, REJECT, CUSTOM); err != nil {
		return nil, err
	} else {

		stmt.Rule = tok.String()

		if p.is(tok, CUSTOM) {
			if stmt.Code, err = p.parseScript(); err != nil {
				return nil, err
			}
		}

	}

	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return

}

func (p *parser) parseRemoveRulesStatement() (stmt *RemoveRulesStatement, err error) {

	stmt = &RemoveRulesStatement{}

	stmt.KV = p.c.Get("KV").(string)
	stmt.NS = p.c.Get("NS").(string)
	stmt.DB = p.c.Get("DB").(string)

	if _, _, err = p.shouldBe(ON); err != nil {
		return nil, err
	}

	if stmt.What, err = p.parseNames(); err != nil {
		return nil, err
	}

	if _, _, err = p.shouldBe(FOR); err != nil {
		return nil, err
	}

	for {

		tok, _, exi := p.mightBe(SELECT, CREATE, UPDATE, DELETE, RELATE)
		if !exi {
			break
		}

		stmt.When = append(stmt.When, tok.String())

		if _, _, exi := p.mightBe(COMMA); !exi {
			break
		}

	}

	if len(stmt.When) == 0 {
		return nil, &ParseError{Found: "", Expected: []string{"SELECT", "CREATE", "UPDATE", "DELETE", "RELATE"}}
	}

	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return

}
