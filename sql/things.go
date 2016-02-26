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

func (p *Parser) parseThings() ([]Expr, error) {

	var ts []Expr

	for {

		t, err := p.parseThing()

		if err != nil {
			return nil, err
		}

		ts = append(ts, t)

		// If the next token is not a comma then break the loop.
		if tok, _ := p.scanIgnoreWhitespace(); tok != COMMA {
			p.unscan()
			break
		}

	}

	return ts, nil

}

func (p *Parser) parseThing() (Expr, error) {

	var (
		err error
		tok Token
		lit string
	)

	_, _, exi := p.mightBe(EAT)

	if exi == false {

		t := &Table{}

		// Parse table name
		tok, lit = p.scan()
		if !is(tok, IDENT, NUMBER) {
			p.unscan()
			return nil, &ParseError{Found: lit, Expected: []string{"table name"}}
		}
		t.Name = lit

		return t, nil

	}

	if exi == true {

		t := &Thing{}

		// Parse table name
		tok, lit = p.scan()
		if !is(tok, IDENT, NUMBER) {
			p.unscan()
			return nil, &ParseError{Found: lit, Expected: []string{"table name"}}
		}
		t.Table = lit

		// Next token should be :
		_, _, err = p.shouldBe(COLON)
		if err != nil {
			return nil, err
		}

		// Parse table id
		tok, lit = p.scan()
		if !is(tok, IDENT, NUMBER) {
			p.unscan()
			return nil, &ParseError{Found: lit, Expected: []string{"table id"}}
		}
		t.ID = lit

		return t, nil

	}

	return nil, nil

}
