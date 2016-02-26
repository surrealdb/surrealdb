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

func (p *Parser) parseSelectStatement() (*SelectStatement, error) {

	stmt := &SelectStatement{}

	var err error

	if stmt.Fields, err = p.parseFields(); err != nil {
		return nil, err
	}

	// Next token should be FROM
	_, _, err = p.shouldBe(FROM)
	if err != nil {
		return nil, err
	}

	if stmt.Thing, err = p.parseThings(); err != nil {
		return nil, err
	}

	if stmt.Where, err = p.parseWhere(); err != nil {
		return nil, err
	}

	if stmt.Group, err = p.parseGroup(); err != nil {
		return nil, err
	}

	if stmt.Order, err = p.parseOrder(); err != nil {
		return nil, err
	}

	if stmt.Limit, err = p.parseLimit(); err != nil {
		return nil, err
	}

	if stmt.Start, err = p.parseStart(); err != nil {
		return nil, err
	}

	if stmt.Version, err = p.parseVersion(); err != nil {
		return nil, err
	}

	// Next token should be EOF
	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return stmt, nil

}

func (p *Parser) parseWhere() (Expr, error) {

	var ws []Expr

	var tok Token
	var lit string
	var err error

	// Remove the WHERE keyword
	if _, _, exi := p.mightBe(WHERE); !exi {
		return nil, nil
	}

	for {

		w := &BinaryExpression{}

		tok, lit, err = p.shouldBe(IDENT, TIME, TRUE, FALSE, STRING, NUMBER, DOUBLE)
		if err != nil {
			return nil, &ParseError{Found: lit, Expected: []string{"field name"}}
		}
		w.LHS = declare(tok, lit)

		tok, lit, err = p.shouldBe(IN, EQ, NEQ, GT, LT, GTE, LTE, EQR, NER)
		if err != nil {
			return nil, err
		}
		w.Op = lit

		tok, lit, err = p.shouldBe(IDENT, NULL, TIME, TRUE, FALSE, STRING, NUMBER, DOUBLE, REGEX, JSON)
		if err != nil {
			return nil, &ParseError{Found: lit, Expected: []string{"field value"}}
		}
		w.RHS = declare(tok, lit)

		ws = append(ws, w)

		// Remove the WHERE keyword
		if _, _, exi := p.mightBe(AND, OR); !exi {
			break
		}

	}

	return ws, nil

}

func (p *Parser) parseGroup() ([]*Group, error) {

	var gs []*Group

	// Remove the GROUP keyword
	if _, _, exi := p.mightBe(GROUP); !exi {
		return nil, nil
	}

	// Next token might be BY
	_, _, _ = p.mightBe(BY)

	return gs, nil

}

func (p *Parser) parseOrder() ([]*Order, error) {

	var m []*Order

	var tok Token
	var lit string
	var err error
	var exi bool

	// Remove the ORDER keyword
	if _, _, exi := p.mightBe(ORDER); !exi {
		return nil, nil
	}

	// Next token might be BY
	_, _, _ = p.mightBe(BY)

	for {

		s := &Order{}

		tok, lit, err = p.shouldBe(IDENT)
		if err != nil {
			return nil, &ParseError{Found: lit, Expected: []string{"field name"}}
		}
		s.Expr = declare(tok, lit)

		tok, lit, exi = p.mightBe(ASC, DESC)
		if !exi {
			tok = ASC
			lit = "ASC"
		}
		s.Dir = declare(tok, lit)

		m = append(m, s)

		// If the next token is not a comma then break the loop.
		if _, _, exi := p.mightBe(COMMA); !exi {
			break
		}

	}

	return m, nil

}

func (p *Parser) parseLimit() (Expr, error) {

	// Remove the LIMIT keyword
	if _, _, exi := p.mightBe(LIMIT); !exi {
		return nil, nil
	}

	// Next token might be BY
	_, _, _ = p.mightBe(BY)

	_, lit, err := p.shouldBe(NUMBER)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"limit number"}}
	}

	return &NumberLiteral{Val: number(lit)}, nil

}

func (p *Parser) parseStart() (Expr, error) {

	var tok Token
	var lit string
	var err error

	// Remove the START keyword
	if _, _, exi := p.mightBe(START); !exi {
		return nil, nil
	}

	// Next token might be AT
	_, _, _ = p.mightBe(AT)

	// Next token might be @
	_, _, exi := p.mightBe(EAT)

	if exi == false {

		// Parse table name
		tok, lit = p.scan()
		if !is(tok, NUMBER) {
			p.unscan()
			return nil, &ParseError{Found: lit, Expected: []string{"table name"}}
		}

		return &NumberLiteral{Val: number(lit)}, nil

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

func (p *Parser) parseVersion() (Expr, error) {

	// Remove the VERSION keyword
	if _, _, exi := p.mightBe(VERSION); !exi {
		return nil, nil
	}

	tok, lit, err := p.shouldBe(DATE, TIME, NANO)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"timestamp"}}
	}

	return declare(tok, lit), nil

}
