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

func (p *Parser) parseSelectStatement(explain bool) (stmt *SelectStatement, err error) {

	stmt = &SelectStatement{}

	stmt.EX = explain

	stmt.KV = p.c.Get("KV").(string)
	stmt.NS = p.c.Get("NS").(string)
	stmt.DB = p.c.Get("DB").(string)

	if stmt.Expr, err = p.parseExpr(); err != nil {
		return nil, err
	}

	_, _, err = p.shouldBe(FROM)
	if err != nil {
		return nil, err
	}

	if stmt.What, err = p.parseWhat(); err != nil {
		return nil, err
	}

	if stmt.Cond, err = p.parseCond(); err != nil {
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

	if _, _, err = p.shouldBe(EOF, SEMICOLON); err != nil {
		return nil, err
	}

	return stmt, nil

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

func (p *Parser) parseOrder() (mul []*Order, err error) {

	var tok Token
	var lit string
	var exi bool

	// Remove the ORDER keyword
	if _, _, exi := p.mightBe(ORDER); !exi {
		return nil, nil
	}

	// Next token might be BY
	_, _, _ = p.mightBe(BY)

	for {

		one := &Order{}

		tok, lit, err = p.shouldBe(IDENT, ID)
		if err != nil {
			return nil, &ParseError{Found: lit, Expected: []string{"field name"}}
		}

		one.Expr, err = declare(tok, lit)
		if err != nil {
			return nil, err
		}

		tok, lit, exi = p.mightBe(ASC, DESC)
		if !exi {
			tok = ASC
			lit = "ASC"
		}

		one.Dir, err = declare(tok, lit)
		if err != nil {
			return nil, err
		}

		mul = append(mul, one)

		// If the next token is not a comma then break the loop.
		if _, _, exi := p.mightBe(COMMA); !exi {
			break
		}

	}

	return mul, nil

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

	// Remove the START keyword
	if _, _, exi := p.mightBe(START); !exi {
		return nil, nil
	}

	// Next token might be AT
	_, _, _ = p.mightBe(AT)

	// Next token might be @
	_, _, exi := p.mightBe(EAT)

	if exi == false {
		val, err := p.parseNumber()
		if err != nil {
			return nil, err
		}
		return val, nil
	}

	if exi == true {
		p.unscan()
		val, err := p.parseThing()
		if err != nil {
			return nil, err
		}
		return val, nil
	}

	return nil, nil

}

func (p *Parser) parseVersion() (Expr, error) {

	// Remove the VERSION keyword
	if _, _, exi := p.mightBe(VERSION); !exi {
		return nil, nil
	}

	tok, lit, err := p.shouldBe(DATE, TIME)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"timestamp"}}
	}

	return declare(tok, lit)

}
