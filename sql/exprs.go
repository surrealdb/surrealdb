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

func (p *Parser) parseTable() (*Table, error) {

	_, lit, err := p.shouldBe(IDENT)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"table name"}}
	}

	return &Table{Name: lit}, nil

}

func (p *Parser) parseIdent() (*IdentLiteral, error) {

	_, lit, err := p.shouldBe(IDENT)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"name"}}
	}

	return &IdentLiteral{Val: lit}, nil

}

func (p *Parser) parseString() (*StringLiteral, error) {

	_, lit, err := p.shouldBe(STRING)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"string"}}
	}

	return &StringLiteral{Val: lit}, nil

}

func (p *Parser) parseRegion() (*StringLiteral, error) {

	_, lit, err := p.shouldBe(IDENT, STRING, REGION)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"string"}}
	}

	return &StringLiteral{Val: lit}, nil

}

func (p *Parser) parseFields() ([]*Field, error) {

	var m []*Field

	for {

		s, err := p.parseField()

		if err != nil {
			return nil, err
		}

		m = append(m, s)

		// If the next token is not a comma then break the loop.
		if _, _, exi := p.mightBe(COMMA); !exi {
			break
		}

	}

	return m, nil

}

func (p *Parser) parseField() (*Field, error) {

	f := &Field{}

	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	f.Expr = expr

	switch expr.(type) {
	case *Wildcard:
		return f, nil
	}

	alias, err := p.parseAlias()
	if err != nil {
		return nil, err
	}
	f.Alias = alias

	return f, nil

}

func (p *Parser) parseExpr() (ex Expr, er error) {

	tok, lit, err := p.shouldBe(IDENT, NULL, ALL, TIME, TRUE, FALSE, STRING, REGION, NUMBER, DOUBLE, JSON)
	if err != nil {
		er = &ParseError{Found: lit, Expected: []string{"field name"}}
	}

	ex = declare(tok, lit)

	return

}

func (p *Parser) parseAlias() (ex Expr, er error) {

	if _, _, exi := p.mightBe(AS); !exi {
		return nil, nil
	}

	tok, lit, err := p.shouldBe(IDENT)
	if err != nil {
		er = &ParseError{Found: lit, Expected: []string{"field name"}}
	}

	ex = declare(tok, lit)

	return

}
