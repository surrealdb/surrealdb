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

func (p *parser) parseSelectStatement() (stmt *SelectStatement, err error) {

	stmt = &SelectStatement{}

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

	if stmt.Echo, err = p.parseEcho(); err != nil {
		return nil, err
	}

	if _, _, err = p.shouldBe(EOF, RPAREN, SEMICOLON); err != nil {
		return nil, err
	}

	return stmt, nil

}

func (p *parser) parseField() (mul []*Field, err error) {

	var lit string
	var exi bool

	for {

		one := &Field{}

		one.Expr, err = p.parseExpr()
		if err != nil {
			return
		}

		one.Alias = "*" // TODO need to implement default field name

		// Chec to see if the next token is an AS
		// clause, and if it is read the defined
		// field alias name from the scanner.

		if _, _, exi = p.mightBe(AS); exi {

			if _, one.Alias, err = p.shouldBe(IDENT); err != nil {
				return nil, &ParseError{Found: lit, Expected: []string{"field alias"}}
			}

		}

		// Append the single expression to the array
		// of return statement expressions.

		mul = append(mul, one)

		// Check to see if the next token is a comma
		// and if not, then break out of the loop,
		// otherwise repeat until we find no comma.

		if _, _, exi = p.mightBe(COMMA); !exi {
			break
		}

	}

	return

}

func (p *parser) parseWhere() (exp Expr, err error) {

	// The next token that we expect to see is a
	// WHERE token, and if we don't find one then
	// return nil, with no error.

	if _, _, exi := p.mightBe(WHERE); !exi {
		return nil, nil
	}

	return p.parseExpr()

}

func (p *parser) parseGroup() (mul []*Group, err error) {

	// The next token that we expect to see is a
	// GROUP token, and if we don't find one then
	// return nil, with no error.

	if _, _, exi := p.mightBe(GROUP); !exi {
		return nil, nil
	}

	// We don't need to have a BY token, but we
	// allow it so that the SQL query would read
	// better when compared to english.

	_, _, _ = p.mightBe(BY)

	for {

		var tok Token
		var lit string

		one := &Group{}

		tok, lit, err = p.shouldBe(IDENT, ID)
		if err != nil {
			return nil, &ParseError{Found: lit, Expected: []string{"field name"}}
		}

		one.Expr, err = p.declare(tok, lit)
		if err != nil {
			return nil, err
		}

		// Append the single expression to the array
		// of return statement expressions.

		mul = append(mul, one)

		// Check to see if the next token is a comma
		// and if not, then break out of the loop,
		// otherwise repeat until we find no comma.

		if _, _, exi := p.mightBe(COMMA); !exi {
			break
		}

	}

	return

}

func (p *parser) parseOrder() (mul []*Order, err error) {

	// The next token that we expect to see is a
	// ORDER token, and if we don't find one then
	// return nil, with no error.

	if _, _, exi := p.mightBe(ORDER); !exi {
		return nil, nil
	}

	// We don't need to have a BY token, but we
	// allow it so that the SQL query would read
	// better when compared to english.

	_, _, _ = p.mightBe(BY)

	for {

		var exi bool
		var tok Token
		var lit string

		one := &Order{}

		tok, lit, err = p.shouldBe(IDENT, ID)
		if err != nil {
			return nil, &ParseError{Found: lit, Expected: []string{"field name"}}
		}

		one.Expr, err = p.declare(tok, lit)
		if err != nil {
			return nil, err
		}

		if tok, lit, exi = p.mightBe(ASC, DESC); !exi {
			tok = ASC
		}

		one.Dir, err = p.declare(tok, lit)
		if err != nil {
			return nil, err
		}

		// Append the single expression to the array
		// of return statement expressions.

		mul = append(mul, one)

		// Check to see if the next token is a comma
		// and if not, then break out of the loop,
		// otherwise repeat until we find no comma.

		if _, _, exi := p.mightBe(COMMA); !exi {
			break
		}

	}

	return

}

func (p *parser) parseLimit() (Expr, error) {

	// The next token that we expect to see is a
	// LIMIT token, and if we don't find one then
	// return nil, with no error.

	if _, _, exi := p.mightBe(LIMIT); !exi {
		return nil, nil
	}

	// We don't need to have a BY token, but we
	// allow it so that the SQL query would read
	// better when compared to english.

	_, _, _ = p.mightBe(BY)

	tok, lit, err := p.shouldBe(NUMBER)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"limit number"}}
	}

	return p.declare(tok, lit)

}

func (p *parser) parseStart() (Expr, error) {

	// The next token that we expect to see is a
	// START token, and if we don't find one then
	// return nil, with no error.

	if _, _, exi := p.mightBe(START); !exi {
		return nil, nil
	}

	// We don't need to have a AT token, but we
	// allow it so that the SQL query would read
	// better when compared to english.

	_, _, _ = p.mightBe(AT)

	tok, lit, err := p.shouldBe(NUMBER)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"start number"}}
	}

	return p.declare(tok, lit)

}

func (p *parser) parseVersion() (Expr, error) {

	if _, _, exi := p.mightBe(VERSION, ON); !exi {
		return nil, nil
	}

	tok, lit, err := p.shouldBe(DATE, TIME)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"timestamp"}}
	}

	return p.declare(tok, lit)

}
