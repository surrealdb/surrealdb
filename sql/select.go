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

	if stmt.KV, stmt.NS, stmt.DB, err = p.a.get(AuthNO); err != nil {
		return nil, err
	}

	if stmt.Expr, err = p.parseFields(); err != nil {
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

	if stmt.Split, err = p.parseSplit(); err != nil {
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

	if stmt.Fetch, err = p.parseFetch(); err != nil {
		return nil, err
	}

	if stmt.Version, err = p.parseVersion(); err != nil {
		return nil, err
	}

	if stmt.Timeout, err = p.parseTimeout(); err != nil {
		return nil, err
	}

	if stmt.Parallel, err = p.parseParallel(); err != nil {
		return nil, err
	}

	if err = checkExpression(aggrs, stmt.Expr, stmt.Group); err != nil {
		return nil, err
	}

	// If this query has any subqueries which
	// need to alter the database then mark
	// this query as a writeable statement.

	stmt.RW = p.buf.rw

	return

}

func (p *parser) parseFields() (mul Fields, err error) {

	for {

		one := &Field{}

		one.Expr, err = p.parseExpr()
		if err != nil {
			return
		}

		// Chec to see if the next token is an AS
		// clause, and if it is read the defined
		// field alias name from the scanner.

		if _, _, exi := p.mightBe(AS); exi {

			if _, one.Alias, err = p.shouldBe(IDENT, EXPR); err != nil {
				return nil, &ParseError{Found: one.Alias, Expected: []string{"alias name"}}
			}

			one.Field = one.Alias

		} else {

			switch v := one.Expr.(type) {
			case *Param:
				one.Field = v.VA
			case *Ident:
				one.Field = v.VA
			case *Value:
				one.Field = v.VA
			default:
				one.Field = one.String()
			}

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

func (p *parser) parseSplit() (Idents, error) {

	// The next token that we expect to see is a
	// SPLIT token, and if we don't find one then
	// return nil, with no error.

	if _, _, exi := p.mightBe(SPLIT); !exi {
		return nil, nil
	}

	// We don't need to have a ON token, but we
	// allow it so that the SQL query would read
	// better when compared to english.

	_, _, _ = p.mightBe(ON)

	return p.parseIdioms()

}

func (p *parser) parseGroup() (mul Groups, err error) {

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

	// If the next token is the ALL keyword then
	// we will group all records together, which
	// will prevent grouping by other fields.

	if _, _, exi := p.mightBe(ALL); exi {

		mul = append(mul, &Group{
			Expr: new(All),
		})

		return

	}

	// Otherwise let's parse the fields with which
	// we will group the selected data, with
	// multiple fields grouped by commas.

	for {

		var tok Token
		var lit string

		one := &Group{}

		tok, lit, err = p.shouldBe(IDENT, EXPR)
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

func (p *parser) parseOrder() (mul Orders, err error) {

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

		tok, lit, err = p.shouldBe(IDENT, EXPR, RAND)
		if err != nil {
			return nil, &ParseError{Found: lit, Expected: []string{"field name"}}
		}

		switch tok {
		default:
			one.Expr, err = p.declare(tok, lit)
			if err != nil {
				return nil, err
			}
		case RAND:
			one.Expr = &FuncExpression{Name: "rand"}
			if _, _, exi = p.mightBe(LPAREN); exi {
				_, _, err = p.shouldBe(RPAREN)
				if err != nil {
					return nil, err
				}
			}
		}

		if _, _, exi = p.mightBe(COLLATE); exi {
			one.Tag, err = p.parseLanguage()
			if err != nil {
				return nil, err
			}
		}

		if tok, _, exi = p.mightBe(ASC, DESC); exi {
			one.Dir = (tok == ASC)
		} else {
			one.Dir = true
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

	tok, lit, err := p.shouldBe(NUMBER, PARAM)
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

	tok, lit, err := p.shouldBe(NUMBER, PARAM)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"start number"}}
	}

	return p.declare(tok, lit)

}

func (p *parser) parseFetch() (mul Fetchs, err error) {

	// The next token that we expect to see is a
	// GROUP token, and if we don't find one then
	// return nil, with no error.

	if _, _, exi := p.mightBe(FETCH); !exi {
		return nil, nil
	}

	for {

		var tok Token
		var lit string

		one := &Fetch{}

		tok, lit, err = p.shouldBe(IDENT, EXPR)
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

func (p *parser) parseVersion() (Expr, error) {

	if _, _, exi := p.mightBe(VERSION, ON); !exi {
		return nil, nil
	}

	return p.parseExpr()

}
