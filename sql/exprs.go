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

import (
	"fmt"
	"regexp"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/abcum/surreal/util/rand"
)

func (p *parser) parseWhat() (mul []Expr, err error) {

	for {

		tok, lit, err := p.shouldBe(IDENT, NUMBER, DOUBLE, THING, PARAM)
		if err != nil {
			return nil, &ParseError{Found: lit, Expected: []string{"table name or record id"}}
		}

		if p.is(tok, IDENT, NUMBER, DOUBLE) {
			one, _ := p.declare(TABLE, lit)
			mul = append(mul, one)
		}

		if p.is(tok, THING) {
			one, _ := p.declare(THING, lit)
			mul = append(mul, one)
		}

		if p.is(tok, PARAM) {
			one, _ := p.declare(PARAM, lit)
			mul = append(mul, one)
		}

		// Check to see if the next token is a comma
		// and if not, then break out of the loop,
		// otherwise repeat until we find no comma.

		if _, _, exi := p.mightBe(COMMA); !exi {
			break
		}

	}

	return

}

func (p *parser) parseName() (string, error) {

	_, lit, err := p.shouldBe(IDENT, NUMBER, DOUBLE)
	if err != nil {
		return string(""), &ParseError{Found: lit, Expected: []string{"name"}}
	}

	val, err := p.declare(STRING, lit)

	return val.(string), err

}

func (p *parser) parseNames() (mul []string, err error) {

	for {

		one, err := p.parseName()
		if err != nil {
			return nil, err
		}

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

// --------------------------------------------------
//
// --------------------------------------------------

func (p *parser) parseCond() (exp Expr, err error) {

	// The next token that we expect to see is a
	// WHERE token, and if we don't find one then
	// return nil, with no error.

	if _, _, exi := p.mightBe(WHERE); !exi {
		return nil, nil
	}

	return p.parseExpr()

}

// --------------------------------------------------
//
// --------------------------------------------------

func (p *parser) parseRand() (exp []byte, err error) {

	exp = rand.New(128)

	return

}

// --------------------------------------------------
//
// --------------------------------------------------

func (p *parser) parseIdent() (*Ident, error) {

	_, lit, err := p.shouldBe(IDENT)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"name"}}
	}

	val, err := p.declare(IDENT, lit)

	return val.(*Ident), err

}

func (p *parser) parseTable() (*Table, error) {

	_, lit, err := p.shouldBe(IDENT, NUMBER, DOUBLE)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"name"}}
	}

	val, err := p.declare(TABLE, lit)

	return val.(*Table), err

}

func (p *parser) parseThing() (*Thing, error) {

	_, lit, err := p.shouldBe(THING)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"record id"}}
	}

	val, err := p.declare(THING, lit)

	return val.(*Thing), err

}

func (p *parser) parseArray() ([]interface{}, error) {

	_, lit, err := p.shouldBe(ARRAY)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"array"}}
	}

	val, err := p.declare(ARRAY, lit)

	return val.([]interface{}), err

}

func (p *parser) parseObject() (exp map[string]interface{}, err error) {

	_, lit, err := p.shouldBe(JSON)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"object"}}
	}

	val, err := p.declare(JSON, lit)

	return val.(map[string]interface{}), err

}

func (p *parser) parseNumber() (int64, error) {

	_, lit, err := p.shouldBe(NUMBER)
	if err != nil {
		return int64(0), &ParseError{Found: lit, Expected: []string{"number"}}
	}

	val, err := p.declare(NUMBER, lit)

	return val.(int64), err

}

func (p *parser) parseDouble() (float64, error) {

	_, lit, err := p.shouldBe(NUMBER, DOUBLE)
	if err != nil {
		return float64(0), &ParseError{Found: lit, Expected: []string{"number"}}
	}

	val, err := p.declare(DOUBLE, lit)

	return val.(float64), err

}

func (p *parser) parseString() (string, error) {

	_, lit, err := p.shouldBe(STRING)
	if err != nil {
		return string(""), &ParseError{Found: lit, Expected: []string{"string"}}
	}

	val, err := p.declare(STRING, lit)

	return val.(string), err

}

func (p *parser) parseRegion() (string, error) {

	tok, lit, err := p.shouldBe(IDENT, STRING, REGION)
	if err != nil {
		return string(""), &ParseError{Found: lit, Expected: []string{"string"}}
	}

	val, err := p.declare(tok, lit)

	return val.(string), err

}

func (p *parser) parseScript() (string, error) {

	tok, lit, err := p.shouldBe(STRING, REGION)
	if err != nil {
		return string(""), &ParseError{Found: lit, Expected: []string{"js/lua script"}}
	}

	val, err := p.declare(tok, lit)

	return val.(string), err

}

func (p *parser) parseRegexp() (string, error) {

	tok, lit, err := p.shouldBe(REGEX)
	if err != nil {
		return string(""), &ParseError{Found: lit, Expected: []string{"regular expression"}}
	}

	val, err := p.declare(tok, lit)

	return val.(*regexp.Regexp).String(), err

}

func (p *parser) parseBoolean() (bool, error) {

	tok, lit, err := p.shouldBe(TRUE, FALSE)
	if err != nil {
		return bool(false), &ParseError{Found: lit, Expected: []string{"boolean"}}
	}

	val, err := p.declare(tok, lit)

	return val.(bool), err

}

func (p *parser) parseDuration() (time.Duration, error) {

	tok, lit, err := p.shouldBe(DURATION)
	if err != nil {
		return 0, &ParseError{Found: lit, Expected: []string{"duration"}}
	}

	val, err := p.declare(tok, lit)

	return val.(time.Duration), err

}

func (p *parser) parseBcrypt() ([]byte, error) {

	_, lit, err := p.shouldBe(STRING)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"string"}}
	}

	val, err := p.declare(STRING, lit)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"string"}}
	}

	return bcrypt.GenerateFromPassword([]byte(val.(string)), bcrypt.DefaultCost)

}

func (p *parser) parseExpr() (exp Expr, err error) {

	// Create the root binary expression tree.

	root := &BinaryExpression{}

	// If the subsequent token is an in, out, or
	// multi way path expression, then parse all
	// following expressions as a path.

	if tok, _, exi := p.mightBe(OEDGE, IEDGE, BEDGE); exi {
		return p.parsePath(tok)
	}

	// Begin with parsing the first expression
	// as the root of the tree to start with.

	root.RHS, err = p.parsePart()
	if err != nil {
		return nil, err
	}

	// If the subsequent token is an in, out, or
	// multi way path expression, then parse all
	// following expressions as a path.

	if tok, _, exi := p.mightBe(OEDGE, IEDGE, BEDGE); exi {
		return p.parsePath(root.RHS, tok)
	}

	// Loop over the operations and expressions
	// and build a binary expression tree based
	// on the precedence of the operators.

	for {

		var rhs Expr

		// Get the next token from the scanner and
		// the literal value that it is scanned as.

		tok, lit, _ := p.scan()

		switch tok {

		// If the token is an AND or OR expression
		// then skip to the next expression without
		// further checks.

		case AND, OR:

		// If the token is not an operator but can
		// be converted into an operator based on
		// logic, then convert it to an operator.

		case IN:

			tok = INS
			if _, _, exi := p.mightBe(NOT); exi {
				tok = NIS
			}

		case CONTAINS:

			tok = SIN
			if _, _, exi := p.mightBe(NOT); exi {
				tok = SNI
			}

		case IS:

			tok = EQ
			if _, _, exi := p.mightBe(NOT); exi {
				tok = NEQ
			}
			if _, _, exi := p.mightBe(IN); exi {
				switch tok {
				case EQ:
					tok = INS
				case NEQ:
					tok = NIS
				}
			}

		// If the token is a keyword which is also
		// actually an operator, then skip to the
		// next expression without further checks.

		case CONTAINSALL, CONTAINSNONE, CONTAINSSOME:

		case ALLCONTAINEDIN, NONECONTAINEDIN, SOMECONTAINEDIN:

		// If the token is an int64 or a float64 then
		// check to see whether the first rune is a
		// + or a - and use it as a token instead.

		case NUMBER, DOUBLE:

			switch lit[0] {
			case '-':
				rhs, err = p.declare(tok, lit[1:])
				tok = SUB
			case '+':
				rhs, err = p.declare(tok, lit[1:])
				tok = ADD
			default:
				p.unscan()
				return root.RHS, nil
			}

		// Check to see if the token is an operator
		// expression. If it is none of those then
		// unscan and break out of the loop.

		default:

			if !tok.isOperator() {
				p.unscan()
				return root.RHS, nil
			}

		}

		// If the token was not an int64 or float64
		// signed value then retrieve the next part
		// of the expression and add it to the right.

		if rhs == nil {
			rhs, err = p.parsePart()
			if err != nil {
				return nil, err
			}
		}

		// Find the right place in the tree to add the
		// new expression, by descending the right side
		// of the tree until we reach the last binary
		// expression, or until we reach an expression
		// whose operator precendence >= this precedence.

		for node := root; ; {
			r, ok := node.RHS.(*BinaryExpression)
			if !ok || r.Op.precedence() >= tok.precedence() {
				node.RHS = &BinaryExpression{LHS: node.RHS, Op: tok, RHS: rhs}
				break
			}
			node = r
		}

	}

	return root, err

}

func (p *parser) parsePart() (exp Expr, err error) {

	toks := []Token{
		MUL, ID, IDENT, THING,
		NULL, VOID, EMPTY, MISSING,
		TRUE, FALSE, STRING, REGION, NUMBER, DOUBLE,
		NOW, DATE, TIME, DURATION, JSON, ARRAY, PARAM, LPAREN,
	}

	tok, lit, _ := p.scan()

	// We need to declare the type up here instead
	// of at the bottom, as the held value might
	// be overwritten by the next token scan.

	exp, err = p.declare(tok, lit)
	if err != nil {
		return nil, err
	}

	// If the current token is a left parenthesis
	// bracket, then we will parse this complete
	// expression part as a subquery.

	if p.is(tok, LPAREN) {
		return p.parseSubq()
	}

	// If the next token is a left parenthesis
	// bracket, then we will parse this complete
	// expression part as a function call.

	if _, _, exi := p.mightBe(LPAREN); exi {
		return p.parseCall(lit)
	}

	// If this expression is not a subquery or a
	// function call, then check to see if the
	// token is in the list of allowed tokens.

	if !p.in(tok, toks) {
		err = &ParseError{Found: lit, Expected: []string{"expression"}}
	}

	return

}

func (p *parser) parseSubq() (sub *SubExpression, err error) {

	var exp Expr
	var tok Token

	tok, _, _ = p.mightBe(SELECT, CREATE, UPDATE, DELETE, RELATE)

	switch tok {
	default:
		exp, err = p.parseExpr()
	case SELECT:
		exp, err = p.parseSelectStatement()
	case CREATE:
		exp, err = p.parseCreateStatement()
	case UPDATE:
		exp, err = p.parseUpdateStatement()
	case DELETE:
		exp, err = p.parseDeleteStatement()
	case RELATE:
		exp, err = p.parseRelateStatement()
	}

	p.mightBe(RPAREN)

	return &SubExpression{Expr: exp}, err

}

func (p *parser) parseCall(name string) (fnc *FuncExpression, err error) {

	fnc = &FuncExpression{Name: name}

	// Check to see if the immediate token
	// is a right parenthesis bracket, and if
	// it is then this function has no args.

	if _, _, exi := p.mightBe(RPAREN); !exi {

		for {

			var arg Expr

			arg, err = p.parseExpr()
			if err != nil {
				return nil, err
			}

			// Append the single expression to the array
			// of function argument expressions.

			fnc.Args = append(fnc.Args, arg)

			// Check to see if the next token is a comma
			// and if not, then break out of the loop,
			// otherwise repeat until we find no comma.

			if _, _, exi := p.mightBe(COMMA); !exi {
				break
			}

		}

		_, _, err = p.shouldBe(RPAREN)

	}

	// Check to see if the used function name is
	// valid according to the currently supported
	// functions. If not then return an error.

	if _, ok := funcs[fnc.Name]; !ok {

		return nil, &ParseError{
			Found:    fmt.Sprintf("%s()", name),
			Expected: []string{"valid function name"},
		}

	}

	// Check to see if the number of arguments
	// is correct for the specified function name,
	// and if not, then return an error.

	if _, ok := funcs[fnc.Name][len(fnc.Args)]; !ok {

		s, t := "", len(funcs[fnc.Name])

		for i := 0; i < t; i++ {
			switch {
			case i > 0 && i == t-1:
				s = s + " or "
			case i > 0:
				s = s + ", "
			}
			s = s + fmt.Sprintf("%d", i)
		}

		switch {
		case t == 1:
			s = s + " argument"
		case t >= 2:
			s = s + " arguments"
		}

		return nil, &ParseError{
			Found:    fmt.Sprintf("%s() with %d arguments", fnc.Name, len(fnc.Args)),
			Expected: []string{s},
		}

	}

	return

}

func (p *parser) parsePath(expr ...Expr) (path *PathExpression, err error) {

	path = &PathExpression{}

	// Take the previosuly scanned expression
	// and append it to the path expression
	// tree as the first item.

	for _, e := range expr {
		switch v := e.(type) {
		case Token:
			path.Expr = append(path.Expr, &JoinExpression{Join: v})
		default:
			path.Expr = append(path.Expr, &PartExpression{Part: v})
		}
	}

	// If the last expression passed in was a
	// path joiner (->, <-, or <->), then we
	// need to process a path part first.

	if _, ok := expr[len(expr)-1].(Token); ok {

		var part Expr

		part, err = p.parseStep()
		if err != nil {
			return nil, err
		}

		if part == nil {
			return
		}

		path.Expr = append(path.Expr, &PartExpression{Part: part})

	}

	for {

		var join Expr
		var part Expr

		// We expect the next token to be a join
		// operator (->, <-, or <->), otherwise we
		// are at the end of the path and will
		// ignore it and return.

		join, err = p.parseJoin()
		if err != nil {
			return nil, err
		}

		if join == nil {
			return
		}

		path.Expr = append(path.Expr, &JoinExpression{Join: join.(Token)})

		// We expect the next token to be a path
		// part identifier, otherwise we are at
		// the end of the path and will ignore it
		// and return.

		part, err = p.parseStep()
		if err != nil {
			return nil, err
		}

		if part == nil {
			return
		}

		path.Expr = append(path.Expr, &PartExpression{Part: part})

	}

	return

}

func (p *parser) parseJoin() (exp Expr, err error) {

	toks := []Token{
		OEDGE, IEDGE, BEDGE,
	}

	tok, _, _ := p.scan()

	if !p.in(tok, toks) {
		p.unscan()
		return
	}

	return tok, err

}

func (p *parser) parseStep() (exp Expr, err error) {

	toks := []Token{
		QMARK, IDENT, THING, PARAM, LPAREN,
	}

	tok, lit, _ := p.scan()

	// We need to declare the type up here instead
	// of at the bottom, as the held value might
	// be overwritten by the next token scan.

	exp, err = p.declare(tok, lit)
	if err != nil {
		return nil, err
	}

	// If the current token is a left parenthesis
	// bracket, then we will parse this complete
	// expression part as a subquery.

	if p.is(tok, LPAREN) {
		return p.parseSubp()
	}

	// If this expression is not a sub-path
	// expression, then check to see if the
	// token is in the list of allowed tokens.

	if !p.in(tok, toks) {
		p.unscan()
		exp = nil
	}

	return

}

func (p *parser) parseSubp() (stmt *SubpExpression, err error) {

	stmt = &SubpExpression{}

	if stmt.What, err = p.parseWhat(); err != nil {
		return nil, err
	}

	if _, _, exi := p.mightBe(AS); exi {
		if stmt.Name, err = p.parseName(); err != nil {
			return nil, err
		}
	}

	if stmt.Cond, err = p.parseCond(); err != nil {
		return nil, err
	}

	if _, _, err = p.shouldBe(RPAREN); err != nil {
		return nil, err
	}

	return

}
