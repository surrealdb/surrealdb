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
	"sort"
	"time"

	"golang.org/x/text/language"
)

func (p *parser) parseWhat() (mul []Expr, err error) {

	for {

		exp, err := p.parsePart()
		if err != nil {
			return nil, err
		}

		mul = append(mul, exp)

		// Check to see if the next token is a comma
		// and if not, then break out of the loop,
		// otherwise repeat until we find no comma.

		if _, _, exi := p.mightBe(COMMA); !exi {
			break
		}

	}

	return

}

func (p *parser) parseValue() (*Value, error) {

	tok, lit, err := p.shouldBe(STRING, REGION)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"string"}}
	}

	val, err := p.declare(tok, lit)

	return val.(*Value), err

}

func (p *parser) parseIdent() (*Ident, error) {

	_, lit, err := p.shouldBe(IDENT)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"name"}}
	}

	val, err := p.declare(IDENT, lit)

	return val.(*Ident), err

}

func (p *parser) parseIdents() (mul Idents, err error) {

	for {

		one, err := p.parseIdent()
		if err != nil {
			return nil, err
		}

		mul = append(mul, one)

		if _, _, exi := p.mightBe(COMMA); !exi {
			break
		}

	}

	return

}

func (p *parser) parseTable() (*Table, error) {

	_, lit, err := p.shouldBe(IDENT)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"table"}}
	}

	val, err := p.declare(TABLE, lit)

	return val.(*Table), err

}

func (p *parser) parseTables() (mul Tables, err error) {

	for {

		one, err := p.parseTable()
		if err != nil {
			return nil, err
		}

		mul = append(mul, one)

		if _, _, exi := p.mightBe(COMMA); !exi {
			break
		}

	}

	return

}

func (p *parser) parseIdiom() (*Ident, error) {

	_, lit, err := p.shouldBe(IDENT, EXPR)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"name, or expression"}}
	}

	val, err := p.declare(IDENT, lit)

	return val.(*Ident), err

}

func (p *parser) parseIdioms() (mul Idents, err error) {

	for {

		one, err := p.parseIdiom()
		if err != nil {
			return nil, err
		}

		mul = append(mul, one)

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

func (p *parser) parseBinary() ([]byte, error) {

	_, lit, err := p.shouldBe(STRING, REGION)
	if err != nil {
		return nil, &ParseError{Found: lit, Expected: []string{"string"}}
	}

	return []byte(lit), err

}

func (p *parser) parseTimeout() (time.Duration, error) {

	if _, _, exi := p.mightBe(TIMEOUT); !exi {
		return 0, nil
	}

	return p.parseDuration()

}

func (p *parser) parseDuration() (time.Duration, error) {

	tok, lit, err := p.shouldBe(DURATION)
	if err != nil {
		return 0, &ParseError{Found: lit, Expected: []string{"duration"}}
	}

	val, err := p.declare(tok, lit)

	return val.(time.Duration), err

}

func (p *parser) parsePriority() (float64, error) {

	tok, lit, err := p.shouldBe(NUMBER)
	if err != nil {
		return 0, &ParseError{Found: lit, Expected: []string{"number"}}
	}

	val, err := p.declare(tok, lit)

	return val.(float64), err

}

func (p *parser) parseType() (t, k string, err error) {

	_, t, err = p.shouldBe(IDENT, STRING, PASSWORD)
	if err != nil {
		err = &ParseError{Found: t, Expected: allowedTypes}
		return
	}

	if !contains(t, allowedTypes) {
		err = &ParseError{Found: t, Expected: allowedTypes}
		return
	}

	if t == "record" {
		if _, _, exi := p.mightBe(LPAREN); exi {
			if _, k, err = p.shouldBe(IDENT); err != nil {
				return
			}
			if _, _, err = p.shouldBe(RPAREN); err != nil {
				return
			}
		}
	}

	return

}

func (p *parser) parseLanguage() (language.Tag, error) {

	_, lit, err := p.shouldBe(IDENT, STRING)
	if err != nil {
		return language.English, &ParseError{Found: lit, Expected: []string{"string"}}
	}

	tag, err := language.Parse(lit)
	if err != nil {
		return language.English, &ParseError{Found: lit, Expected: []string{"BCP47 language"}}
	}

	if _, _, exi := p.mightBe(NUMERIC); exi {
		tag, _ = tag.SetTypeForKey("kn", "true")
	}

	return tag, err

}

func (p *parser) parseAlgorithm() (string, error) {

	_, lit, err := p.shouldBe(IDENT, STRING)
	if err != nil {
		return string(""), &ParseError{Found: lit, Expected: allowedAlgorithms}
	}

	switch lit {
	case
		"ES256", "ES384", "ES512",
		"HS256", "HS384", "HS512",
		"PS256", "PS384", "PS512",
		"RS256", "RS384", "RS512":
	default:
		return string(""), &ParseError{Found: lit, Expected: allowedAlgorithms}
	}

	return lit, err

}

func (p *parser) parseExpr() (exp Expr, err error) {

	// Create the root binary expression tree.

	root := &BinaryExpression{}

	// If the primary token is an in, out, or
	// multi way path expression, then follow
	// the path through to the end.

	if tok, _, exi := p.mightBe(OEDGE, IEDGE, BEDGE); exi {

		root.RHS, err = p.parsePath(tok)
		if err != nil {
			return nil, err
		}

	} else {

		// Otherwise begin with parsing the first
		// expression, as the root of the tree.

		root.RHS, err = p.parsePart()
		if err != nil {
			return nil, err
		}

		// But if the subsequent token is an in, out,
		// or multi way path expression, then follow
		// the path through to the end.

		if tok, _, exi := p.mightBe(DOT, OEDGE, IEDGE, BEDGE); exi {

			root.RHS, err = p.parsePath(root.RHS, tok)
			if err != nil {
				return nil, err
			}

		}

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
			rhs, err = p.parseExpr()
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

			if r, ok := rhs.(*BinaryExpression); ok {

				if r.Op.precedence() < tok.precedence() {

					r.LHS = &BinaryExpression{
						LHS: root.RHS,
						Op:  tok,
						RHS: r.LHS,
					}

					node.RHS = rhs

					break

				}

			}

			r, ok := node.RHS.(*BinaryExpression)

			if !ok || r.Op.precedence() <= tok.precedence() {

				node.RHS = &BinaryExpression{
					LHS: node.RHS,
					Op:  tok,
					RHS: rhs,
				}

				break

			}

			node = r

		}

	}

	return nil, nil

}

func (p *parser) parsePart() (exp Expr, err error) {

	toks := []Token{
		MUL, EXPR, IDENT, THING, MODEL,
		NULL, VOID, EMPTY, MISSING,
		TRUE, FALSE, STRING, REGION, NUMBER, DOUBLE, REGEX,
		DATE, TIME, DURATION, JSON, ARRAY, PARAM, LPAREN, IF,
	}

	tok, lit, _ := p.scan()

	// We need to declare the type up here instead
	// of at the bottom, as the held value might
	// be overwritten by the next token scan.

	exp, err = p.declare(tok, lit)
	if err != nil {
		return nil, err
	}

	// If the current token is a IF word clause
	// then we will parse anything from here on
	// as an IF expression clause.

	if is(tok, IF) {
		return p.parseIfel()
	}

	// If the current token is a left parenthesis
	// bracket, then we will parse this complete
	// expression part as a subquery.

	if is(tok, LPAREN) {
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

	if !in(tok, toks) {
		err = &ParseError{Found: lit, Expected: []string{"expression"}}
	}

	return

}

func (p *parser) parseSubq() (sub *SubExpression, err error) {

	var exp Expr
	var tok Token

	tok, _, _ = p.mightBe(SELECT, CREATE, UPDATE, DELETE, RELATE, INSERT, UPSERT)

	switch tok {
	case SELECT:
		exp, err = p.parseSelectStatement()
	case CREATE:
		p.buf.rw = true
		exp, err = p.parseCreateStatement()
	case UPDATE:
		p.buf.rw = true
		exp, err = p.parseUpdateStatement()
	case DELETE:
		p.buf.rw = true
		exp, err = p.parseDeleteStatement()
	case RELATE:
		p.buf.rw = true
		exp, err = p.parseRelateStatement()
	case INSERT:
		p.buf.rw = true
		exp, err = p.parseInsertStatement()
	case UPSERT:
		p.buf.rw = true
		exp, err = p.parseUpsertStatement()
	default:
		exp, err = p.parseExpr()
	}

	if err != nil {
		return nil, err
	}

	_, _, err = p.shouldBe(RPAREN)

	return &SubExpression{Expr: exp}, err

}

func (p *parser) parseIfel() (exp *IfelExpression, err error) {

	exp = &IfelExpression{}

	for {

		var tok Token

		if cond, err := p.parseExpr(); err != nil {
			return nil, err
		} else {
			exp.Cond = append(exp.Cond, cond)
		}

		if _, _, err = p.shouldBe(THEN); err != nil {
			return nil, err
		}

		if then, err := p.parseExpr(); err != nil {
			return nil, err
		} else {
			exp.Then = append(exp.Then, then)
		}

		// Check to see if the next token is an
		// ELSE keyword and if it is then check to
		// see if there is another if statement.

		if tok, _, err = p.shouldBe(ELSE, END); err != nil {
			return nil, err
		}

		if tok == END {
			return
		}

		if tok == ELSE {
			if _, _, exi := p.mightBe(IF); !exi {
				break
			}
		}

	}

	if then, err := p.parseExpr(); err != nil {
		return nil, err
	} else {
		exp.Else = then
	}

	if _, _, err = p.shouldBe(END); err != nil {
		return nil, err
	}

	return

}

func (p *parser) parseCall(name string) (fnc *FuncExpression, err error) {

	fnc = &FuncExpression{Name: name}

	// Check to see if this is an aggregate
	// function, and if it is then mark it,
	// so we can process it correcyly in the
	// 'iterator' and 'document' layers.

	if _, ok := aggrs[name]; ok {
		fnc.Aggr = true
	}

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

	// Check to see if this function is allowed to
	// have an undefined number of arguments, and
	// if it is then skip argument checking.

	if _, ok := funcs[fnc.Name][-1]; ok {
		return
	}

	// Check to see if the number of arguments
	// is correct for the specified function name,
	// and if not, then return an error.

	if _, ok := funcs[fnc.Name][len(fnc.Args)]; !ok {

		s, a, t := "", []int{}, len(funcs[fnc.Name])

		for i := range funcs[fnc.Name] {
			a = append(a, i)
		}

		sort.Ints(a)

		for i := 0; i < t; i++ {
			switch {
			case i > 0 && i == t-1:
				s = s + " or "
			case i > 0:
				s = s + ", "
			}
			s = s + fmt.Sprintf("%d", a[i])
		}

		switch t {
		case 1:
			s = s + " argument"
		default:
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

	defer func() {
		if val, ok := path.Expr[len(path.Expr)-1].(*JoinExpression); ok {
			if val.Join == DOT {
				err = &ParseError{
					Found:    fmt.Sprintf("."),
					Expected: []string{"field expression"},
				}
			}
		}
	}()

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

}

func (p *parser) parseJoin() (exp Expr, err error) {

	toks := []Token{
		DOT, OEDGE, IEDGE, BEDGE,
	}

	tok, _, _ := p.scan()

	if !in(tok, toks) {
		p.unscan()
		return
	}

	return tok, err

}

func (p *parser) parseStep() (exp Expr, err error) {

	toks := []Token{
		QMARK, IDENT, THING, LPAREN, EXPR, MUL,
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

	if is(tok, LPAREN) {
		return p.parseSubp()
	}

	// If this expression is not a sub-path
	// expression, then check to see if the
	// token is in the list of allowed tokens.

	if !in(tok, toks) {
		p.unscan()
		exp = nil
	}

	return

}

func (p *parser) parseSubp() (stmt *SubpExpression, err error) {

	stmt = &SubpExpression{}

	// IMPORTANT maybe we should not accept any expression here

	if stmt.What, err = p.parseWhat(); err != nil {
		return nil, err
	}

	if _, _, exi := p.mightBe(AS); exi {
		if stmt.Name, err = p.parseIdent(); err != nil {
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
