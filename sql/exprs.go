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
	"regexp"
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
			one, err := p.declare(PARAM, lit)
			if err != nil {
				return nil, err
			}
			mul = append(mul, one)
		}

		// If the next token is not a comma then break the loop.
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

		// If the next token is not a comma then break the loop.
		if _, _, exi := p.mightBe(COMMA); !exi {
			break
		}

	}

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

func (p *parser) parseDefault() (interface{}, error) {

	tok, lit, err := p.shouldBe(NULL, NOW, DATE, TIME, TRUE, FALSE, NUMBER, DOUBLE, STRING, REGION, IDENT, THING, ARRAY, JSON)
	if err != nil {
		return nil, err
	}

	return p.declare(tok, lit)

}

func (p *parser) parseExpr() (mul []*Field, err error) {

	var tok Token
	var lit string
	var exi bool
	var val interface{}

	for {

		one := &Field{}

		tok, lit, err = p.shouldBe(IDENT, ID, NOW, PATH, NULL, ALL, TIME, TRUE, FALSE, STRING, REGION, NUMBER, DOUBLE, JSON, ARRAY)
		if err != nil {
			return nil, &ParseError{Found: lit, Expected: []string{"field name"}}
		}

		one.Expr, err = p.declare(tok, lit)
		if err != nil {
			return
		}

		one.Alias = lit

		// Next token might be AS
		if _, _, exi = p.mightBe(AS); exi {

			_, lit, err = p.shouldBe(IDENT)
			if err != nil {
				return nil, &ParseError{Found: lit, Expected: []string{"field alias"}}
			}

			val, err = p.declare(STRING, lit)
			if err != nil {
				return
			}

			one.Alias = val.(string)

		}

		mul = append(mul, one)

		// If the next token is not a comma then break the loop.
		if _, _, exi = p.mightBe(COMMA); !exi {
			break
		}

	}

	return

}
