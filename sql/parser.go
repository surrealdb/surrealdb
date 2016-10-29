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
	"bytes"
	"io"
	"strings"

	"github.com/abcum/fibre"
)

// parser represents a parser.
type parser struct {
	s   *scanner
	c   *fibre.Context
	v   map[string]interface{}
	buf struct {
		n   int         // buffer size
		tok Token       // last read token
		lit string      // last read literal
		val interface{} // Last read value
	}
}

// newParser returns a new instance of Parser.
func newParser(c *fibre.Context, v map[string]interface{}) *parser {
	return &parser{c: c, v: v}
}

// Parse parses sql from a []byte, string, or io.Reader.
func Parse(c *fibre.Context, i interface{}, v map[string]interface{}) (*Query, error) {
	switch x := i.(type) {
	default:
		return nil, &EmptyError{}
	case []byte:
		return parseBytes(c, x, v)
	case string:
		return parseString(c, x, v)
	case io.Reader:
		return parseBuffer(c, x, v)
	}
}

// parseBytes parses a byte array.
func parseBytes(c *fibre.Context, i []byte, v map[string]interface{}) (*Query, error) {
	r := bytes.NewReader(i)
	p := newParser(c, v)
	p.s = newScanner(p, r)
	return p.parse()
}

// parseString parses a string.
func parseString(c *fibre.Context, i string, v map[string]interface{}) (*Query, error) {
	r := strings.NewReader(i)
	p := newParser(c, v)
	p.s = newScanner(p, r)
	return p.parse()
}

// parseBuffer parses a buffer.
func parseBuffer(c *fibre.Context, r io.Reader, v map[string]interface{}) (*Query, error) {
	p := newParser(c, v)
	p.s = newScanner(p, r)
	return p.parse()
}

// parse parses single or multiple SQL queries.
func (p *parser) parse() (*Query, error) {
	return p.parseMulti()
}

// parseMulti parses multiple SQL SELECT statements.
func (p *parser) parseMulti() (*Query, error) {

	var statements Statements

	var semi bool
	var text bool

	for {
		if tok, _, _ := p.scan(); tok == EOF {
			if !text {
				return nil, &EmptyError{}
			}
			return &Query{Statements: statements}, nil
		} else if !semi && tok == SEMICOLON {
			semi = true
		} else {
			text = true
			p.unscan()
			s, err := p.parseSingle()
			if err != nil {
				return nil, err
			}
			statements = append(statements, s)
			semi = false
		}
	}

}

// parseSingle parses a single SQL SELECT statement.
func (p *parser) parseSingle() (Statement, error) {

	tok, _, err := p.shouldBe(USE, INFO, LET, BEGIN, CANCEL, COMMIT, ROLLBACK, RETURN, SELECT, CREATE, UPDATE, INSERT, UPSERT, DELETE, RELATE, DEFINE, REMOVE)

	switch tok {

	case USE:
		return p.parseUseStatement()

	case LET:
		return p.parseLetStatement()

	case INFO:
		return p.parseInfoStatement()

	case BEGIN:
		return p.parseBeginStatement()
	case CANCEL, ROLLBACK:
		return p.parseCancelStatement()
	case COMMIT:
		return p.parseCommitStatement()
	case RETURN:
		return p.parseReturnStatement()

	case SELECT:
		return p.parseSelectStatement()
	case CREATE, INSERT:
		return p.parseCreateStatement()
	case UPDATE, UPSERT:
		return p.parseUpdateStatement()
	case DELETE:
		return p.parseDeleteStatement()
	case RELATE:
		return p.parseRelateStatement()

	case DEFINE:
		return p.parseDefineStatement()
	case REMOVE:
		return p.parseRemoveStatement()

	default:
		return nil, err

	}

}

func (p *parser) mightBe(expected ...Token) (tok Token, lit string, found bool) {

	tok, lit, _ = p.scan()

	if found = p.in(tok, expected); !found {
		p.unscan()
	}

	return

}

func (p *parser) shouldBe(expected ...Token) (tok Token, lit string, err error) {

	tok, lit, _ = p.scan()

	if found := p.in(tok, expected); !found {
		p.unscan()
		err = &ParseError{Found: lit, Expected: lookup(expected)}
	}

	return

}

// scan scans the next non-whitespace token.
func (p *parser) scan() (tok Token, lit string, val interface{}) {

	tok, lit, val = p.seek()

	for {
		if tok == WS {
			tok, lit, val = p.seek()
		} else {
			break
		}
	}

	return

}

func (p *parser) hold(tok Token) (val interface{}) {
	if tok == p.buf.tok {
		return p.buf.val
	}
	return nil
}

// seek returns the next token from the underlying scanner.
// If a token has been unscanned then read that instead.
func (p *parser) seek() (tok Token, lit string, val interface{}) {

	// If we have a token on the buffer, then return it.
	if p.buf.n != 0 {
		p.buf.n = 0
		return p.buf.tok, p.buf.lit, p.buf.val
	}

	// Otherwise read the next token from the scanner.
	tok, lit, val = p.s.scan()

	// Save it to the buffer in case we unscan later.
	p.buf.tok, p.buf.lit, p.buf.val = tok, lit, val

	return

}

// unscan pushes the previously read token back onto the buffer.
func (p *parser) unscan() {
	p.buf.n = 1
}
