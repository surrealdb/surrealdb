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

// Parser represents a parser.
type Parser struct {
	s   *Scanner
	c   *fibre.Context
	v   map[string]interface{}
	buf struct {
		tok Token  // last read token
		lit string // last read literal
		n   int    // buffer size (max=1)
	}
}

// Parse parses sql from a []byte, string, or io.Reader.
func Parse(ctx *fibre.Context, i interface{}) (*Query, error) {
	switch v := i.(type) {
	default:
		return nil, &EmptyError{}
	case []byte:
		return ParseBytes(ctx, v)
	case string:
		return ParseString(ctx, v)
	case io.Reader:
		return ParseBuffer(ctx, v)
	}
}

// ParseBytes parses a byte array.
func ParseBytes(ctx *fibre.Context, i []byte) (*Query, error) {
	r := bytes.NewReader(i)
	p := &Parser{c: ctx}
	p.s = NewScanner(p, r)
	return p.Parse()
}

// ParseString parses a string.
func ParseString(ctx *fibre.Context, i string) (*Query, error) {
	r := strings.NewReader(i)
	p := &Parser{c: ctx}
	p.s = NewScanner(p, r)
	return p.Parse()
}

// ParseBuffer parses a buffer.
func ParseBuffer(ctx *fibre.Context, r io.Reader) (*Query, error) {
	p := &Parser{c: ctx}
	p.s = NewScanner(p, r)
	return p.Parse()
}

// Parse parses single or multiple SQL queries.
func (p *Parser) Parse() (*Query, error) {
	return p.ParseMulti()
}

// ParseMulti parses multiple SQL SELECT statements.
func (p *Parser) ParseMulti() (*Query, error) {

	var statements Statements

	var semi bool
	var text bool

	for {
		if tok, _ := p.scanIgnoreWhitespace(); tok == EOF {
			if !text {
				return nil, &EmptyError{}
			}
			return &Query{Statements: statements}, nil
		} else if !semi && tok == SEMICOLON {
			semi = true
		} else {
			text = true
			p.unscan()
			s, err := p.ParseSingle()
			if err != nil {
				return nil, err
			}
			statements = append(statements, s)
			semi = false
		}
	}

}

// ParseSingle parses a single SQL SELECT statement.
func (p *Parser) ParseSingle() (Statement, error) {

	var explain bool

	if _, _, exi := p.mightBe(EXPLAIN); exi {
		explain = true
	}

	tok, _, err := p.shouldBe(USE, LET, BEGIN, CANCEL, COMMIT, ROLLBACK, SELECT, CREATE, UPDATE, INSERT, UPSERT, MODIFY, DELETE, RELATE, RECORD, DEFINE, RESYNC, REMOVE)

	switch tok {

	case USE:
		return p.parseUseStatement(explain)

	case BEGIN:
		return p.parseBeginStatement(explain)
	case CANCEL, ROLLBACK:
		return p.parseCancelStatement(explain)
	case COMMIT:
		return p.parseCommitStatement(explain)

	case SELECT:
		return p.parseSelectStatement(explain)
	case CREATE, INSERT:
		return p.parseCreateStatement(explain)
	case UPDATE, UPSERT:
		return p.parseUpdateStatement(explain)
	case MODIFY:
		return p.parseModifyStatement(explain)
	case DELETE:
		return p.parseDeleteStatement(explain)
	case RELATE:
		return p.parseRelateStatement(explain)
	case RECORD:
		return p.parseRecordStatement(explain)

	case DEFINE:
		return p.parseDefineStatement(explain)
	case RESYNC:
		return p.parseResyncStatement(explain)
	case REMOVE:
		return p.parseRemoveStatement(explain)

	default:
		return nil, err

	}

}

func (p *Parser) mightBe(expected ...Token) (tok Token, lit string, found bool) {

	tok, lit = p.scanIgnoreWhitespace()

	if found = p.in(tok, expected); !found {
		p.unscan()
	}

	return

}

func (p *Parser) shouldBe(expected ...Token) (tok Token, lit string, err error) {

	tok, lit = p.scanIgnoreWhitespace()

	if found := p.in(tok, expected); !found {
		p.unscan()
		err = &ParseError{Found: lit, Expected: lookup(expected)}
	}

	return

}

// scan returns the next token from the underlying scanner.
// If a token has been unscanned then read that instead.
func (p *Parser) scan() (tok Token, lit string) {
	// If we have a token on the buffer, then return it.
	if p.buf.n != 0 {
		p.buf.n = 0
		return p.buf.tok, p.buf.lit
	}

	// Otherwise read the next token from the scanner.
	tok, lit = p.s.Scan()

	// Save it to the buffer in case we unscan later.
	p.buf.tok, p.buf.lit = tok, lit

	return
}

// unscan pushes the previously read token back onto the buffer.
func (p *Parser) unscan() { p.buf.n = 1 }

// scanIgnoreWhitespace scans the next non-whitespace token.
func (p *Parser) scanIgnoreWhitespace() (tok Token, lit string) {
	tok, lit = p.scan()
	for {
		if tok == WS {
			tok, lit = p.scan()
		} else {
			break
		}
	}
	return
}
