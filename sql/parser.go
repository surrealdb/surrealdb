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
	"io"
	"strings"
)

// Parser represents a parser.
type Parser struct {
	s   *Scanner
	buf struct {
		tok Token  // last read token
		lit string // last read literal
		n   int    // buffer size (max=1)
	}
}

// Parse parses a string.
func Parse(s string) (*Query, error) {
	r := strings.NewReader(s)
	p := &Parser{s: NewScanner(r)}
	return p.Parse()
}

// NewParser returns a new instance of Parser.
func NewParser(r io.Reader) *Parser {
	return &Parser{s: NewScanner(r)}
}

// Parse parses single or multiple SQL queries.
func (p *Parser) Parse() (*Query, error) {
	return p.ParseMulti()
}

// ParseMulti parses multiple SQL SELECT statements.
func (p *Parser) ParseMulti() (*Query, error) {

	var statements Statements
	var semi bool

	for {
		if tok, _ := p.scanIgnoreWhitespace(); tok == EOF {
			return &Query{Statements: statements}, nil
		} else if !semi && tok == SEMICOLON {
			semi = true
		} else {
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

	// Inspect the first token.
	tok, lit := p.scanIgnoreWhitespace()

	switch tok {

	case SELECT:
		return p.parseSelectStatement()
	case CREATE:
		return p.parseCreateStatement()
	case UPDATE:
		return p.parseUpdateStatement()
	case MODIFY:
		return p.parseModifyStatement()
	case DELETE:
		return p.parseDeleteStatement()
	case RELATE:
		return p.parseRelateStatement()
	case RECORD:
		return p.parseRecordStatement()

	case DEFINE:
		return p.parseDefineStatement()
	case RESYNC:
		return p.parseResyncStatement()
	case REMOVE:
		return p.parseRemoveStatement()

	default:

		return nil, &ParseError{
			Found: lit,
			Expected: []string{
				"SELECT",
				"CREATE",
				"UPDATE",
				"MODIFY",
				"DELETE",
				"RELATE",
				"RECORD",
				"DEFINE",
				"RESYNC",
				"REMOVE",
			},
		}

	}

}

func (p *Parser) mightBe(expected ...Token) (tok Token, lit string, found bool) {

	tok, lit = p.scanIgnoreWhitespace()

	if found = in(tok, expected); !found {
		p.unscan()
	}

	return

}

func (p *Parser) shouldBe(expected ...Token) (tok Token, lit string, err error) {

	tok, lit = p.scanIgnoreWhitespace()

	if found := in(tok, expected); !found {
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
	if tok == WS {
		tok, lit = p.scan()
	}
	return
}
