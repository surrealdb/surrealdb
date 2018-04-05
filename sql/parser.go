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
	o   *options
	c   *fibre.Context
	buf struct {
		n   int         // buffer size
		rw  bool        // writeable
		txn bool        // inside txn
		tok Token       // last read token
		lit string      // last read literal
		val interface{} // Last read value
	}
}

// Parse parses sql from a []byte, string, or io.Reader.
func Parse(c *fibre.Context, i interface{}) (*Query, error) {

	switch x := i.(type) {
	default:
		return nil, &EmptyError{}
	case []byte:
		return parseBytes(c, x)
	case string:
		return parseString(c, x)
	case io.Reader:
		return parseBuffer(c, x)
	}

}

// newParser returns a new instance of Parser.
func newParser(c *fibre.Context) *parser {
	return &parser{c: c, o: newOptions(c)}
}

// parseBytes parses a byte array.
func parseBytes(c *fibre.Context, i []byte) (*Query, error) {
	p := newParser(c)
	r := bytes.NewReader(i)
	p.s = newScanner(r)
	return p.parse()
}

// parseString parses a string.
func parseString(c *fibre.Context, i string) (*Query, error) {
	p := newParser(c)
	r := strings.NewReader(i)
	p.s = newScanner(r)
	return p.parse()
}

// parseBuffer parses a buffer.
func parseBuffer(c *fibre.Context, r io.Reader) (*Query, error) {
	p := newParser(c)
	p.s = newScanner(r)
	return p.parse()
}

// parse parses single or multiple SQL queries.
func (p *parser) parse() (*Query, error) {
	return p.parseMulti()
}

// parseMulti parses multiple SQL SELECT statements.
func (p *parser) parseMulti() (*Query, error) {

	var semi bool

	var stmts Statements

	for {

		// If the next token is an EOF then
		// check to see if the query is empty
		// or return the parsed statements.

		if _, _, exi := p.mightBe(EOF); exi {
			if len(stmts) == 0 {
				return nil, new(EmptyError)
			}
			return &Query{Statements: stmts}, nil
		}

		// If this is a multi statement query
		// and there is no semicolon separating
		// the statements, then return an error.

		if len(stmts) > 0 {
			switch semi {
			case true:
				_, _, exi := p.mightBe(SEMICOLON)
				if exi {
					continue
				}
			case false:
				_, _, err := p.shouldBe(SEMICOLON)
				if err != nil {
					return nil, err
				}
				semi = true
				continue
			}
		}

		// Parse the next token as a statement
		// and append it to the statements
		// array for the current sql query.

		stmt, err := p.parseSingle()
		if err != nil {
			return nil, err
		}

		stmts = append(stmts, stmt)

	}

}

// parseSingle parses a single SQL SELECT statement.
func (p *parser) parseSingle() (stmt Statement, err error) {

	p.buf.rw = false

	tok, _, err := p.shouldBe(
		USE,
		INFO,
		BEGIN,
		CANCEL,
		COMMIT,
		IF,
		LET,
		RETURN,
		LIVE,
		KILL,
		SELECT,
		CREATE,
		UPDATE,
		DELETE,
		RELATE,
		INSERT,
		UPSERT,
		DEFINE,
		REMOVE,
	)

	switch tok {

	case IF:
		return p.parseIfStatement()

	case USE:
		return p.parseUseStatement()

	case LET:
		return p.parseLetStatement()

	case INFO:
		return p.parseInfoStatement()

	case LIVE:
		return p.parseLiveStatement()
	case KILL:
		return p.parseKillStatement()

	case BEGIN:
		return p.parseBeginStatement()
	case CANCEL:
		return p.parseCancelStatement()
	case COMMIT:
		return p.parseCommitStatement()

	case RETURN:
		return p.parseReturnStatement()

	case SELECT:
		return p.parseSelectStatement()
	case CREATE:
		return p.parseCreateStatement()
	case UPDATE:
		return p.parseUpdateStatement()
	case DELETE:
		return p.parseDeleteStatement()
	case RELATE:
		return p.parseRelateStatement()

	case INSERT:
		return p.parseInsertStatement()
	case UPSERT:
		return p.parseUpsertStatement()

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

	if found = in(tok, expected); !found {
		p.unscan()
	}

	return

}

func (p *parser) shouldBe(expected ...Token) (tok Token, lit string, err error) {

	tok, lit, _ = p.scan()

	if !in(tok, expected) {
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
