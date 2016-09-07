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
	"bufio"
	"bytes"
	"io"
	"regexp"
	"strings"
	"time"
)

// Scanner represents a lexical scanner.
type Scanner struct {
	b []rune // any runes before
	a []rune // any runes after
	p *Parser
	r *bufio.Reader
}

// NewScanner returns a new instance of Scanner.
func NewScanner(p *Parser, r io.Reader) *Scanner {
	return &Scanner{p: p, r: bufio.NewReader(r)}
}

// Scan returns the next token and literal value.
func (s *Scanner) Scan() (tok Token, lit string) {

	// Read the next rune.
	ch := s.next()

	// If we see whitespace then consume all contiguous whitespace.
	if isWhitespace(ch) {
		return s.scanBlank(ch)
	}

	// If we see a letter then consume as an string.
	if isLetter(ch) {
		return s.scanIdent(ch)
	}

	// If we see a number then consume as a number.
	if isNumber(ch) {
		return s.scanNumber(ch)
	}

	// Otherwise read the individual character.
	switch ch {

	case eof:
		return EOF, ""
	case '*':
		return ALL, string(ch)
	case '×':
		return MUL, string(ch)
	case '∙':
		return MUL, string(ch)
	case '÷':
		return DIV, string(ch)
	case '@':
		return EAT, string(ch)
	case ',':
		return COMMA, string(ch)
	case '.':
		return DOT, string(ch)
	case '"':
		return s.scanString(ch)
	case '\'':
		return s.scanString(ch)
	case '`':
		return s.scanQuoted(ch)
	case '⟨':
		return s.scanQuoted(ch)
	case '{':
		return s.scanObject(ch)
	case '[':
		return s.scanObject(ch)
	case '$':
		return s.scanParams(ch)
	case ':':
		return COLON, string(ch)
	case ';':
		return SEMICOLON, string(ch)
	case '(':
		return LPAREN, string(ch)
	case ')':
		return RPAREN, string(ch)
	case '¬':
		return NEQ, string(ch)
	case '≤':
		return LTE, string(ch)
	case '≥':
		return GTE, string(ch)
	case '~':
		return SIN, string(ch)
	case '∋':
		return SIN, string(ch)
	case '∌':
		return SNI, string(ch)
	case '⊇':
		return CONTAINSALL, string(ch)
	case '⊃':
		return CONTAINSSOME, string(ch)
	case '⊅':
		return CONTAINSNONE, string(ch)
	case '∈':
		return INS, string(ch)
	case '∉':
		return NIS, string(ch)
	case '⊆':
		return ALLCONTAINEDIN, string(ch)
	case '⊂':
		return SOMECONTAINEDIN, string(ch)
	case '⊄':
		return NONECONTAINEDIN, string(ch)
	case '#':
		return s.scanCommentSingle(ch)
	case '/':
		chn := s.next()
		switch {
		case chn == '*':
			return s.scanCommentMultiple(ch)
		case chn == ' ':
			s.undo()
			return DIV, string(ch)
		default:
			s.undo()
			return s.scanRegexp(ch)
		}
	case '=':
		chn := s.next()
		switch {
		case chn == '~':
			return SIN, "=~"
		case chn == '=':
			return EEQ, "=="
		default:
			s.undo()
			return EQ, string(ch)
		}
	case '?':
		chn := s.next()
		switch {
		case chn == '=':
			return ANY, "?="
		default:
			s.undo()
			return QMARK, string(ch)
		}
	case '!':
		chn := s.next()
		switch {
		case chn == '=':
			if s.next() == '=' {
				return NEE, "!=="
			} else {
				s.undo()
				return NEQ, "!="
			}
		case chn == '~':
			return SNI, "!~"
		default:
			s.undo()
			return EXC, string(ch)
		}
	case '+':
		chn := s.next()
		switch {
		case chn == '=':
			return INC, "+="
		case isNumber(chn):
			return s.scanNumber(ch, chn)
		default:
			s.undo()
			return ADD, string(ch)
		}
	case '-':
		chn := s.next()
		switch {
		case chn == '=':
			return DEC, "-="
		case chn == '>':
			return OEDGE, "->"
		case chn == '-':
			return s.scanCommentSingle(ch)
		case isNumber(chn):
			return s.scanNumber(ch, chn)
		default:
			s.undo()
			return SUB, string(ch)
		}
	case '>':
		chn := s.next()
		switch {
		case chn == '=':
			return GTE, ">="
		default:
			s.undo()
			return GT, string(ch)
		}
	case '<':
		chn := s.next()
		switch {
		case chn == '>':
			return NEQ, "<>"
		case chn == '=':
			return LTE, "<="
		case chn == '-':
			if s.next() == '>' {
				return BEDGE, "<->"
			} else {
				s.undo()
				return IEDGE, "<-"
			}
		default:
			s.undo()
			return LT, string(ch)
		}
	}

	return ILLEGAL, string(ch)

}

// scanBlank consumes the current rune and all contiguous whitespace.
func (s *Scanner) scanBlank(chp ...rune) (tok Token, lit string) {

	tok = WS

	// Create a buffer
	var buf bytes.Buffer

	// Read passed in runes
	for _, ch := range chp {
		buf.WriteRune(ch)
	}

	// Read subsequent characters
	for {
		if ch := s.next(); ch == eof {
			break
		} else if !isWhitespace(ch) {
			s.undo()
			break
		} else {
			buf.WriteRune(ch)
		}
	}

	return tok, buf.String()

}

// scanCommentSingle consumes the current rune and all contiguous whitespace.
func (s *Scanner) scanCommentSingle(chp ...rune) (tok Token, lit string) {

	tok = WS

	// Create a buffer
	var buf bytes.Buffer

	// Read passed in runes
	for _, ch := range chp {
		buf.WriteRune(ch)
	}

	// Read subsequent characters
	for {
		if ch := s.next(); ch == '\n' || ch == '\r' {
			buf.WriteRune(ch)
			break
		} else {
			buf.WriteRune(ch)
		}
	}

	return tok, buf.String()

}

// scanCommentMultiple consumes the current rune and all contiguous whitespace.
func (s *Scanner) scanCommentMultiple(chp ...rune) (tok Token, lit string) {

	tok = WS

	// Create a buffer
	var buf bytes.Buffer

	// Read passed in runes
	for _, ch := range chp {
		buf.WriteRune(ch)
	}

	// Read subsequent characters
	for {
		if ch := s.next(); ch == eof {
			break
		} else if ch == '*' {
			if chn := s.next(); chn == '/' {
				buf.WriteRune(chn)
				break
			}
			buf.WriteRune(ch)
		} else {
			buf.WriteRune(ch)
		}
	}

	return tok, buf.String()

}

func (s *Scanner) scanParams(chp ...rune) (Token, string) {

	tok, lit := s.scanIdent(chp...)

		return BOUNDPARAM, lit
	if s.p.is(tok, IDENT) {
	}

	return tok, lit

}

// scanIdent consumes the current rune and all contiguous ident runes.
func (s *Scanner) scanIdent(chp ...rune) (tok Token, lit string) {

	tok = IDENT

	// Create a buffer
	var buf bytes.Buffer

	// Read passed in runes
	for _, ch := range chp {
		buf.WriteRune(ch)
	}

	// Read subsequent characters
	for {
		if ch := s.next(); ch == eof {
			break
		} else if !isIdentChar(ch) {
			s.undo()
			break
		} else {
			buf.WriteRune(ch)
		}
	}

	// If the string matches a keyword then return that keyword.
	if tok := keywords[strings.ToUpper(buf.String())]; tok > 0 {
		return tok, buf.String()
	}

	if _, err := time.ParseDuration(buf.String()); err == nil {
		return DURATION, buf.String()
	}

	// Otherwise return as a regular identifier.
	return tok, buf.String()

}

func (s *Scanner) scanNumber(chp ...rune) (tok Token, lit string) {

	tok = NUMBER

	// Create a buffer
	var buf bytes.Buffer

	// Read passed in runes
	for _, ch := range chp {
		buf.WriteRune(ch)
	}

	// Read subsequent characters
	for {
		if ch := s.next(); ch == eof {
			break
		} else if isNumber(ch) {
			buf.WriteRune(ch)
		} else if isLetter(ch) {
			tok = IDENT
			buf.WriteRune(ch)
		} else if ch == '.' {
			if tok == DOUBLE {
				tok = IDENT
			}
			if tok == NUMBER {
				tok = DOUBLE
			}
			buf.WriteRune(ch)
		} else {
			s.undo()
			break
		}
	}

	return tok, buf.String()

}

func (s *Scanner) scanQuoted(chp ...rune) (Token, string) {

	tok, lit := s.scanString(chp...)

		return IDENT, lit
	if s.p.is(tok, STRING) {
	}

	return tok, lit

}

func (s *Scanner) scanString(chp ...rune) (tok Token, lit string) {

	beg := chp[0]
	end := beg

	if beg == '"' {
		end = '"'
	}

	if beg == '`' {
		end = '`'
	}

	if beg == '⟨' {
		end = '⟩'
	}

	tok = STRING

	// Create a buffer
	var buf bytes.Buffer

	// Ignore passed in runes

	// Read subsequent characters
	for {
		if ch := s.next(); ch == end {
			break
		} else if ch == eof {
			return ILLEGAL, buf.String()
		} else if ch == '\n' {
			tok = REGION
			buf.WriteRune(ch)
		} else if ch == '\r' {
			tok = REGION
			buf.WriteRune(ch)
		} else if ch == '\\' {
			switch chn := s.next(); chn {
			default:
				buf.WriteRune(chn)
			case 'b':
				continue
			case 't':
				tok = REGION
				buf.WriteRune('\t')
			case 'r':
				tok = REGION
				buf.WriteRune('\r')
			case 'n':
				tok = REGION
				buf.WriteRune('\n')
			}
		} else {
			buf.WriteRune(ch)
		}
	}

	if _, err := time.ParseDuration(buf.String()); err == nil {
		return DURATION, buf.String()
	}

	if _, err := time.Parse("2006-01-02", buf.String()); err == nil {
		return DATE, buf.String()
	}

	if _, err := time.Parse(time.RFC3339, buf.String()); err == nil {
		return TIME, buf.String()
	}

	return tok, buf.String()

}

func (s *Scanner) scanRegexp(chp ...rune) (tok Token, lit string) {

	tok = IDENT

	// Create a buffer
	var buf bytes.Buffer

	// Ignore passed in runes

	// Read subsequent characters
	for {
		if ch := s.next(); ch == chp[0] {
			break
		} else if ch == eof {
			return ILLEGAL, buf.String()
		} else if ch == '\\' {
			chn := s.next()
			buf.WriteRune(ch)
			buf.WriteRune(chn)
		} else {
			buf.WriteRune(ch)
		}
	}

	if _, err := regexp.Compile(buf.String()); err == nil {
		return REGEX, buf.String()
	}

	return tok, buf.String()

}

func (s *Scanner) scanObject(chp ...rune) (tok Token, lit string) {

	beg := chp[0]
	end := beg
	sub := 0

	if beg == '{' {
		end = '}'
	}

	if beg == '[' {
		end = ']'
	}

	tok = IDENT

	// Create a buffer
	var buf bytes.Buffer

	// Read passed in runes
	for _, ch := range chp {
		buf.WriteRune(ch)
	}

	// Read subsequent characters
	for {
		if ch := s.next(); ch == end && sub == 0 {
			buf.WriteRune(ch)
			break
		} else if ch == beg {
			sub++
			buf.WriteRune(ch)
		} else if ch == end {
			sub--
			buf.WriteRune(ch)
		} else if ch == eof {
			return ILLEGAL, buf.String()
		} else if ch == '\\' {
			switch chn := s.next(); chn {
			default:
				return ILLEGAL, buf.String()
			case 'b', 't', 'r', 'n', 'f', '"', '\\':
				buf.WriteRune(ch)
				buf.WriteRune(chn)
			}
		} else {
			buf.WriteRune(ch)
		}
	}

	if beg == '{' {
		return JSON, buf.String()
	}
	if beg == '[' {
		return ARRAY, buf.String()
	}

	return ILLEGAL, buf.String()

}

// next reads the next rune from the bufferred reader.
// Returns the rune(0) if an error occurs (or io.EOF is returned).
func (s *Scanner) next() rune {

	if len(s.a) > 0 {
		var r rune
		r, s.a = s.a[len(s.a)-1], s.a[:len(s.a)-1]
		s.b = append(s.b, r)
		return r
	}

	r, _, err := s.r.ReadRune()
	if err != nil {
		return eof
	}
	s.b = append(s.b, r)
	return r

}

// undo places the previously read rune back on the reader.
func (s *Scanner) undo() {

	if len(s.b) > 0 {
		var r rune
		r, s.b = s.b[len(s.b)-1], s.b[:len(s.b)-1]
		s.a = append(s.a, r)
		return
	}

	_ = s.r.UnreadRune()

}

// isWhitespace returns true if the rune is a space, tab, or newline.
func isWhitespace(ch rune) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

// isNumber returns true if the rune is a number.
func isNumber(ch rune) bool {
	return (ch >= '0' && ch <= '9')
}

// isLetter returns true if the rune is a letter.
func isLetter(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

// isIdentChar returns true if the rune is allowed in a IDENT.
func isIdentChar(ch rune) bool {
	return isLetter(ch) || isNumber(ch) || ch == '.' || ch == '_' || ch == '*'
}

// eof represents a marker rune for the end of the reader.
var eof = rune(0)
