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
	"strconv"
	"strings"
	"time"
)

// scanner represents a lexical scanner.
type scanner struct {
	b []rune // any runes before
	a []rune // any runes after
	p *parser
	r *bufio.Reader
}

// newScanner returns a new instance of Scanner.
func newScanner(p *parser, r io.Reader) *scanner {
	return &scanner{p: p, r: bufio.NewReader(r)}
}

// scan returns the next token and literal value.
func (s *scanner) scan() (tok Token, lit string, val interface{}) {

	// Read the next rune.
	ch := s.next()

	// If we see whitespace then consume all contiguous whitespace.
	if isBlank(ch) {
		return s.scanBlank(ch)
	}

	// If we see a letter then consume as a string.
	if isLetter(ch) {
		return s.scanIdiom(ch)
	}

	// If we see a number then consume as a number.
	if isNumber(ch) {
		return s.scanNumber(ch)
	}

	// Otherwise read the individual character.
	switch ch {

	case eof:
		return EOF, "", val
	case '*':
		return MUL, string(ch), val
	case '×':
		return MUL, string(ch), val
	case '∙':
		return MUL, string(ch), val
	case '÷':
		return DIV, string(ch), val
	case ',':
		return COMMA, string(ch), val
	case '.':
		return DOT, string(ch), val
	case '@':
		return s.scanThing(ch)
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
		return COLON, string(ch), val
	case ';':
		return SEMICOLON, string(ch), val
	case '(':
		return LPAREN, string(ch), val
	case ')':
		return RPAREN, string(ch), val
	case '¬':
		return NEQ, string(ch), val
	case '≤':
		return LTE, string(ch), val
	case '≥':
		return GTE, string(ch), val
	case '~':
		return SIN, string(ch), val
	case '∋':
		return SIN, string(ch), val
	case '∌':
		return SNI, string(ch), val
	case '⊇':
		return CONTAINSALL, string(ch), val
	case '⊃':
		return CONTAINSSOME, string(ch), val
	case '⊅':
		return CONTAINSNONE, string(ch), val
	case '∈':
		return INS, string(ch), val
	case '∉':
		return NIS, string(ch), val
	case '⊆':
		return ALLCONTAINEDIN, string(ch), val
	case '⊂':
		return SOMECONTAINEDIN, string(ch), val
	case '⊄':
		return NONECONTAINEDIN, string(ch), val
	case '#':
		return s.scanCommentSingle(ch)
	case '|':
		chn := s.next()
		switch {
		case chn == '|':
			return OR, "OR", val
		default:
			s.undo()
			return s.scanModel(ch)
		}
	case '&':
		chn := s.next()
		switch {
		case chn == '&':
			return AND, "AND", val
		default:
			s.undo()
		}
	case '/':
		chn := s.next()
		switch {
		case chn == '/':
			return s.scanCommentSingle(ch)
		case chn == '*':
			return s.scanCommentMultiple(ch)
		case isNumber(chn):
			s.undo()
			return DIV, string(ch), val
		case chn == ' ':
			s.undo()
			return DIV, string(ch), val
		default:
			s.undo()
			return s.scanRegexp(ch)
		}
	case '=':
		chn := s.next()
		switch {
		case chn == '~':
			return SIN, "=~", val
		case chn == '=':
			return EEQ, "==", val
		default:
			s.undo()
			return EQ, string(ch), val
		}
	case '?':
		chn := s.next()
		switch {
		case chn == '=':
			return ANY, "?=", val
		default:
			s.undo()
			return QMARK, string(ch), val
		}
	case '!':
		chn := s.next()
		switch {
		case chn == '=':
			if s.next() == '=' {
				return NEE, "!==", val
			} else {
				s.undo()
				return NEQ, "!=", val
			}
		case chn == '~':
			return SNI, "!~", val
		default:
			s.undo()
			return EXC, string(ch), val
		}
	case '+':
		chn := s.next()
		switch {
		case chn == '=':
			return INC, "+=", val
		case isNumber(chn):
			return s.scanNumber(ch, chn)
		default:
			s.undo()
			return ADD, string(ch), val
		}
	case '-':
		chn := s.next()
		switch {
		case chn == '=':
			return DEC, "-=", val
		case chn == '>':
			return OEDGE, "->", val
		case chn == '-':
			return s.scanCommentSingle(ch)
		case isNumber(chn):
			return s.scanNumber(ch, chn)
		default:
			s.undo()
			return SUB, string(ch), val
		}
	case '>':
		chn := s.next()
		switch {
		case chn == '=':
			return GTE, ">=", val
		default:
			s.undo()
			return GT, string(ch), val
		}
	case '<':
		chn := s.next()
		switch {
		case chn == '>':
			return NEQ, "<>", val
		case chn == '=':
			return LTE, "<=", val
		case chn == '-':
			if s.next() == '>' {
				return BEDGE, "<->", val
			} else {
				s.undo()
				return IEDGE, "<-", val
			}
		default:
			s.undo()
			return LT, string(ch), val
		}
	}

	return ILLEGAL, string(ch), val

}

// scanBlank consumes the current rune and all contiguous whitespace.
func (s *scanner) scanBlank(chp ...rune) (tok Token, lit string, val interface{}) {

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
		} else if !isBlank(ch) {
			s.undo()
			break
		} else {
			buf.WriteRune(ch)
		}
	}

	return tok, buf.String(), val

}

// scanCommentSingle consumes the current rune and all contiguous whitespace.
func (s *scanner) scanCommentSingle(chp ...rune) (tok Token, lit string, val interface{}) {

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
		} else if ch == '\n' || ch == '\r' {
			buf.WriteRune(ch)
			break
		} else {
			buf.WriteRune(ch)
		}
	}

	return tok, buf.String(), val

}

// scanCommentMultiple consumes the current rune and all contiguous whitespace.
func (s *scanner) scanCommentMultiple(chp ...rune) (tok Token, lit string, val interface{}) {

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

	return tok, buf.String(), val

}

func (s *scanner) scanParams(chp ...rune) (tok Token, lit string, val interface{}) {

	tok, lit, _ = s.scanIdiom()

	if s.p.is(tok, THING) {
		return ILLEGAL, lit, val
	}

	if s.p.is(tok, REGION) {
		return ILLEGAL, lit, val
	}

	if s.p.is(tok, ILLEGAL) {
		return ILLEGAL, lit, val
	}

	return PARAM, lit, val

}

func (s *scanner) scanQuoted(chp ...rune) (tok Token, lit string, val interface{}) {

	var tbv string
	var idv interface{}

	// Create a buffer
	var buf bytes.Buffer

	tok, lit, _ = s.scanString(chp...)

	if s.p.is(tok, REGION) {
		return ILLEGAL, lit, val
	}

	if s.p.is(tok, ILLEGAL) {
		return ILLEGAL, lit, val
	}

	if ch := s.next(); ch == ':' {

		tbv = lit

		buf.WriteString(lit)

		buf.WriteRune(ch)

		if tok, lit, idv = s.part(); tok == ILLEGAL {
			buf.WriteString(lit)
			return ILLEGAL, buf.String(), val
		} else {
			buf.WriteString(lit)
		}

		return THING, buf.String(), NewThing(tbv, idv)

	} else if ch != eof {
		s.undo()
	}

	return IDENT, lit, val

}

func (s *scanner) scanSection(chp ...rune) (tok Token, lit string, val interface{}) {

	tok, lit, _ = s.scanString(chp...)

	if s.p.is(tok, REGION) {
		return ILLEGAL, lit, val
	}

	if s.p.is(tok, ILLEGAL) {
		return ILLEGAL, lit, val
	}

	return IDENT, lit, val

}

// scanIdent consumes the current rune and all contiguous ident runes.
func (s *scanner) scanIdent(chp ...rune) (tok Token, lit string, val interface{}) {

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
		} else if isIdentChar(ch) {
			buf.WriteRune(ch)
		} else {
			s.undo()
			break
		}
	}

	// If the string matches a keyword then return that keyword.
	if tok := keywords[strings.ToUpper(buf.String())]; tok > 0 {
		return tok, buf.String(), val
	}

	if val, err := time.ParseDuration(buf.String()); err == nil {
		return DURATION, buf.String(), val
	}

	// Otherwise return as a regular identifier.
	return tok, buf.String(), val

}

// scanIdiom consumes the current rune and all contiguous ident runes.
func (s *scanner) scanIdiom(chp ...rune) (tok Token, lit string, val interface{}) {

	tok = IDENT

	var tbv string
	var idv interface{}

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
		} else if isIdentChar(ch) {
			buf.WriteRune(ch)
		} else if isExprsChar(ch) {
			tok = EXPR
			buf.WriteRune(ch)
		} else if ch == ':' {

			if tok == EXPR {
				s.undo()
				break
			}

			tbv = buf.String()

			buf.WriteRune(ch)

			if tok, lit, idv = s.part(); tok == ILLEGAL {
				buf.WriteString(lit)
				return ILLEGAL, buf.String(), val
			} else {
				buf.WriteString(lit)
			}

			return THING, buf.String(), NewThing(tbv, idv)

		} else {
			s.undo()
			break
		}
	}

	// If the string matches a keyword then return that keyword.
	if tok := keywords[strings.ToUpper(buf.String())]; tok > 0 {
		return tok, buf.String(), val
	}

	if val, err := time.ParseDuration(buf.String()); err == nil {
		return DURATION, buf.String(), val
	}

	// Otherwise return as a regular identifier.
	return tok, buf.String(), val

}

// scanThing consumes the current rune and all contiguous ident runes.
func (s *scanner) scanThing(chp ...rune) (tok Token, lit string, val interface{}) {

	tok = THING

	var tbv string
	var idv interface{}

	// Create a buffer
	var buf bytes.Buffer

	// Read passed in runes
	for _, ch := range chp {
		buf.WriteRune(ch)
	}

	if tok, tbv, _ = s.part(); tok == ILLEGAL {
		buf.WriteString(tbv)
		return ILLEGAL, buf.String(), val
	} else {
		buf.WriteString(tbv)
	}

	if ch := s.next(); ch == ':' {
		buf.WriteRune(ch)
	} else {
		return ILLEGAL, buf.String(), val
	}

	if tok, lit, idv = s.part(); tok == ILLEGAL {
		buf.WriteString(lit)
		return ILLEGAL, buf.String(), val
	} else {
		buf.WriteString(lit)
	}

	return THING, buf.String(), NewThing(tbv, idv)

}

func (s *scanner) scanModel(chp ...rune) (tok Token, lit string, val interface{}) {

	var com bool
	var dot bool

	var tbv string
	var min float64 = 0
	var inc float64 = 0
	var max float64 = 0

	// Create a buffer
	var buf bytes.Buffer

	// Read passed in runes
	for _, ch := range chp {
		buf.WriteRune(ch)
	}

	if tok, tbv, _ = s.part(); tok == ILLEGAL {
		buf.WriteString(tbv)
		return ILLEGAL, buf.String(), val
	} else {
		buf.WriteString(tbv)
	}

	if ch := s.next(); ch == ':' {
		buf.WriteRune(ch)
	} else {
		return ILLEGAL, buf.String(), val
	}

	if ch := s.next(); isSignal(ch) {
		tok, lit, _ = s.scanSignal(ch)
		buf.WriteString(lit)
		max, _ = strconv.ParseFloat(lit, 64)
	} else {
		return ILLEGAL, buf.String(), val
	}

	if ch := s.next(); ch == ',' {
		com = true
		buf.WriteRune(ch)
		if ch := s.next(); isSignal(ch) {
			tok, lit, _ = s.scanSignal(ch)
			buf.WriteString(lit)
			inc, _ = strconv.ParseFloat(lit, 64)
		} else {
			return ILLEGAL, buf.String(), val
		}
	} else {
		s.undo()
	}

	if ch := s.next(); ch == '.' {
		dot = true
		buf.WriteRune(ch)
		if ch := s.next(); ch == '.' {
			buf.WriteRune(ch)
			if ch := s.next(); isSignal(ch) {
				tok, lit, _ = s.scanSignal(ch)
				buf.WriteString(lit)
				min = max
				max, _ = strconv.ParseFloat(lit, 64)
			} else {
				return ILLEGAL, buf.String(), val
			}
		} else {
			return ILLEGAL, buf.String(), val
		}
	} else {
		s.undo()
	}

	if ch := s.next(); ch == '|' {
		buf.WriteRune(ch)
	} else {
		return ILLEGAL, buf.String(), val
	}

	// If the minimum value is the
	// same as the maximum value then
	// error, as there is no ability
	// to increment or decrement.

	if min == max {
		return ILLEGAL, buf.String(), val
	}

	// If we have a comma, but the
	// value is below zero, we will
	// error as this will cause an
	// infinite loop in db.

	if com == true && inc <= 0 {
		return ILLEGAL, buf.String(), val
	}

	// If we have a min, and a max
	// with .. notation, but no `inc`
	// is specified, set the `inc` to
	// a default of `1`.

	if dot == true && inc <= 0 {
		inc = 1
	}

	// If we have a comma, but no
	// max value is specified then
	// error, as we need a max with
	// incrementing integer ids.

	if com == true && dot == false {
		return ILLEGAL, buf.String(), val
	}

	return MODEL, buf.String(), NewModel(tbv, min, inc, max)

}

func (s *scanner) scanSignal(chp ...rune) (tok Token, lit string, val interface{}) {

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
		} else if ch == '.' {
			if s.next() == '.' {
				s.undo()
				s.undo()
				break
			} else {
				s.undo()
				buf.WriteRune(ch)
			}
		} else {
			s.undo()
			break
		}
	}

	return NUMBER, buf.String(), nil

}

func (s *scanner) scanNumber(chp ...rune) (tok Token, lit string, val interface{}) {

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
			if tok == NUMBER || tok == DOUBLE {
				tok = IDENT
				buf.WriteRune(ch)
				switch ch {
				case 'e', 'E':
					if chn := s.next(); chn == '+' {
						tok = DOUBLE
						buf.WriteRune(chn)
					} else if ch == '-' {
						tok = DOUBLE
						buf.WriteRune(chn)
					} else {
						s.undo()
					}
				case 's', 'h', 'd', 'w':
					tok = DURATION
				case 'n', 'u', 'µ', 'm':
					if chn := s.next(); chn == 's' {
						tok = DURATION
						buf.WriteRune(chn)
					} else if ch == 'm' {
						tok = DURATION
						s.undo()
					} else {
						s.undo()
					}
				}
			} else {
				tok = IDENT
				buf.WriteRune(ch)
			}
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

	return tok, buf.String(), nil

}

func (s *scanner) scanString(chp ...rune) (tok Token, lit string, val interface{}) {

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
			return ILLEGAL, buf.String(), val
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

	if val, err := time.Parse(RFCDate, buf.String()); err == nil {
		return DATE, buf.String(), val.UTC()
	}

	if val, err := time.Parse(RFCTime, buf.String()); err == nil {
		return TIME, buf.String(), val.UTC()
	}

	return tok, buf.String(), val

}

func (s *scanner) scanRegexp(chp ...rune) (tok Token, lit string, val interface{}) {

	tok = IDENT

	// Create a buffer
	var buf bytes.Buffer

	// Ignore passed in runes

	// Read subsequent characters
	for {
		if ch := s.next(); ch == chp[0] {
			break
		} else if ch == eof {
			return ILLEGAL, buf.String(), val
		} else if ch == '\\' {
			chn := s.next()
			buf.WriteRune(ch)
			buf.WriteRune(chn)
		} else {
			buf.WriteRune(ch)
		}
	}

	if val, err := regexp.Compile(buf.String()); err == nil {
		return REGEX, buf.String(), val
	}

	return tok, buf.String(), val

}

func (s *scanner) scanObject(chp ...rune) (tok Token, lit string, val interface{}) {

	beg := chp[0]
	end := beg
	sub := 0
	qut := 0

	if beg == '{' {
		end = '}'
		tok = JSON
	}

	if beg == '[' {
		end = ']'
		tok = ARRAY
	}

	// Create a buffer
	var buf bytes.Buffer

	// Read passed in runes
	for _, ch := range chp {
		buf.WriteRune(ch)
	}

	// Read subsequent characters
	for {
		if ch := s.next(); ch == end && sub == 0 && qut == 0 {
			buf.WriteRune(ch)
			break
		} else if ch == beg {
			sub++
			buf.WriteRune(ch)
		} else if ch == end {
			sub--
			buf.WriteRune(ch)
		} else if ch == eof {
			return ILLEGAL, buf.String(), val
		} else if ch == '"' {
			if qut == 0 {
				qut++
			} else {
				qut--
			}
			buf.WriteRune(ch)
		} else if ch == '\\' {
			switch chn := s.next(); chn {
			default:
				return ILLEGAL, buf.String(), val
			case 'b', 't', 'r', 'n', 'f', '"', '\\':
				buf.WriteRune(ch)
				buf.WriteRune(chn)
			}
		} else {
			buf.WriteRune(ch)
		}
	}

	return tok, buf.String(), val

}

func (s *scanner) part() (tok Token, lit string, val interface{}) {

	if ch := s.next(); isLetter(ch) {
		tok, lit, _ = s.scanIdent(ch)
	} else if isNumber(ch) {
		tok, lit, _ = s.scanNumber(ch)
	} else if ch == '`' {
		tok, lit, _ = s.scanSection(ch)
	} else if ch == '⟨' {
		tok, lit, _ = s.scanSection(ch)
	} else {
		s.undo()
		tok = ILLEGAL
	}

	if tok != IDENT && tok != NUMBER && tok != DOUBLE {
		tok = ILLEGAL
	}

	if val == nil {
		val = lit
	}

	return

}

// next reads the next rune from the bufferred reader.
// Returns the rune(0) if an error occurs (or io.EOF is returned).
func (s *scanner) next() rune {

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
func (s *scanner) undo() {

	if len(s.b) > 0 {
		var r rune
		r, s.b = s.b[len(s.b)-1], s.b[:len(s.b)-1]
		s.a = append(s.a, r)
		return
	}

	_ = s.r.UnreadRune()

}

// isBlank returns true if the rune is a space, tab, or newline.
func isBlank(ch rune) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

// isNumber returns true if the rune is a number.
func isNumber(ch rune) bool {
	return (ch >= '0' && ch <= '9')
}

// isSignal returns true if the rune is a number.
func isSignal(ch rune) bool {
	return (ch >= '0' && ch <= '9') || ch == '-' || ch == '+'
}

// isLetter returns true if the rune is a letter.
func isLetter(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == 'µ'
}

// isIdentChar returns true if the rune is allowed in a IDENT.
func isIdentChar(ch rune) bool {
	return isLetter(ch) || isNumber(ch) || ch == '_'
}

// isThingChar returns true if the rune is allowed in a THING.
func isThingChar(ch rune) bool {
	return isLetter(ch) || isNumber(ch) || ch == '_'
}

// isExprsChar returns true if the rune is allowed in a IDENT.
func isExprsChar(ch rune) bool {
	return isLetter(ch) || isNumber(ch) || ch == '.' || ch == '_' || ch == '*' || ch == '[' || ch == ']'
}

// eof represents a marker rune for the end of the reader.
var eof = rune(0)
