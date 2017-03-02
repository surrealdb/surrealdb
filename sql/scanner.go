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
		return s.scanIdent(ch)
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

	tok, lit, _ = s.scanIdent()

	if s.p.is(tok, REGION) {
		return ILLEGAL, lit, val
	}

	if s.p.is(tok, ILLEGAL) {
		return ILLEGAL, lit, val
	}

	return PARAM, lit, val

}

func (s *scanner) scanQuoted(chp ...rune) (tok Token, lit string, val interface{}) {

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
		} else if isExprsChar(ch) {
			tok = EXPR
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

// scanThing consumes the current rune and all contiguous ident runes.
func (s *scanner) scanThing(chp ...rune) (tok Token, lit string, val interface{}) {

	tok = THING

	// Store whether params
	var tbp bool
	var idp bool

	// Store section values
	var tbv string
	var idv interface{}

	// Create a buffer
	var buf bytes.Buffer
	var beg bytes.Buffer
	var mid bytes.Buffer
	var end bytes.Buffer

	// Read passed in runes
	for _, ch := range chp {
		buf.WriteRune(ch)
	}

	for {
		if ch := s.next(); ch == eof {
			break
		} else if isThingChar(ch) {
			tok, lit, _ = s.scanIdent(ch)
			beg.WriteString(lit)
			break
		} else if ch == '$' {
			tbp = true // The TB is a param
			tok, lit, _ = s.scanParams(ch)
			beg.WriteString(lit)
			break
		} else if ch == '`' {
			tok, lit, _ = s.scanQuoted(ch)
			beg.WriteString(lit)
			break
		} else if ch == '{' {
			tok, lit, _ = s.scanQuoted(ch)
			beg.WriteString(lit)
			break
		} else if ch == '⟨' {
			tok, lit, _ = s.scanQuoted(ch)
			beg.WriteString(lit)
			break
		} else {
			s.undo()
			break
		}
	}

	if beg.Len() < 1 || tok == ILLEGAL {
		return ILLEGAL, buf.String() + beg.String() + mid.String() + end.String(), val
	}

	for {
		if ch := s.next(); ch != ':' {
			s.undo()
			break
		} else {
			mid.WriteRune(ch)
			break
		}
	}

	if mid.Len() < 1 {
		return ILLEGAL, buf.String() + beg.String() + mid.String() + end.String(), val
	}

	for {
		if ch := s.next(); ch == eof {
			break
		} else if isThingChar(ch) {
			tok, lit, _ = s.scanIdent(ch)
			end.WriteString(lit)
			break
		} else if ch == '$' {
			idp = true // The ID is a param
			tok, lit, _ = s.scanParams(ch)
			end.WriteString(lit)
			break
		} else if ch == '`' {
			tok, lit, _ = s.scanQuoted(ch)
			end.WriteString(lit)
			break
		} else if ch == '{' {
			tok, lit, _ = s.scanQuoted(ch)
			end.WriteString(lit)
			break
		} else if ch == '⟨' {
			tok, lit, _ = s.scanQuoted(ch)
			end.WriteString(lit)
			break
		} else {
			s.undo()
			break
		}
	}

	if end.Len() < 1 || tok == ILLEGAL {
		return ILLEGAL, buf.String() + beg.String() + mid.String() + end.String(), val
	}

	tbv = beg.String()
	idv = end.String()

	if tbp { // The TB is a param
		if p, ok := s.p.v[tbv]; ok {
			switch v := p.(type) {
			case string:
				tbv = v
			default:
				return ILLEGAL, buf.String() + beg.String() + mid.String() + end.String(), val
			}
		} else {
			return ILLEGAL, buf.String() + beg.String() + mid.String() + end.String(), val
		}
	}

	if idp { // The ID is a param
		if p, ok := s.p.v[idv.(string)]; ok {
			switch v := p.(type) {
			case bool, int64, float64, string, []interface{}, map[string]interface{}:
				idv = v
			default:
				return ILLEGAL, buf.String() + beg.String() + mid.String() + end.String(), val
			}
		} else {
			return ILLEGAL, buf.String() + beg.String() + mid.String() + end.String(), val
		}
	}

	val = NewThing(tbv, idv)

	// Otherwise return as a regular thing.
	return THING, buf.String() + beg.String() + mid.String() + end.String(), val

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

	if beg == '{' {
		end = '}'
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

	if val, err := time.Parse(RFCNorm, buf.String()); err == nil {
		return TIME, buf.String(), val.UTC()
	}

	if val, err := time.Parse(RFCText, buf.String()); err == nil {
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
			return ILLEGAL, buf.String(), val
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

	if beg == '{' {
		return JSON, buf.String(), val
	}
	if beg == '[' {
		return ARRAY, buf.String(), val
	}

	return ILLEGAL, buf.String(), val

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
