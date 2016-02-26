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

// Token defines a lexical token
type Token int

const (

	// special

	ILLEGAL Token = iota
	EOF
	WS

	// literals

	literalsBeg

	DATE     // 1970-01-01
	TIME     // 1970-01-01T00:00:00+00:00
	NANO     // 1970-01-01T00:00:00.000000000+00:00
	PATH     // :friend
	JSON     // {"test":true}
	IDENT    // something
	STRING   // "something"
	REGION   // "a multiline \n string"
	NUMBER   // 123456
	DOUBLE   // 123.456
	REGEX    // /.*/
	DURATION // 13h

	EAT       // @
	DOT       // .
	COMMA     // ,
	LPAREN    // (
	RPAREN    // )
	LBRACK    // [
	RBRACK    // ]
	COLON     // :
	SEMICOLON // ;

	literalsEnd

	// operators

	operatorBeg

	ADD // +
	SUB // -
	MUL // *
	DIV // /
	INC // +=
	DEC // -=

	EQ  // =
	NEQ // !=
	LT  // <
	LTE // <=
	GT  // >
	GTE // >=
	EQR // =~
	NER // !~
	SEQ // ∋
	SNE // ∌

	operatorEnd

	// literals

	keywordsBeg

	ALL
	AND
	AS
	ASC
	AT
	BY
	COLUMNS
	CREATE
	DEFINE
	DELETE
	DESC
	DISTINCT
	EMPTY
	FALSE
	FROM
	GROUP
	IN
	INDEX
	INSERT
	INTO
	LIMIT
	MAP
	MODIFY
	NULL
	OFFSET
	ON
	OR
	ORDER
	RECORD
	REDUCE
	RELATE
	REMOVE
	RESYNC
	SELECT
	SET
	START
	TO
	TRUE
	UNIQUE
	UPDATE
	UPSERT
	VERSION
	VIEW
	WHERE

	keywordsEnd
)

var tokens = [...]string{

	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",
	WS:      "WS",

	// literals

	DATE:     "DATE",
	TIME:     "TIME",
	NANO:     "NANO",
	PATH:     "PATH",
	JSON:     "JSON",
	IDENT:    "IDENT",
	STRING:   "STRING",
	REGION:   "REGION",
	NUMBER:   "NUMBER",
	DOUBLE:   "DOUBLE",
	REGEX:    "REGEX",
	DURATION: "DURATION",

	EAT:       "@",
	DOT:       ".",
	COMMA:     ",",
	LPAREN:    "(",
	RPAREN:    ")",
	LBRACK:    "[",
	RBRACK:    "]",
	COLON:     ":",
	SEMICOLON: ";",

	// operators

	ADD: "+",
	SUB: "-",
	MUL: "*",
	DIV: "/",
	INC: "+=",
	DEC: "-=",

	EQ:  "=",
	NEQ: "!=",
	LT:  "<",
	LTE: "<=",
	GT:  ">",
	GTE: ">=",
	EQR: "=~",
	NER: "!~",
	SEQ: "∋",
	SNE: "∌",

	// keywords

	ALL:      "ALL",
	AND:      "AND",
	AS:       "AS",
	ASC:      "ASC",
	AT:       "AT",
	BY:       "BY",
	COLUMNS:  "COLUMNS",
	CREATE:   "CREATE",
	DEFINE:   "DEFINE",
	DELETE:   "DELETE",
	DESC:     "DESC",
	DISTINCT: "DISTINCT",
	EMPTY:    "EMPTY",
	FALSE:    "FALSE",
	FROM:     "FROM",
	GROUP:    "GROUP",
	IN:       "IN",
	INDEX:    "INDEX",
	INSERT:   "INSERT",
	INTO:     "INTO",
	LIMIT:    "LIMIT",
	MAP:      "MAP",
	MODIFY:   "MODIFY",
	NULL:     "NULL",
	ON:       "ON",
	OR:       "OR",
	ORDER:    "ORDER",
	RECORD:   "RECORD",
	REDUCE:   "REDUCE",
	RELATE:   "RELATE",
	REMOVE:   "REMOVE",
	RESYNC:   "RESYNC",
	SELECT:   "SELECT",
	SET:      "SET",
	START:    "START",
	TO:       "TO",
	TRUE:     "TRUE",
	UNIQUE:   "UNIQUE",
	UPDATE:   "UPDATE",
	UPSERT:   "UPSERT",
	VIEW:     "VIEW",
	VERSION:  "VERSION",
	WHERE:    "WHERE",
}

var literals map[string]Token
var operator map[string]Token
var keywords map[string]Token

func init() {

	literals = make(map[string]Token)
	for tok := literalsBeg + 1; tok < literalsEnd; tok++ {
		literals[tokens[tok]] = tok
	}

	operator = make(map[string]Token)
	for tok := operatorBeg + 1; tok < operatorEnd; tok++ {
		operator[tokens[tok]] = tok
	}

	keywords = make(map[string]Token)
	for tok := keywordsBeg + 1; tok < keywordsEnd; tok++ {
		keywords[tokens[tok]] = tok
	}

}

func lookup(lookups []Token) (literals []string) {
	for _, token := range lookups {
		literals = append(literals, token.String())
	}
	return
}

func (tok Token) precedence() int {

	switch tok {
	case OR:
		return 1
	case AND:
		return 2
	case EQ, NEQ, EQR, NER, LT, LTE, GT, GTE:
		return 3
	case ADD, SUB:
		return 4
	case MUL, DIV:
		return 5
	}

	return 0

}

func (tok Token) String() string {
	if tok >= 0 && tok < Token(len(tokens)) {
		return tokens[tok]
	}
	return ""
}

func (tok Token) isLiteral() bool { return tok > literalsBeg && tok < literalsEnd }

func (tok Token) isKeyword() bool { return tok > keywordsBeg && tok < keywordsEnd }

func (tok Token) isOperator() bool { return tok > operatorBeg && tok < operatorEnd }
