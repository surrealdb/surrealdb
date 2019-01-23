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

import "strings"

// Token defines a lexical token
type Token int16

const (

	// special

	ILLEGAL Token = iota
	EOF
	WS

	// literals

	literalsBeg

	DATE     // 1970-01-01
	TIME     // 1970-01-01T00:00:00+00:00
	JSON     // {"test":true}
	EXPR     // something[0].value
	IDENT    // something
	THING    // @class:id
	MODEL    // [person|1..1000]
	STRING   // "something"
	REGION   // "a multiline \n string"
	NUMBER   // 123456
	DOUBLE   // 123.456
	REGEX    // /.*/
	ARRAY    // [0,1,2]
	DURATION // 13h
	PARAM    // $1

	OEDGE // ->
	IEDGE // <-
	BEDGE // <->

	DOT       // .
	COMMA     // ,
	QMARK     // ?
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
	EEQ // ==
	EXC // !
	NEQ // !=
	NEE // !==
	ANY // ?=
	LT  // <
	LTE // <=
	GT  // >
	GTE // >=
	SIN // ∋
	SNI // ∌
	INS // ∈
	NIS // ∉
	MAT // ~
	NAT // !~
	MAY // ?~

	operatorEnd

	// keywords

	keywordsBeg

	AFTER
	ALL
	ALLCONTAINEDIN
	AND
	AS
	ASC
	ASSERT
	AT
	BEFORE
	BEGIN
	BOTH
	BY
	CANCEL
	COLLATE
	COLUMNS
	COMMIT
	CONNECT
	CONTAINS
	CONTAINSALL
	CONTAINSNONE
	CONTAINSSOME
	CONTENT
	CREATE
	DATABASE
	DB
	DEFINE
	DELETE
	DESC
	DIFF
	DROP
	ELSE
	EMPTY
	END
	EVENT
	FALSE
	FETCH
	FIELD
	FOR
	FROM
	FULL
	GROUP
	IF
	IN
	INDEX
	INFO
	INSERT
	INTO
	IS
	KILL
	LET
	LIMIT
	LIVE
	LOGIN
	MERGE
	MISSING
	NAMESPACE
	NONE
	NONECONTAINEDIN
	NOT
	NS
	NULL
	NUMERIC
	ON
	OPTION
	OR
	ORDER
	PARALLEL
	PASSHASH
	PASSWORD
	PERMISSIONS
	PRIORITY
	RAND
	RELATE
	REMOVE
	RETURN
	RUN
	SCHEMAFULL
	SCHEMALESS
	SCOPE
	SELECT
	SESSION
	SET
	SIGNIN
	SIGNUP
	SOMECONTAINEDIN
	SPLIT
	START
	TABLE
	THEN
	TIMEOUT
	TO
	TOKEN
	TRANSACTION
	TRUE
	TYPE
	UNIQUE
	UNVERSIONED
	UPDATE
	UPSERT
	USE
	VALUE
	VERSION
	VERSIONED
	VOID
	WHEN
	WHERE
	WITH

	keywordsEnd
)

var tokens = [...]string{

	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",
	WS:      "WS",

	// literals

	DATE:     "DATE",
	TIME:     "TIME",
	JSON:     "JSON",
	EXPR:     "EXPR",
	IDENT:    "IDENT",
	THING:    "THING",
	MODEL:    "MODEL",
	STRING:   "STRING",
	REGION:   "REGION",
	NUMBER:   "NUMBER",
	DOUBLE:   "DOUBLE",
	REGEX:    "REGEX",
	ARRAY:    "ARRAY",
	DURATION: "DURATION",
	PARAM:    "PARAM",

	OEDGE: "->",
	IEDGE: "<-",
	BEDGE: "<->",

	DOT:       ".",
	COMMA:     ",",
	QMARK:     "?",
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
	EEQ: "==",
	EXC: "!",
	NEQ: "!=",
	NEE: "!==",
	ANY: "?=",
	LT:  "<",
	LTE: "<=",
	GT:  ">",
	GTE: ">=",
	SIN: "∋",
	SNI: "∌",
	INS: "∈",
	NIS: "∉",
	MAT: "~",
	NAT: "!~",
	MAY: "?~",

	// keywords

	AFTER:           "AFTER",
	ALL:             "ALL",
	ALLCONTAINEDIN:  "ALLCONTAINEDIN",
	AND:             "AND",
	AS:              "AS",
	ASC:             "ASC",
	ASSERT:          "ASSERT",
	AT:              "AT",
	BEFORE:          "BEFORE",
	BEGIN:           "BEGIN",
	BOTH:            "BOTH",
	BY:              "BY",
	CANCEL:          "CANCEL",
	COLLATE:         "COLLATE",
	COLUMNS:         "COLUMNS",
	COMMIT:          "COMMIT",
	CONNECT:         "CONNECT",
	CONTAINS:        "CONTAINS",
	CONTAINSALL:     "CONTAINSALL",
	CONTAINSNONE:    "CONTAINSNONE",
	CONTAINSSOME:    "CONTAINSSOME",
	CONTENT:         "CONTENT",
	CREATE:          "CREATE",
	DATABASE:        "DATABASE",
	DB:              "DB",
	DEFINE:          "DEFINE",
	DELETE:          "DELETE",
	DESC:            "DESC",
	DIFF:            "DIFF",
	DROP:            "DROP",
	ELSE:            "ELSE",
	EMPTY:           "EMPTY",
	END:             "END",
	EVENT:           "EVENT",
	FALSE:           "FALSE",
	FETCH:           "FETCH",
	FIELD:           "FIELD",
	FOR:             "FOR",
	FROM:            "FROM",
	FULL:            "FULL",
	GROUP:           "GROUP",
	IF:              "IF",
	IN:              "IN",
	INDEX:           "INDEX",
	INFO:            "INFO",
	INSERT:          "INSERT",
	INTO:            "INTO",
	IS:              "IS",
	KILL:            "KILL",
	LET:             "LET",
	LIMIT:           "LIMIT",
	LIVE:            "LIVE",
	LOGIN:           "LOGIN",
	MERGE:           "MERGE",
	MISSING:         "MISSING",
	NAMESPACE:       "NAMESPACE",
	NONE:            "NONE",
	NONECONTAINEDIN: "NONECONTAINEDIN",
	NOT:             "NOT",
	NS:              "NS",
	NULL:            "NULL",
	NUMERIC:         "NUMERIC",
	ON:              "ON",
	OPTION:          "OPTION",
	OR:              "OR",
	ORDER:           "ORDER",
	PARALLEL:        "PARALLEL",
	PASSHASH:        "PASSHASH",
	PASSWORD:        "PASSWORD",
	PERMISSIONS:     "PERMISSIONS",
	PRIORITY:        "PRIORITY",
	RAND:            "RAND",
	RELATE:          "RELATE",
	REMOVE:          "REMOVE",
	RETURN:          "RETURN",
	RUN:             "RUN",
	SCHEMAFULL:      "SCHEMAFULL",
	SCHEMALESS:      "SCHEMALESS",
	SCOPE:           "SCOPE",
	SELECT:          "SELECT",
	SESSION:         "SESSION",
	SET:             "SET",
	SIGNIN:          "SIGNIN",
	SIGNUP:          "SIGNUP",
	SOMECONTAINEDIN: "SOMECONTAINEDIN",
	SPLIT:           "SPLIT",
	START:           "START",
	TABLE:           "TABLE",
	THEN:            "THEN",
	TIMEOUT:         "TIMEOUT",
	TO:              "TO",
	TOKEN:           "TOKEN",
	TRANSACTION:     "TRANSACTION",
	TRUE:            "TRUE",
	TYPE:            "TYPE",
	UNIQUE:          "UNIQUE",
	UNVERSIONED:     "UNVERSIONED",
	UPDATE:          "UPDATE",
	UPSERT:          "UPSERT",
	USE:             "USE",
	VALUE:           "VALUE",
	VERSION:         "VERSION",
	VERSIONED:       "VERSIONED",
	VOID:            "VOID",
	WHEN:            "WHEN",
	WHERE:           "WHERE",
	WITH:            "WITH",
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
	case OR, AND:
		return 1
	case EQ, NEQ, EEQ, NEE,
		LT, LTE, GT, GTE,
		ANY, SIN, SNI, INS, NIS, MAT, NAT, MAY,
		CONTAINSALL, CONTAINSNONE, CONTAINSSOME,
		ALLCONTAINEDIN, NONECONTAINEDIN, SOMECONTAINEDIN:
		return 2
	case ADD, SUB:
		return 3
	case MUL, DIV:
		return 4
	}

	return 0

}

func newToken(s string) Token {
	for k, v := range tokens {
		if len(v) == len(s) {
			if strings.EqualFold(v, s) {
				return Token(k)
			}
		}
	}
	return ILLEGAL
}

func (tok Token) String() string {
	if tok >= 0 && tok < Token(len(tokens)) {
		return tokens[tok]
	}
	return ""
}

func (this Token) MarshalText() (data []byte, err error) {
	return []byte(this.String()), err
}

func (this Token) MarshalBinary() (data []byte, err error) {
	return []byte(this.String()), err
}

func (this *Token) UnmarshalBinary(data []byte) (err error) {
	*this = newToken(string(data))
	return err
}

func (tok Token) isLiteral() bool { return tok > literalsBeg && tok < literalsEnd }

func (tok Token) isKeyword() bool { return tok > keywordsBeg && tok < keywordsEnd }

func (tok Token) isOperator() bool { return tok > operatorBeg && tok < operatorEnd }
