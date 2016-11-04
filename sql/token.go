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
	IDENT    // something
	THING    // @class:id
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

	operatorEnd

	// literals

	keywordsBeg

	ACCEPT
	AFTER
	ALL
	ALLCONTAINEDIN
	AND
	AS
	ASC
	AT
	BEFORE
	BEGIN
	BOTH
	BY
	CANCEL
	CODE
	COLUMNS
	COMMIT
	CONTAINS
	CONTAINSALL
	CONTAINSNONE
	CONTAINSSOME
	CONTENT
	CREATE
	CUSTOM
	DATABASE
	DEFAULT
	DEFINE
	DELETE
	DESC
	DIFF
	DISTINCT
	EMPTY
	ENUM
	EXPUNGE
	FALSE
	FIELD
	FOR
	FROM
	GROUP
	HISTORY
	ID
	IF
	IN
	INDEX
	INFO
	INTO
	IS
	LET
	LIMIT
	LIVE
	MANDATORY
	MATCH
	MAX
	MERGE
	MIN
	MISSING
	MULTI
	NAMESPACE
	NONE
	NONECONTAINEDIN
	NOT
	NOTNULL
	NOW
	NULL
	OFFSET
	ON
	OR
	ORDER
	POLICY
	READONLY
	REJECT
	RELATE
	REMOVE
	RETURN
	RULES
	SCHEMAFULL
	SCHEMALESS
	SCOPE
	SELECT
	SESSION
	SET
	SIGNIN
	SIGNUP
	SOMECONTAINEDIN
	START
	TABLE
	TO
	TRANSACTION
	TRUE
	TYPE
	UNIQUE
	UPDATE
	UPSERT
	USE
	VALIDATE
	VERSION
	VIEW
	VOID
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
	JSON:     "JSON",
	IDENT:    "IDENT",
	THING:    "THING",
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

	// keywords

	ACCEPT:          "ACCEPT",
	AFTER:           "AFTER",
	ALL:             "ALL",
	ALLCONTAINEDIN:  "ALLCONTAINEDIN",
	AND:             "AND",
	AS:              "AS",
	ASC:             "ASC",
	AT:              "AT",
	BEFORE:          "BEFORE",
	BEGIN:           "BEGIN",
	BOTH:            "BOTH",
	BY:              "BY",
	CANCEL:          "CANCEL",
	CODE:            "CODE",
	COLUMNS:         "COLUMNS",
	COMMIT:          "COMMIT",
	CONTAINS:        "CONTAINS",
	CONTAINSALL:     "CONTAINSALL",
	CONTAINSNONE:    "CONTAINSNONE",
	CONTAINSSOME:    "CONTAINSSOME",
	CONTENT:         "CONTENT",
	CREATE:          "CREATE",
	CUSTOM:          "CUSTOM",
	DATABASE:        "DATABASE",
	DEFAULT:         "DEFAULT",
	DEFINE:          "DEFINE",
	DELETE:          "DELETE",
	DESC:            "DESC",
	DIFF:            "DIFF",
	DISTINCT:        "DISTINCT",
	EMPTY:           "EMPTY",
	ENUM:            "ENUM",
	EXPUNGE:         "EXPUNGE",
	FALSE:           "FALSE",
	FIELD:           "FIELD",
	FOR:             "FOR",
	FROM:            "FROM",
	GROUP:           "GROUP",
	HISTORY:         "HISTORY",
	ID:              "ID",
	IF:              "IF",
	IN:              "IN",
	INDEX:           "INDEX",
	INFO:            "INFO",
	INTO:            "INTO",
	IS:              "IS",
	LET:             "LET",
	LIMIT:           "LIMIT",
	LIVE:            "LIVE",
	MANDATORY:       "MANDATORY",
	MATCH:           "MATCH",
	MAX:             "MAX",
	MERGE:           "MERGE",
	MIN:             "MIN",
	MISSING:         "MISSING",
	MULTI:           "MULTI",
	NAMESPACE:       "NAMESPACE",
	NONE:            "NONE",
	NONECONTAINEDIN: "NONECONTAINEDIN",
	NOT:             "NOT",
	NOTNULL:         "NOTNULL",
	NOW:             "NOW",
	NULL:            "NULL",
	ON:              "ON",
	OR:              "OR",
	ORDER:           "ORDER",
	POLICY:          "POLICY",
	READONLY:        "READONLY",
	REJECT:          "REJECT",
	RELATE:          "RELATE",
	REMOVE:          "REMOVE",
	RETURN:          "RETURN",
	RULES:           "RULES",
	SCHEMAFULL:      "SCHEMAFULL",
	SCHEMALESS:      "SCHEMALESS",
	SCOPE:           "SCOPE",
	SELECT:          "SELECT",
	SESSION:         "SESSION",
	SET:             "SET",
	SIGNIN:          "SIGNIN",
	SIGNUP:          "SIGNUP",
	SOMECONTAINEDIN: "SOMECONTAINEDIN",
	START:           "START",
	TABLE:           "TABLE",
	TO:              "TO",
	TRANSACTION:     "TRANSACTION",
	TRUE:            "TRUE",
	TYPE:            "TYPE",
	UNIQUE:          "UNIQUE",
	UPDATE:          "UPDATE",
	UPSERT:          "UPSERT",
	USE:             "USE",
	VALIDATE:        "VALIDATE",
	VERSION:         "VERSION",
	VIEW:            "VIEW",
	VOID:            "VOID",
	WHERE:           "WHERE",
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
	case EQ, NEQ, EEQ, NEE,
		LT, LTE, GT, GTE,
		ANY, SIN, SNI, INS, NIS,
		CONTAINSALL, CONTAINSNONE, CONTAINSSOME,
		ALLCONTAINEDIN, NONECONTAINEDIN, SOMECONTAINEDIN:
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

}

func (tok Token) isLiteral() bool { return tok > literalsBeg && tok < literalsEnd }

func (tok Token) isKeyword() bool { return tok > keywordsBeg && tok < keywordsEnd }

func (tok Token) isOperator() bool { return tok > operatorBeg && tok < operatorEnd }
