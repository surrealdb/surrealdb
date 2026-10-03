//! Unit and integration tests for PostgreSQL clean-room lexer.

use surrealdb_pgsql::lexer::Lexer;
use surrealdb_pgsql::lexer::keyword::Keyword;
use surrealdb_pgsql::lexer::split::split_statements;
use surrealdb_pgsql::lexer::token::TokenKind;

fn lex_all(sql: &str) -> Vec<TokenKind<'_>> {
	let mut lexer = Lexer::new(sql);
	let mut tokens = Vec::new();
	loop {
		let tok = lexer.next_token().expect("lexing must succeed");
		if tok.is_eof() {
			break;
		}
		tokens.push(tok.kind);
	}
	tokens
}

#[test]
fn test_keywords_and_case_folding() {
	let tokens = lex_all("SELECT from WHERE foo BAR _test");
	assert_eq!(
		tokens,
		vec![
			TokenKind::Keyword(Keyword::Select),
			TokenKind::Keyword(Keyword::From),
			TokenKind::Keyword(Keyword::Where),
			TokenKind::Ident("foo".into()),
			TokenKind::Ident("bar".into()), // unquoted identifiers are lowercased
			TokenKind::Ident("_test".into()),
		]
	);
}

#[test]
fn test_quoted_identifiers() {
	let tokens = lex_all(r#" "MixedCase" "has""quote" "" "#);
	assert_eq!(
		tokens,
		vec![
			TokenKind::QuotedIdent("MixedCase".to_string()),
			TokenKind::QuotedIdent("has\"quote".to_string()),
			TokenKind::QuotedIdent("".to_string()),
		]
	);
}

#[test]
fn test_string_literals() {
	let tokens = lex_all("'simple' 'with''quote' ''");
	assert_eq!(
		tokens,
		vec![
			TokenKind::StringConstant("simple".to_string()),
			TokenKind::StringConstant("with'quote".to_string()),
			TokenKind::StringConstant("".to_string()),
		]
	);
}

#[test]
fn test_escape_strings() {
	let tokens = lex_all(r#" E'hello\nworld' e'tab\there' E'hex\x41' "#);
	assert_eq!(
		tokens,
		vec![
			TokenKind::StringConstant("hello\nworld".to_string()),
			TokenKind::StringConstant("tab\there".to_string()),
			TokenKind::StringConstant("hexA".to_string()),
		]
	);
}

#[test]
fn test_dollar_quoted_strings() {
	let tokens = lex_all("$$plain $$ $tag$tagged string; with 'quotes' and --comments$tag$");
	assert_eq!(
		tokens,
		vec![
			TokenKind::DollarQuotedString {
				tag: "",
				content: "plain ",
			},
			TokenKind::DollarQuotedString {
				tag: "tag",
				content: "tagged string; with 'quotes' and --comments",
			},
		]
	);
}

#[test]
fn test_bit_strings() {
	let tokens = lex_all("B'10101' X'DEADBEEF' b'0' x'ff'");
	assert_eq!(
		tokens,
		vec![
			TokenKind::BitString {
				is_hex: false,
				content: "10101",
			},
			TokenKind::BitString {
				is_hex: true,
				content: "DEADBEEF",
			},
			TokenKind::BitString {
				is_hex: false,
				content: "0",
			},
			TokenKind::BitString {
				is_hex: true,
				content: "ff",
			},
		]
	);
}

#[test]
fn test_numeric_literals() {
	let tokens = lex_all("42 0x2A 0o52 0b101010 12.34 .5 1e-4 1_000_000");
	assert_eq!(
		tokens,
		vec![
			TokenKind::Integer(42),
			TokenKind::IntegerRaw("0x2A"),
			TokenKind::IntegerRaw("0o52"),
			TokenKind::IntegerRaw("0b101010"),
			TokenKind::Float(12.34),
			TokenKind::Float(0.5),
			TokenKind::Float(0.0001),
			TokenKind::Integer(1_000_000),
		]
	);
}

#[test]
fn test_parameters() {
	let tokens = lex_all("$1 $2 $42");
	assert_eq!(tokens, vec![TokenKind::Param(1), TokenKind::Param(2), TokenKind::Param(42)]);
}

#[test]
fn test_comments() {
	let tokens =
		lex_all("1 /* block comment */ + /* outer /* nested */ outer */ 2 -- line comment\n * 3");
	assert_eq!(
		tokens,
		vec![
			TokenKind::Integer(1),
			TokenKind::Operator("+"),
			TokenKind::Integer(2),
			TokenKind::Operator("*"),
			TokenKind::Integer(3),
		]
	);
}

#[test]
fn test_statement_splitter() {
	let sql = "SELECT 1; INSERT INTO t VALUES ('a;b'); SELECT $$c;d$$; /* comment; */ SELECT 4";
	let stmts = split_statements(sql).expect("statement splitting must succeed");
	let texts: Vec<&str> = stmts.iter().map(|s| s.text).collect();
	assert_eq!(
		texts,
		vec![
			"SELECT 1",
			"INSERT INTO t VALUES ('a;b')",
			"SELECT $$c;d$$",
			"/* comment; */ SELECT 4",
		]
	);
}
