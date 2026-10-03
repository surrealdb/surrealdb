#![forbid(unsafe_code)]
#![cfg_attr(
	not(test),
	deny(
		clippy::indexing_slicing,
		clippy::string_slice,
		clippy::panic,
		clippy::panic_in_result_fn,
		clippy::unreachable,
		clippy::todo,
		clippy::unimplemented,
		clippy::expect_used,
		clippy::unwrap_used,
		clippy::print_stdout,
		clippy::print_stderr,
		clippy::dbg_macro
	)
)]

//! PostgreSQL language front end for SurrealDB.
//!
//! Shared base for PostgreSQL 17, 18, and 19 grammar parsing, AST representation,
//! and faithful printing. Unrelated to `surrealdb-sql` (SurrealQL).

pub mod ast;
pub mod error;
pub mod lexer;
pub mod parser;
pub mod printer;
pub mod span;

pub use ast::*;
pub use error::{ErrorKind, ParseError, ParseResult};
pub use lexer::Lexer;
pub use lexer::keyword::{Keyword, KeywordCategory};
pub use lexer::split::{StatementBoundary, split_statements};
pub use lexer::token::{Token, TokenKind};
pub use parser::{MAX_RECURSION_DEPTH, Parser, ParserSettings};
pub use printer::ToPgSql;
pub use span::Span;

/// Parse a single PostgreSQL expression from string input with default settings.
pub fn parse_expr(src: &str) -> ParseResult<Expr> {
	let mut parser = Parser::new(src)?;
	parser.parse_expr()
}

/// Parse a single PostgreSQL expression from string input with custom settings.
pub fn parse_expr_with_settings(src: &str, settings: ParserSettings) -> ParseResult<Expr> {
	let mut parser = Parser::with_settings(src, settings)?;
	parser.parse_expr()
}

/// Parse a single PostgreSQL statement from string input with default settings.
pub fn parse_statement(src: &str) -> ParseResult<Statement> {
	let mut parser = Parser::new(src)?;
	parser.parse_statement()
}

/// Parse a single PostgreSQL statement from string input with custom settings.
pub fn parse_statement_with_settings(
	src: &str,
	settings: ParserSettings,
) -> ParseResult<Statement> {
	let mut parser = Parser::with_settings(src, settings)?;
	parser.parse_statement()
}

/// Parse multiple PostgreSQL statements from string input with default settings.
pub fn parse_statements(src: &str) -> ParseResult<Vec<Statement>> {
	let mut parser = Parser::new(src)?;
	parser.parse_statements()
}

/// Parse multiple PostgreSQL statements from string input with custom settings.
pub fn parse_statements_with_settings(
	src: &str,
	settings: ParserSettings,
) -> ParseResult<Vec<Statement>> {
	let mut parser = Parser::with_settings(src, settings)?;
	parser.parse_statements()
}
