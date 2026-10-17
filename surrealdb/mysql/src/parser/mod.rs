//! Recursive-descent and Pratt parser for MySQL SQL syntax.

pub mod error;
pub mod expr;
pub mod stmt;

use crate::ast::expr::Expr;
use crate::ast::stmt::{QualifiedName, Statement};
use crate::lexer::keyword::Keyword;
use crate::lexer::span::Span;
use crate::lexer::token::{Token, TokenKind};
use crate::lexer::tokenize_with_settings;
use crate::mode::SqlMode;
use crate::parser::error::{ErrorKind, ParseError, ParseResult};

pub const MAX_RECURSION_DEPTH: usize = 128;
pub const DEFAULT_MAX_INPUT_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_MAX_WORK_COUNT: usize = 100_000;

/// Configuration settings for MySQL parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParserSettings {
	pub max_recursion_depth: usize,
	pub max_input_bytes: usize,
	pub max_work_count: usize,
	pub sql_mode: SqlMode,
	pub server_version: u32,
}

impl Default for ParserSettings {
	fn default() -> Self {
		Self {
			max_recursion_depth: MAX_RECURSION_DEPTH,
			max_input_bytes: DEFAULT_MAX_INPUT_BYTES,
			max_work_count: DEFAULT_MAX_WORK_COUNT,
			sql_mode: SqlMode::default_mysql_8(),
			server_version: 80400,
		}
	}
}

impl ParserSettings {
	pub fn with_sql_mode(mut self, mode: SqlMode) -> Self {
		self.sql_mode = mode;
		self
	}

	pub fn with_server_version(mut self, version: u32) -> Self {
		self.server_version = version;
		self
	}
}

/// Parse a single MySQL statement from source string.
pub fn parse_statement(src: &str) -> ParseResult<Statement> {
	parse_statement_with_settings(src, ParserSettings::default())
}

/// Parse a single MySQL statement with custom settings.
pub fn parse_statement_with_settings(
	src: &str,
	settings: ParserSettings,
) -> ParseResult<Statement> {
	if src.len() > settings.max_input_bytes {
		return Err(ParseError::new(
			Span::empty(0),
			ErrorKind::InputSizeLimitExceeded {
				max: settings.max_input_bytes,
				actual: src.len(),
			},
		));
	}
	let tokens = tokenize_with_settings(src, settings)
		.map_err(|e| ParseError::new(Span::empty(0), ErrorKind::Lexer(e.to_string())))?;
	let mut parser = Parser::with_settings(&tokens, settings);
	let stmt = parser.parse_statement()?;
	while parser.peek().kind == TokenKind::Semicolon {
		parser.bump();
	}
	if parser.peek().kind != TokenKind::Eof {
		return Err(ParseError::new(
			parser.peek().span,
			ErrorKind::UnexpectedToken {
				expected: "end of statement",
				found: parser.peek().kind.to_string(),
			},
		));
	}
	Ok(stmt)
}

/// Parse multiple semicolon-separated MySQL statements from source string.
pub fn parse_statements(src: &str) -> ParseResult<Vec<Statement>> {
	parse_statements_with_settings(src, ParserSettings::default())
}

/// Parse multiple semicolon-separated MySQL statements with custom settings.
pub fn parse_statements_with_settings(
	src: &str,
	settings: ParserSettings,
) -> ParseResult<Vec<Statement>> {
	if src.len() > settings.max_input_bytes {
		return Err(ParseError::new(
			Span::empty(0),
			ErrorKind::InputSizeLimitExceeded {
				max: settings.max_input_bytes,
				actual: src.len(),
			},
		));
	}
	let tokens = tokenize_with_settings(src, settings)
		.map_err(|e| ParseError::new(Span::empty(0), ErrorKind::Lexer(e.to_string())))?;
	let mut parser = Parser::with_settings(&tokens, settings);
	let mut stmts = Vec::new();

	while parser.peek().kind != TokenKind::Eof {
		while parser.peek().kind == TokenKind::Semicolon {
			parser.bump();
		}
		if parser.peek().kind == TokenKind::Eof {
			break;
		}
		stmts.push(parser.parse_statement()?);
		if parser.peek().kind != TokenKind::Eof {
			if parser.peek().kind == TokenKind::Semicolon {
				parser.bump();
			} else {
				return Err(ParseError::new(
					parser.peek().span,
					ErrorKind::UnexpectedToken {
						expected: ";",
						found: parser.peek().kind.to_string(),
					},
				));
			}
		}
	}

	Ok(stmts)
}

/// Parse an isolated MySQL expression from source string.
pub fn parse_expr(src: &str) -> ParseResult<Expr> {
	parse_expr_with_settings(src, ParserSettings::default())
}

/// Parse an isolated MySQL expression with custom settings.
pub fn parse_expr_with_settings(src: &str, settings: ParserSettings) -> ParseResult<Expr> {
	if src.len() > settings.max_input_bytes {
		return Err(ParseError::new(
			Span::empty(0),
			ErrorKind::InputSizeLimitExceeded {
				max: settings.max_input_bytes,
				actual: src.len(),
			},
		));
	}
	let tokens = tokenize_with_settings(src, settings)
		.map_err(|e| ParseError::new(Span::empty(0), ErrorKind::Lexer(e.to_string())))?;
	let mut parser = Parser::with_settings(&tokens, settings);
	let expr = parser.parse_expr()?;
	if parser.peek().kind != TokenKind::Eof {
		return Err(ParseError::new(
			parser.peek().span,
			ErrorKind::UnexpectedToken {
				expected: "end of expression",
				found: parser.peek().kind.to_string(),
			},
		));
	}
	Ok(expr)
}

/// Parser state maintaining token stream, cursor, and recursion depth.
pub struct Parser<'a> {
	tokens: &'a [Token],
	cursor: usize,
	pub recursion_depth: usize,
	pub work_count: usize,
	pub settings: ParserSettings,
}

impl<'a> Parser<'a> {
	/// Create a new parser over token slice.
	pub fn new(tokens: &'a [Token]) -> Self {
		Self::with_settings(tokens, ParserSettings::default())
	}

	/// Create a new parser with custom settings.
	pub fn with_settings(tokens: &'a [Token], settings: ParserSettings) -> Self {
		Self {
			tokens,
			cursor: 0,
			recursion_depth: 0,
			work_count: 0,
			settings,
		}
	}

	#[inline]
	pub fn check_recursion_limit(&self) -> ParseResult<()> {
		if self.recursion_depth >= self.settings.max_recursion_depth {
			Err(ParseError::new(self.peek().span, ErrorKind::RecursionLimitExceeded))
		} else if self.work_count > self.settings.max_work_count {
			Err(ParseError::new(self.peek().span, ErrorKind::WorkLimitExceeded))
		} else {
			Ok(())
		}
	}

	#[inline]
	pub fn peek(&self) -> &'a Token {
		self.tokens.get(self.cursor).unwrap_or_else(|| {
			// Fallback token at EOF
			static EOF_TOKEN: Token = Token {
				kind: TokenKind::Eof,
				span: Span {
					start: 0,
					end: 0,
				},
			};
			&EOF_TOKEN
		})
	}

	#[inline]
	pub fn peek_nth(&self, n: usize) -> &'a Token {
		self.tokens.get(self.cursor + n).unwrap_or_else(|| {
			static EOF_TOKEN: Token = Token {
				kind: TokenKind::Eof,
				span: Span {
					start: 0,
					end: 0,
				},
			};
			&EOF_TOKEN
		})
	}

	#[inline]
	pub fn peek_keyword(&self) -> Option<Keyword> {
		match &self.peek().kind {
			TokenKind::Keyword(kw) => Some(*kw),
			_ => None,
		}
	}

	#[inline]
	pub fn peek_nth_keyword(&self, n: usize) -> Option<Keyword> {
		match &self.peek_nth(n).kind {
			TokenKind::Keyword(kw) => Some(*kw),
			_ => None,
		}
	}

	#[inline]
	pub fn bump(&mut self) -> &'a Token {
		let tok = self.peek();
		self.work_count = self.work_count.saturating_add(1);
		if self.cursor < self.tokens.len() {
			self.cursor += 1;
		}
		tok
	}

	pub fn expect(&mut self, expected: &TokenKind, msg: &'static str) -> ParseResult<&'a Token> {
		let tok = self.peek();
		if &tok.kind == expected {
			Ok(self.bump())
		} else {
			Err(ParseError::new(
				tok.span,
				ErrorKind::UnexpectedToken {
					expected: msg,
					found: tok.kind.to_string(),
				},
			))
		}
	}

	pub fn expect_keyword(
		&mut self,
		expected: Keyword,
		msg: &'static str,
	) -> ParseResult<&'a Token> {
		let tok = self.peek();
		if tok.kind == TokenKind::Keyword(expected) {
			Ok(self.bump())
		} else {
			Err(ParseError::new(
				tok.span,
				ErrorKind::UnexpectedToken {
					expected: msg,
					found: tok.kind.to_string(),
				},
			))
		}
	}

	#[inline]
	pub fn eat(&mut self, expected: &TokenKind) -> bool {
		if &self.peek().kind == expected {
			self.bump();
			true
		} else {
			false
		}
	}

	#[inline]
	pub fn eat_keyword(&mut self, expected: Keyword) -> bool {
		if self.peek_keyword() == Some(expected) {
			self.bump();
			true
		} else {
			false
		}
	}

	pub fn parse_ident(&mut self) -> ParseResult<(String, Span)> {
		let tok = self.peek();
		match &tok.kind {
			TokenKind::Ident(s) => {
				let s = s.clone();
				let span = self.bump().span;
				Ok((s, span))
			}
			TokenKind::Keyword(kw) => {
				// MySQL allows many keywords as unquoted identifiers (e.g. status, value, etc.)
				let s = kw.as_str().to_string();
				let span = self.bump().span;
				Ok((s, span))
			}
			_ => Err(ParseError::new(
				tok.span,
				ErrorKind::UnexpectedToken {
					expected: "identifier",
					found: tok.kind.to_string(),
				},
			)),
		}
	}

	pub fn parse_qualified_name(&mut self) -> ParseResult<QualifiedName> {
		let (first, start_span) = self.parse_ident()?;
		let mut parts = vec![first];
		let mut end_span = start_span;

		while self.peek().kind == TokenKind::Dot {
			self.bump(); // '.'
			let (part, span) = self.parse_ident()?;
			parts.push(part);
			end_span = span;
		}

		Ok(QualifiedName::new(parts, start_span.merge(end_span)))
	}
}
