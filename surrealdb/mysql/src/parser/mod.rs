//! Recursive-descent and Pratt parser for MySQL SQL syntax.

pub mod error;
pub mod expr;
pub mod stmt;

use crate::ast::expr::Expr;
use crate::ast::stmt::{QualifiedName, Statement};
use crate::lexer::keyword::Keyword;
use crate::lexer::span::Span;
use crate::lexer::token::{Token, TokenKind};
use crate::lexer::tokenize;
use crate::parser::error::{ErrorKind, ParseError, ParseResult};

pub const MAX_RECURSION_DEPTH: usize = 128;

/// Parse a single MySQL statement from source string.
pub fn parse_statement(src: &str) -> ParseResult<Statement> {
	let tokens = tokenize(src)
		.map_err(|e| ParseError::new(Span::empty(0), ErrorKind::Lexer(e.to_string())))?;
	let mut parser = Parser::new(&tokens);
	let stmt = parser.parse_statement()?;
	if parser.peek().kind == TokenKind::Semicolon {
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
	let tokens = tokenize(src)
		.map_err(|e| ParseError::new(Span::empty(0), ErrorKind::Lexer(e.to_string())))?;
	let mut parser = Parser::new(&tokens);
	let mut stmts = Vec::new();

	while parser.peek().kind != TokenKind::Eof {
		while parser.peek().kind == TokenKind::Semicolon {
			parser.bump();
		}
		if parser.peek().kind == TokenKind::Eof {
			break;
		}
		stmts.push(parser.parse_statement()?);
		if parser.peek().kind == TokenKind::Semicolon {
			parser.bump();
		}
	}

	Ok(stmts)
}

/// Parse an isolated MySQL expression from source string.
pub fn parse_expr(src: &str) -> ParseResult<Expr> {
	let tokens = tokenize(src)
		.map_err(|e| ParseError::new(Span::empty(0), ErrorKind::Lexer(e.to_string())))?;
	let mut parser = Parser::new(&tokens);
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
}

impl<'a> Parser<'a> {
	/// Create a new parser over token slice.
	pub fn new(tokens: &'a [Token]) -> Self {
		Self {
			tokens,
			cursor: 0,
			recursion_depth: 0,
		}
	}

	#[inline]
	pub fn check_recursion_limit(&self) -> ParseResult<()> {
		if self.recursion_depth >= MAX_RECURSION_DEPTH {
			Err(ParseError::new(self.peek().span, ErrorKind::RecursionLimitExceeded))
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
