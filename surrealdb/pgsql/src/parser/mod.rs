//! PostgreSQL parser implementation.

pub mod expr;
pub mod precedence;
pub mod stmt;

use crate::error::{ErrorKind, ParseError, ParseResult};
use crate::lexer::Lexer;
use crate::lexer::token::{Token, TokenKind};
use crate::span::Span;

/// Maximum recursion depth allowed during parsing before aborting.
pub const MAX_RECURSION_DEPTH: u32 = 256;

/// Parser for PostgreSQL SQL grammar.
#[derive(Debug, Clone)]
pub struct Parser<'a> {
	lexer: Lexer<'a>,
	current: Token<'a>,
	depth: u32,
}

impl<'a> Parser<'a> {
	/// Create a new parser initialized to the first token of `src`.
	pub fn new(src: &'a str) -> ParseResult<Self> {
		let mut lexer = Lexer::new(src);
		let current = lexer.next_token()?;
		Ok(Self {
			lexer,
			current,
			depth: 0,
		})
	}

	/// Return the underlying source text.
	#[inline]
	pub const fn src(&self) -> &'a str {
		self.lexer.src()
	}

	/// Reference to the current lookahead token.
	#[inline]
	pub const fn peek(&self) -> &Token<'a> {
		&self.current
	}

	/// Consume and return the current token, advancing to the next.
	pub fn bump(&mut self) -> ParseResult<Token<'a>> {
		let next = self.lexer.next_token()?;
		let prev = std::mem::replace(&mut self.current, next);
		Ok(prev)
	}

	/// Consume the current token if its kind matches `expected`.
	pub fn eat(&mut self, expected: &TokenKind<'_>) -> ParseResult<bool> {
		if &self.current.kind == expected {
			self.bump()?;
			Ok(true)
		} else {
			Ok(false)
		}
	}

	/// Consume the current token if it matches `expected`, or return an `UnexpectedToken` error.
	pub fn expect(
		&mut self,
		expected: &TokenKind<'_>,
		expected_desc: &'static str,
	) -> ParseResult<Token<'a>> {
		if &self.current.kind == expected {
			self.bump()
		} else {
			Err(ParseError::new(
				self.current.span,
				ErrorKind::UnexpectedToken {
					expected: expected_desc,
					found: self.current.kind.to_string(),
				},
			))
		}
	}

	/// Increase recursion depth counter, erroring if limit exceeded.
	#[inline]
	pub fn enter_recursion(&mut self, span: Span) -> ParseResult<()> {
		self.depth = self.depth.saturating_add(1);
		if self.depth > MAX_RECURSION_DEPTH {
			Err(ParseError::new(span, ErrorKind::RecursionLimitExceeded))
		} else {
			Ok(())
		}
	}

	/// Decrease recursion depth counter.
	#[inline]
	pub fn leave_recursion(&mut self) {
		self.depth = self.depth.saturating_sub(1);
	}
}
