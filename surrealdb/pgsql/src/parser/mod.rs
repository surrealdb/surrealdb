//! PostgreSQL parser implementation.

pub mod expr;
pub mod precedence;
pub mod stmt;

use crate::error::{ErrorKind, ParseError, ParseResult};
use crate::lexer::Lexer;
use crate::lexer::token::{Token, TokenKind};
use crate::span::Span;

/// Maximum recursion depth allowed during parsing before aborting.
pub const MAX_RECURSION_DEPTH: u32 = 128;
/// Maximum input size in bytes (16 MiB).
pub const DEFAULT_MAX_INPUT_BYTES: usize = 16 * 1024 * 1024;
/// Maximum work counter (steps/tokens) allowed during parsing.
pub const DEFAULT_MAX_WORK_COUNT: usize = 100_000;

/// Configuration settings for PostgreSQL parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParserSettings {
	/// Maximum recursion depth allowed during parsing.
	pub max_recursion_depth: u32,
	/// Maximum input size in bytes.
	pub max_input_bytes: usize,
	/// Maximum parsing work count.
	pub max_work_count: usize,
}

impl Default for ParserSettings {
	fn default() -> Self {
		Self {
			max_recursion_depth: MAX_RECURSION_DEPTH,
			max_input_bytes: DEFAULT_MAX_INPUT_BYTES,
			max_work_count: DEFAULT_MAX_WORK_COUNT,
		}
	}
}

/// Parser for PostgreSQL SQL grammar.
#[derive(Debug, Clone)]
pub struct Parser<'a> {
	lexer: Lexer<'a>,
	current: Token<'a>,
	peeked: Option<Token<'a>>,
	depth: u32,
	work_count: usize,
	settings: ParserSettings,
}

impl<'a> Parser<'a> {
	/// Create a new parser initialized to the first token of `src` with default settings.
	pub fn new(src: &'a str) -> ParseResult<Self> {
		Self::with_settings(src, ParserSettings::default())
	}

	/// Create a new parser initialized with custom settings.
	pub fn with_settings(src: &'a str, settings: ParserSettings) -> ParseResult<Self> {
		if src.len() > settings.max_input_bytes {
			return Err(ParseError::new(
				Span::empty(0),
				ErrorKind::InputSizeLimitExceeded {
					max: settings.max_input_bytes,
					actual: src.len(),
				},
			));
		}
		let mut lexer = Lexer::new(src);
		let current = lexer.next_token()?;
		Ok(Self {
			lexer,
			current,
			peeked: None,
			depth: 0,
			work_count: 0,
			settings,
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

	/// Reference to the next lookahead token (2nd token).
	pub fn peek2(&mut self) -> ParseResult<&Token<'a>> {
		if self.peeked.is_none() {
			self.peeked = Some(self.lexer.next_token()?);
		}
		if let Some(tok) = &self.peeked {
			Ok(tok)
		} else {
			Err(ParseError::new(
				self.current.span,
				ErrorKind::UnexpectedToken {
					expected: "token",
					found: "none".to_string(),
				},
			))
		}
	}

	/// Record work step, erroring if work limit exceeded.
	#[inline]
	pub fn record_work(&mut self) -> ParseResult<()> {
		self.work_count = self.work_count.saturating_add(1);
		if self.work_count > self.settings.max_work_count {
			Err(ParseError::new(self.current.span, ErrorKind::WorkLimitExceeded))
		} else {
			Ok(())
		}
	}

	/// Consume and return the current token, advancing to the next.
	pub fn bump(&mut self) -> ParseResult<Token<'a>> {
		self.record_work()?;
		let next = match self.peeked.take() {
			Some(tok) => tok,
			None => self.lexer.next_token()?,
		};
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
		if self.depth > self.settings.max_recursion_depth {
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

	/// Decrease recursion depth counter by a given amount.
	#[inline]
	pub fn leave_recursion_by(&mut self, n: u32) {
		self.depth = self.depth.saturating_sub(n);
	}

	/// Return infix binding power taking 2-token lookahead into account (e.g. `NOT IN`, `NOT
	/// BETWEEN`).
	pub fn peek_infix_bp(&mut self) -> ParseResult<Option<(u8, u8, precedence::Associativity)>> {
		if self.peek().kind == TokenKind::Keyword(crate::lexer::keyword::Keyword::Not) {
			let next_tok = self.peek2()?;
			if let TokenKind::Keyword(
				crate::lexer::keyword::Keyword::Between
				| crate::lexer::keyword::Keyword::In
				| crate::lexer::keyword::Keyword::Like
				| crate::lexer::keyword::Keyword::Ilike
				| crate::lexer::keyword::Keyword::Similar,
			) = next_tok.kind
			{
				Ok(Some((40, 41, precedence::Associativity::None)))
			} else {
				Ok(None)
			}
		} else if self.peek().kind == TokenKind::Keyword(crate::lexer::keyword::Keyword::Operator) {
			// gram.y: `%left Op OPERATOR`, so `OPERATOR(schema.op)` binds as any other operator.
			// Without its parenthesis the word is a name, such as a column alias.
			if self.peek2()?.kind == TokenKind::LParen {
				Ok(Some((65, 66, precedence::Associativity::Left)))
			} else {
				Ok(None)
			}
		} else {
			Ok(precedence::infix_binding_power(&self.peek().kind))
		}
	}
}
