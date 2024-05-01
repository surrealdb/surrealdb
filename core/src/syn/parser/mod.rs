//! Module implementing the SurrealQL parser.
//!
//! The SurrealQL parse is a relatively simple recursive decent parser.
//! Most of the functions of the SurrealQL parser peek a token from the lexer and then decide to
//! take a path depending on which token is next.
//!
//! There are a bunch of common patterns for which this module has some confinence functions.
//! - Whenever only one token can be next you should use the [`expected!`] macro. This macro
//! ensures that the given token type is next and if not returns a parser error.
//! - Whenever a limited set of tokens can be next it is common to match the token kind and then
//! have a catch all arm which calles the macro [`unexpected!`]. This macro will raise an parse
//! error with information about the type of token it recieves and what it expected.
//! - If a single token can be optionally next use [`Parser::eat`] this function returns a bool
//! depending on if the given tokenkind was eaten.
//! - If a closing delimiting token is expected use [`Parser::expect_closing_delimiter`]. This
//! function will raise an error if the expected delimiter isn't the next token. This error will
//! also point to which delimiter the parser expected to be closed.

use self::token_buffer::TokenBuffer;
use crate::{
	sql,
	syn::{
		lexer::{Error as LexError, Lexer},
		token::{t, Span, Token, TokenKind},
	},
};
use reblessive::Stk;

mod basic;
mod builtin;
mod error;
mod expression;
mod function;
mod idiom;
mod json;
mod kind;
pub(crate) mod mac;
mod object;
mod prime;
mod stmt;
mod thing;
mod token_buffer;

#[cfg(test)]
pub mod test;

pub use error::{IntErrorKind, ParseError, ParseErrorKind};

/// The result returned by most parser function.
pub type ParseResult<T> = Result<T, ParseError>;

/// A result of trying to parse a possibly partial query.
#[derive(Debug)]
#[non_exhaustive]
pub enum PartialResult<T> {
	/// The parser couldn't be sure that it has finished a full value.
	Pending {
		/// The value that was parsed.
		/// This will not always be an error, if optional keywords after the end of a statement
		/// where missing this will still parse that statement in full.
		possible_value: Result<T, ParseError>,
		/// number of bytes used for parsing the above statement.
		used: usize,
	},
	/// The parser is sure that it doesn't need more data to return either an error or a value.
	Ready {
		/// The value the parser is sure the query should return.
		value: Result<T, ParseError>,
		/// number of bytes used
		used: usize,
	},
}

/// The SurrealQL parser.
#[non_exhaustive]
pub struct Parser<'a> {
	lexer: Lexer<'a>,
	last_span: Span,
	token_buffer: TokenBuffer<4>,
	table_as_field: bool,
	legacy_strands: bool,
	flexible_record_id: bool,
	object_recursion: usize,
	query_recursion: usize,
}

impl<'a> Parser<'a> {
	/// Create a new parser from a give source.
	pub fn new(source: &'a [u8]) -> Self {
		Parser {
			lexer: Lexer::new(source),
			last_span: Span::empty(),
			token_buffer: TokenBuffer::new(),
			table_as_field: false,
			legacy_strands: false,
			flexible_record_id: true,
			object_recursion: 100,
			query_recursion: 20,
		}
	}

	pub fn with_object_recursion_limit(mut self, limit: usize) -> Self {
		self.object_recursion = limit;
		self
	}

	pub fn with_query_recursion_limit(mut self, limit: usize) -> Self {
		self.query_recursion = limit;
		self
	}

	pub fn with_allow_legacy_strand(mut self, value: bool) -> Self {
		self.legacy_strands = value;
		self
	}

	/// Set whether to parse strands as legacy strands.
	pub fn allow_legacy_strand(&mut self, value: bool) {
		self.legacy_strands = value;
	}

	/// Set whether to allow record-id's which don't adheare to regular ident rules.
	pub fn allow_fexible_record_id(&mut self, value: bool) {
		self.flexible_record_id = value;
	}

	/// Reset the parser state. Doesnt change the position of the parser in buffer.
	pub fn reset(&mut self) {
		self.last_span = Span::empty();
		self.token_buffer.clear();
		self.table_as_field = false;
		self.lexer.reset();
	}

	/// Change the source of the parser reusing the existing buffers.
	pub fn change_source(self, source: &[u8]) -> Parser {
		Parser {
			lexer: self.lexer.change_source(source),
			last_span: Span::empty(),
			token_buffer: TokenBuffer::new(),
			legacy_strands: self.legacy_strands,
			flexible_record_id: self.flexible_record_id,
			table_as_field: false,
			object_recursion: self.object_recursion,
			query_recursion: self.query_recursion,
		}
	}

	/// Returns the next token and advance the parser one token forward.
	#[allow(clippy::should_implement_trait)]
	pub fn next(&mut self) -> Token {
		let res = self.token_buffer.pop().unwrap_or_else(|| self.lexer.next_token());
		self.last_span = res.span;
		res
	}

	/// Consume the current peeked value and advance the parser one token forward.
	///
	/// Should only be called after peeking a value.
	pub fn pop_peek(&mut self) -> Token {
		let res = self.token_buffer.pop().unwrap();
		self.last_span = res.span;
		res
	}

	/// Returns the next token without consuming it.
	pub fn peek(&mut self) -> Token {
		let Some(x) = self.token_buffer.first() else {
			let res = self.lexer.next_token();
			self.token_buffer.push(res);
			return res;
		};
		x
	}

	/// Return the token kind of the next token without consuming it.
	pub fn peek_kind(&mut self) -> TokenKind {
		let Some(x) = self.token_buffer.first().map(|x| x.kind) else {
			let res = self.lexer.next_token();
			self.token_buffer.push(res);
			return res.kind;
		};
		x
	}

	/// Returns the next n'th token without consuming it.
	/// `peek_token_at(0)` is equivalent to `peek`.
	pub fn peek_token_at(&mut self, at: u8) -> Token {
		for _ in self.token_buffer.len()..=at {
			self.token_buffer.push(self.lexer.next_token());
		}
		self.token_buffer.at(at).unwrap()
	}

	/// Returns the span of the next token if it was already peeked, otherwise returns the token of
	/// the last consumed token.
	pub fn recent_span(&mut self) -> Span {
		self.token_buffer.first().map(|x| x.span).unwrap_or(self.last_span)
	}

	///  returns the token of the last consumed token.
	pub fn last_span(&mut self) -> Span {
		self.last_span
	}

	/// Eat the next token if it is of the given kind.
	/// Returns whether a token was eaten.
	pub fn eat(&mut self, token: TokenKind) -> bool {
		if token == self.peek().kind {
			self.token_buffer.pop();
			true
		} else {
			false
		}
	}

	/// Checks if the next token is of the given kind. If it isn't it returns a UnclosedDelimiter
	/// error.
	fn expect_closing_delimiter(&mut self, kind: TokenKind, should_close: Span) -> ParseResult<()> {
		if !self.eat(kind) {
			return Err(ParseError::new(
				ParseErrorKind::UnclosedDelimiter {
					expected: kind,
					should_close,
				},
				self.recent_span(),
			));
		}
		Ok(())
	}

	/// Ensure that there was no whitespace parser between the last token and the current one.
	///
	/// This is used in places where whitespace is prohibited like inside a record id.
	fn no_whitespace(&mut self) -> ParseResult<()> {
		if let Some(span) = self.lexer.whitespace_span() {
			Err(ParseError::new(ParseErrorKind::NoWhitespace, span))
		} else {
			Ok(())
		}
	}

	/// Recover the parser state to after a given span.
	pub fn backup_after(&mut self, span: Span) {
		self.token_buffer.clear();
		self.lexer.backup_after(span);
	}

	/// Parse a full query.
	///
	/// This is the primary entry point of the parser.
	pub async fn parse_query(&mut self, ctx: &mut Stk) -> ParseResult<sql::Query> {
		let statements = self.parse_stmt_list(ctx).await?;
		Ok(sql::Query(statements))
	}

	/// Parse a single statement.
	pub async fn parse_statement(&mut self, ctx: &mut Stk) -> ParseResult<sql::Statement> {
		self.parse_stmt(ctx).await
	}

	/// Parse a possibly partial statement.
	///
	/// This will try to parse a statement if a full statement can be parsed from the buffer parser
	/// is operating on.
	pub async fn parse_partial_statement(
		&mut self,
		ctx: &mut Stk,
	) -> PartialResult<sql::Statement> {
		while self.eat(t!(";")) {}

		let res = ctx.run(|ctx| self.parse_stmt(ctx)).await;
		match res {
			Err(ParseError {
				kind: ParseErrorKind::UnexpectedEof {
					..
				},
				..
			})
			| Err(ParseError {
				kind: ParseErrorKind::InvalidToken(LexError::UnexpectedEof),
				..
			}) => {
				return PartialResult::Pending {
					possible_value: res,
					used: self.lexer.reader.offset(),
				};
			}
			Err(ParseError {
				kind: ParseErrorKind::Unexpected {
					..
				},
				at,
				..
			}) => {
				// Ensure the we are sure that the last token was fully parsed.
				self.backup_after(at);
				if self.peek().kind != TokenKind::Eof || self.lexer.whitespace_span().is_some() {
					// if there is a next token or we ate whitespace after the eof we can be sure
					// that the error is not the result of a token only being partially present.
					return PartialResult::Ready {
						value: res,
						used: self.lexer.reader.offset(),
					};
				}
			}
			_ => {}
		};

		let colon = self.next();
		if colon.kind != t!(";") {
			return PartialResult::Pending {
				possible_value: res,
				used: self.lexer.reader.offset(),
			};
		}

		// Might have peeked more tokens past the final ";" so backup to after the semi-colon.
		self.backup_after(colon.span);
		let used = self.lexer.reader.offset();

		PartialResult::Ready {
			value: res,
			used,
		}
	}
}
