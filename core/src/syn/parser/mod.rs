//! Module implementing the SurrealQL parser.
//!
//! The SurrealQL parse is a relatively simple recursive decent parser.
//! Most of the functions of the SurrealQL parser peek a token from the lexer and then decide to
//! take a path depending on which token is next.
//!
//! # Implementation Details
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
//!
//! ## Far Token Peek
//!
//! Occasionally the parser needs to check further ahead than peeking allows.
//! This is done with the [`Parser::peek_token_at`] function. This function peeks a given number
//! of tokens further than normal up to 3 tokens further.
//!
//! ## WhiteSpace Tokens
//!
//! The lexer produces whitespace tokens, these are tokens which are normally ignored in most place
//! in the syntax as they have no bearing on the meaning of a statements. [`Parser::next`] and
//! [`Parser::peek`] automatically skip over any whitespace tokens. However in some places, like
//! in a record-id and when gluing tokens, these white-space tokens are required for correct
//! parsing. In which case the function [`Parser::next_whitespace`] and others with `_whitespace`
//! are used. These functions don't skip whitespace tokens. However these functions do not undo
//! whitespace tokens which might have been skipped. Implementers must be carefull to not call a
//! functions which requires whitespace tokens when they may already have been skipped.
//!
//! ## Token Gluing
//!
//! Tokens produces from the lexer are in some place more fine-grained then normal. Numbers,
//! Identifiers and strand-like productions could be making up from multiple smaller tokens. A
//! floating point number for example can be at most made up from a 3 digits token, a dot token,
//! an exponent token and number suffix token and two `-` or `+` tokens. Whenever these tokens
//! are required the parser calls a `glue_` method which will take the current peeked token and
//! replace it with a more complex glued together token if possible.
//!
//! ## Use of reblessive
//!
//! This parser uses reblessive to be able to parse deep without overflowing the stack. This means
//! that all functions which might recurse, i.e. in some paths can call themselves again, are async
//! functions taking argument from reblessive to call recursive functions without using more stack
//! with each depth.

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
mod token;
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

	/// Disallow a query to have objects deeper that limit.
	/// Arrays also count towards objects. So `[{foo: [] }]` would be 3 deep.
	pub fn with_object_recursion_limit(mut self, limit: usize) -> Self {
		self.object_recursion = limit;
		self
	}

	/// Disallow a query from being deeper than the give limit.
	/// A query recurses when a statement contains another statement within itself.
	/// Examples are subquery and blocks like block statements and if statements and such.
	pub fn with_query_recursion_limit(mut self, limit: usize) -> Self {
		self.query_recursion = limit;
		self
	}

	/// Parse strand like the old parser where a strand which looks like a UUID, Record-Id, Or a
	/// DateTime will be parsed as a date-time.
	pub fn with_allow_legacy_strand(mut self, value: bool) -> Self {
		self.legacy_strands = value;
		self
	}

	/// Set whether to parse strands as legacy strands.
	pub fn allow_legacy_strand(&mut self, value: bool) {
		self.legacy_strands = value;
	}

	/// Set whether to allow record-id's which don't adheare to regular ident rules.
	/// Setting this to true will allow parsing of, for example, `foo:0bar`. This would be rejected
	/// by normal identifier rules as most identifiers can't start with a number.
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
		let res = loop {
			let res = self.token_buffer.pop().unwrap_or_else(|| self.lexer.next_token());
			if res.kind != TokenKind::WhiteSpace {
				break res;
			}
		};
		self.last_span = res.span;
		res
	}

	/// Returns the next token and advance the parser one token forward.
	///
	/// This function is like next but returns whitespace tokens which are normally skipped
	#[allow(clippy::should_implement_trait)]
	pub fn next_whitespace(&mut self) -> Token {
		let res = self.token_buffer.pop().unwrap_or_else(|| self.lexer.next_token());
		self.last_span = res.span;
		res
	}

	/// Returns if there is a token in the token buffer, meaning that a token was peeked.
	pub fn has_peek(&self) -> bool {
		self.token_buffer.is_empty()
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
			let res = loop {
				let res = self.lexer.next_token();
				if res.kind != TokenKind::WhiteSpace {
					break res;
				}
			};
			self.token_buffer.push(res);
			return res;
		};
		x
	}

	/// Returns the next token without consuming it.
	///
	/// This function is like peek but returns whitespace tokens which are normally skipped
	/// Does not undo tokens skipped in a previous normal peek.
	pub fn peek_whitespace(&mut self) -> Token {
		let Some(x) = self.token_buffer.first() else {
			let res = self.lexer.next_token();
			self.token_buffer.push(res);
			return res;
		};
		x
	}

	/// Return the token kind of the next token without consuming it.
	pub fn peek_kind(&mut self) -> TokenKind {
		self.peek().kind
	}

	/// Returns the next n'th token without consuming it.
	/// `peek_token_at(0)` is equivalent to `peek`.
	pub fn peek_token_at(&mut self, at: u8) -> Token {
		for _ in self.token_buffer.len()..=at {
			let r = loop {
				let r = self.lexer.next_token();
				if r.kind != TokenKind::WhiteSpace {
					break r;
				}
			};
			self.token_buffer.push(r);
		}
		self.token_buffer.at(at).unwrap()
	}

	/// Returns the next n'th token without consuming it.
	/// `peek_token_at(0)` is equivalent to `peek`.
	pub fn peek_whitespace_token_at(&mut self, at: u8) -> Token {
		for _ in self.token_buffer.len()..=at {
			let r = self.lexer.next_token();
			self.token_buffer.push(r);
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

	/// Forces the next token to be the given one.
	/// Used in token gluing to replace the current one with the glued token.
	fn prepend_token(&mut self, token: Token) {
		self.token_buffer.push_front(token);
	}

	/// Returns the string for a given span of the source.
	/// Will panic if the given span was not valid for the source, or invalid utf8
	fn span_str(&self, span: Span) -> &'a str {
		std::str::from_utf8(self.lexer.reader.span(span)).expect("invalid span segment for source")
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
