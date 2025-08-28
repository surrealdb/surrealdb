//! Module implementing the SurrealQL parser.
//!
//! The SurrealQL parse is a relatively simple recursive decent parser.
//! Most of the functions of the SurrealQL parser peek a token from the lexer
//! and then decide to take a path depending on which token is next.
//!
//! # Implementation Details
//!
//! There are a bunch of common patterns for which this module has some
//! confinence functions.
//! - Whenever only one token can be next you should use the `expected!` macro. This macro ensures
//!   that the given token type is next and if not returns a parser error.
//! - Whenever a limited set of tokens can be next it is common to match the token kind and then
//!   have a catch all arm which calles the macro `unexpected!`. This macro will raise an parse
//!   error with information about the type of token it recieves and what it expected.
//! - If a single token can be optionally next use [`Parser::eat`] this function returns a bool
//!   depending on if the given tokenkind was eaten.
//! - If a closing delimiting token is expected use `Parser::expect_closing_delimiter`. This
//!   function will raise an error if the expected delimiter isn't the next token. This error will
//!   also point to which delimiter the parser expected to be closed.
//!
//! ## Far Token Peek
//!
//! Occasionally the parser needs to check further ahead than peeking allows.
//! This is done with the [`Parser::peek1`] function. This function peeks one
//! token further then peek.
//!
//! ## WhiteSpace Tokens
//!
//! The lexer produces whitespace tokens, these are tokens which are normally
//! ignored in most place in the syntax as they have no bearing on the meaning
//! of a statements. [`Parser::next`] and [`Parser::peek`] automatically skip
//! over any whitespace tokens. However in some places, like in a record-id and
//! when gluing tokens, these white-space tokens are required for correct
//! parsing. In which case the function [`Parser::next_whitespace`] and others
//! with `_whitespace` are used. These functions don't skip whitespace tokens.
//! However these functions do not undo whitespace tokens which might have been
//! skipped. Implementers must be carefull to not call a functions which
//! requires whitespace tokens when they may already have been skipped.
//!
//! ## Compound tokens and token gluing.
//!
//! SurrealQL has a bunch of tokens which have complex rules for when they are
//! allowed and the value they contain. Such tokens are named compound tokens,
//! and examples include a javascript body, strand-like tokens, regex, numbers,
//! etc.
//!
//! These tokens need to be manually requested from the lexer with the
//! [`Lexer::lex_compound`] function.
//!
//! This manually request of tokens leads to a problems when used in conjunction
//! with peeking. Take for instance the production `{ "foo": "bar"}`. `"foo"` is
//! a compound token so when intially encountered the lexer only returns a `"`
//! token and then that token needs to be collected into a the full strand
//! token. However the parser needs to figure out if we are parsing an object or
//! a block so it needs to look past the compound token to see if the next token
//! is `:`. This is where gluing comes in. Calling `Parser::glue` checks if the
//! next token could start a compound token and combines them into a single
//! token. This can only be done in places where we know if we encountered a
//! leading token of a compound token it will result in the 'default' compound
//! token.

use bytes::BytesMut;
use reblessive::{Stack, Stk};

use self::token_buffer::TokenBuffer;
use crate::sql;
use crate::syn::error::{SyntaxError, bail};
use crate::syn::lexer::Lexer;
use crate::syn::lexer::compound::NumberKind;
use crate::syn::token::{Span, Token, TokenKind, t};
use crate::val::{Bytes, Datetime, Duration, File, Strand, Uuid};

mod basic;
mod builtin;
mod expression;
mod function;
mod glue;
mod idiom;
mod kind;
pub(crate) mod mac;
mod object;
mod prime;
mod record_id;
mod stmt;
mod token;
mod token_buffer;
mod value;

pub(crate) use mac::{enter_object_recursion, enter_query_recursion, unexpected};

use super::error::{RenderedError, syntax_error};

#[cfg(test)]
pub mod test;

/// The result returned by most parser function.
pub type ParseResult<T> = Result<T, SyntaxError>;

/// A result of trying to parse a possibly partial query.
#[derive(Debug)]
pub enum PartialResult<T> {
	MoreData,
	/// Parsing the source produced no reasonable value.
	Empty {
		used: usize,
	},
	Ok {
		value: T,
		used: usize,
	},
	Err {
		err: SyntaxError,
		used: usize,
	},
}

#[derive(Default)]
pub enum GluedValue {
	Duration(Duration),
	Datetime(Datetime),
	Uuid(Uuid),
	Number(NumberKind),
	Strand(Strand),
	#[default]
	None,
	Bytes(Bytes),
	File(File),
}

#[derive(Clone, Debug)]
pub struct ParserSettings {
	/// Parse strand like the old parser where a strand which looks like a UUID,
	/// Record-Id, Or a DateTime will be parsed as a date-time.
	pub legacy_strands: bool,
	/// Set whether to allow record-id's which don't adheare to regular ident
	/// rules. Setting this to true will allow parsing of, for example,
	/// `foo:0bar`. This would be rejected by normal identifier rules as most
	/// identifiers can't start with a number.
	pub flexible_record_id: bool,
	/// Disallow a query to have objects deeper that limit.
	/// Arrays also count towards objects. So `[{foo: [] }]` would be 3 deep.
	pub object_recursion_limit: usize,
	/// Disallow a query from being deeper than the give limit.
	/// A query recurses when a statement contains another statement within
	/// itself. Examples are subquery and blocks like block statements and if
	/// statements and such.
	pub query_recursion_limit: usize,
	/// Whether record references are enabled.
	pub references_enabled: bool,
	/// Whether bearer access is enabled
	pub bearer_access_enabled: bool,
	/// Whether bearer access is enabled
	pub define_api_enabled: bool,
	/// Whether the files feature is enabled
	pub files_enabled: bool,
}

impl Default for ParserSettings {
	fn default() -> Self {
		ParserSettings {
			legacy_strands: false,
			flexible_record_id: true,
			object_recursion_limit: 100,
			query_recursion_limit: 20,
			references_enabled: false,
			bearer_access_enabled: false,
			define_api_enabled: false,
			files_enabled: false,
		}
	}
}

impl ParserSettings {
	pub fn default_with_experimental(enabled: bool) -> Self {
		ParserSettings {
			references_enabled: enabled,
			bearer_access_enabled: enabled,
			define_api_enabled: enabled,
			files_enabled: enabled,
			..Self::default()
		}
	}
}

/// The SurrealQL parser.
pub struct Parser<'a> {
	lexer: Lexer<'a>,
	last_span: Span,
	token_buffer: TokenBuffer<4>,
	glued_value: GluedValue,
	pub(crate) table_as_field: bool,
	settings: ParserSettings,
}

impl<'a> Parser<'a> {
	/// Create a new parser from a give source.
	pub fn new(source: &'a [u8]) -> Self {
		Parser::new_with_settings(source, ParserSettings::default())
	}

	/// Create a new parser from a give source.
	pub fn new_with_experimental(source: &'a [u8], enabled: bool) -> Self {
		Parser::new_with_settings(source, ParserSettings::default_with_experimental(enabled))
	}

	/// Create a new parser from a give source.
	pub fn new_with_settings(source: &'a [u8], settings: ParserSettings) -> Self {
		Parser {
			lexer: Lexer::new(source),
			last_span: Span::empty(),
			token_buffer: TokenBuffer::new(),
			glued_value: GluedValue::None,
			table_as_field: true,
			settings,
		}
	}

	pub fn with_settings(mut self, settings: ParserSettings) -> Self {
		self.settings = settings;
		self
	}

	/// Returns the next token and advance the parser one token forward.
	#[expect(clippy::should_implement_trait)]
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
	/// This function is like next but returns whitespace tokens which are
	/// normally skipped
	pub fn next_whitespace(&mut self) -> Token {
		let res = self.token_buffer.pop().unwrap_or_else(|| self.lexer.next_token());
		self.last_span = res.span;
		res
	}

	/// Returns if there is a token in the token buffer, meaning that a token
	/// was peeked.
	pub fn has_peek(&self) -> bool {
		self.token_buffer.is_empty()
	}

	/// Consume the current peeked value and advance the parser one token
	/// forward.
	///
	/// Should only be called after peeking a value.
	pub fn pop_peek(&mut self) -> Token {
		let res = self.token_buffer.pop().unwrap();
		self.last_span = res.span;
		res
	}

	/// Returns the next token without consuming it.
	pub fn peek(&mut self) -> Token {
		loop {
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
			if x.kind == TokenKind::WhiteSpace {
				self.token_buffer.pop();
				continue;
			}
			break x;
		}
	}

	/// Returns the next token without consuming it.
	///
	/// This function is like peek but returns whitespace tokens which are
	/// normally skipped Does not undo tokens skipped in a previous normal
	/// peek.
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
	pub(crate) fn peek_token_at(&mut self, at: u8) -> Token {
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

	pub fn peek1(&mut self) -> Token {
		self.peek_token_at(1)
	}

	pub fn peek2(&mut self) -> Token {
		self.peek_token_at(2)
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

	pub fn peek_whitespace1(&mut self) -> Token {
		self.peek_whitespace_token_at(1)
	}

	pub fn peek_whitespace2(&mut self) -> Token {
		self.peek_whitespace_token_at(2)
	}

	/// Returns the span of the next token if it was already peeked, otherwise
	/// returns the token of the last consumed token.
	pub fn recent_span(&mut self) -> Span {
		self.token_buffer.first().map(|x| x.span).unwrap_or(self.last_span)
	}

	///  returns the token of the last consumed token.
	pub fn last_span(&mut self) -> Span {
		self.last_span
	}

	pub fn assert_finished(&mut self) -> ParseResult<()> {
		let p = self.peek();
		if p.kind != TokenKind::Eof {
			bail!("Unexpected token `{}`, expected no more tokens",p.kind, @p.span);
		}
		Ok(())
	}

	/// Eat the next token if it is of the given kind.
	/// Returns whether a token was eaten.
	pub fn eat(&mut self, token: TokenKind) -> bool {
		let peek = self.peek();
		if token == peek.kind {
			self.token_buffer.pop();
			self.last_span = peek.span;
			true
		} else {
			false
		}
	}

	/// Eat the next token if it is of the given kind.
	/// Returns whether a token was eaten.
	///
	/// Unlike [`Parser::eat`] this doesn't skip whitespace tokens
	pub fn eat_whitespace(&mut self, token: TokenKind) -> bool {
		let peek = self.peek_whitespace();
		if token == peek.kind {
			self.token_buffer.pop();
			self.last_span = peek.span;
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

	/// Checks if the next token is of the given kind. If it isn't it returns a
	/// UnclosedDelimiter error.
	fn expect_closing_delimiter(&mut self, kind: TokenKind, should_close: Span) -> ParseResult<()> {
		let peek = self.peek();
		if peek.kind != kind {
			bail!("Unexpected token `{}` expected delimiter `{kind}`",
				peek.kind,
				@self.recent_span(),
				@should_close => "expected this delimiter to close"
			);
		}
		self.pop_peek();
		Ok(())
	}

	/// Recover the parser state to after a given span.
	pub fn backup_after(&mut self, span: Span) {
		self.token_buffer.clear();
		self.lexer.backup_after(span);
	}

	/// Parse a full query.
	///
	/// This is the primary entry point of the parser.
	pub async fn parse_query(&mut self, stk: &mut Stk) -> ParseResult<sql::Ast> {
		let statements = self.parse_stmt_list(stk).await?;
		Ok(sql::Ast {
			expressions: statements,
		})
	}

	/// Parse a single statement.
	pub async fn parse_statement(&mut self, stk: &mut Stk) -> ParseResult<sql::TopLevelExpr> {
		self.parse_top_level_expr(stk).await
	}

	/// Parse a single expression.
	pub(crate) async fn parse_expr(&mut self, stk: &mut Stk) -> ParseResult<sql::Expr> {
		self.parse_expr_start(stk).await
	}
}

/// A struct which can parse queries statements by statement
pub struct StatementStream {
	stack: Stack,
	settings: ParserSettings,
	col_offset: usize,
	line_offset: usize,
}

impl StatementStream {
	#[expect(clippy::new_without_default)]
	pub fn new() -> Self {
		Self::new_with_settings(ParserSettings::default())
	}

	pub fn new_with_settings(settings: ParserSettings) -> Self {
		StatementStream {
			stack: Stack::new(),
			settings,
			col_offset: 0,
			line_offset: 0,
		}
	}

	/// updates the line and column offset after consuming bytes.
	fn accumulate_line_col(&mut self, bytes: &[u8]) {
		// The parser should have ensured that bytes is a valid utf-8 string.
		// TODO: Maybe change this to unsafe cast once we have more convidence in the
		// parsers correctness.
		let (line_num, remaining) =
			std::str::from_utf8(bytes).unwrap().lines().enumerate().last().unwrap_or((0, ""));

		self.line_offset += line_num;
		if line_num > 0 {
			self.col_offset = 0;
		}
		self.col_offset += remaining.chars().count();
	}

	/// Parses a statement if the buffer contains sufficient data to parse a
	/// statement.
	///
	/// When it will have done so the it will remove the read bytes from the
	/// buffer and return Ok(Some(_)). In case of a parsing error it will
	/// return Err(_), this will not consume data.
	///
	/// If the function returns Ok(None), not enough data was in the buffer to
	/// fully parse a statement, the function might still consume data from the
	/// buffer, like whitespace between statements, when a none is returned.
	pub fn parse_partial(
		&mut self,
		buffer: &mut BytesMut,
	) -> Result<Option<sql::TopLevelExpr>, RenderedError> {
		let mut slice = &**buffer;
		if slice.len() > u32::MAX as usize {
			// limit slice length.
			slice = &slice[..u32::MAX as usize];
		}

		let mut parser = Parser::new_with_settings(slice, self.settings.clone());

		// eat empty statements.
		while parser.eat(t!(";")) {}

		if parser.peek().span.offset != 0 && buffer.len() > u32::MAX as usize {
			// we ate some bytes statements, so in order to ensure whe can parse a full
			// statement of 4gigs we need recreate the parser starting with the empty
			// bytes removed.
			let eaten = buffer.split_to(parser.peek().span.offset as usize);
			self.accumulate_line_col(&eaten);
			slice = &**buffer;
			if slice.len() > u32::MAX as usize {
				// limit slice length.
				slice = &slice[..u32::MAX as usize];
			}
			parser = Parser::new_with_settings(slice, self.settings.clone())
		}

		// test if the buffer is now empty, which would cause the parse_statement
		// function to fail.
		if parser.peek().is_eof() {
			return Ok(None);
		}

		let res = self.stack.enter(|stk| parser.parse_statement(stk)).finish();
		if parser.peek().is_eof() {
			if buffer.len() > u32::MAX as usize {
				let error = syntax_error!("Cannot parse query, statement exceeded maximum size of 4GB", @parser.last_span());
				return Err(error
					.render_on_bytes(buffer)
					.offset_location(self.line_offset, self.col_offset));
			}

			// finished on an eof token.
			// We can't know if this is an actual result, or if it would change when more
			// data is available.
			return Ok(None);
		}

		// we need a trailing semicolon.
		if !parser.eat(t!(";")) {
			let peek = parser.next();

			if parser.peek1().is_eof() {
				return Ok(None);
			}

			if let Err(e) = res {
				return Err(e
					.render_on_bytes(slice)
					.offset_location(self.line_offset, self.col_offset));
			}

			let error = syntax_error!("Unexpected token `{}` expected the query to end.",peek.kind.as_str(),
				@peek.span => "maybe forgot a semicolon after the previous statement?");
			return Err(error
				.render_on_bytes(slice)
				.offset_location(self.line_offset, self.col_offset));
		}

		// Eat possible empty statements.
		while parser.eat(t!(";")) {}

		let eaten = buffer.split_to(parser.last_span().after_offset() as usize);
		let res = res.map(Some).map_err(|e| {
			e.render_on_bytes(&eaten).offset_location(self.line_offset, self.col_offset)
		});
		self.accumulate_line_col(&eaten);
		res
	}

	/// Parse remaining statements once the buffer is complete.
	pub fn parse_complete(
		&mut self,
		buffer: &mut BytesMut,
	) -> Result<Option<sql::TopLevelExpr>, RenderedError> {
		let mut slice = &**buffer;
		if slice.len() > u32::MAX as usize {
			// limit slice length.
			slice = &slice[..u32::MAX as usize];
		}

		let mut parser = Parser::new_with_settings(slice, self.settings.clone());
		// eat empty statements.
		while parser.eat(t!(";")) {}

		if parser.peek().is_eof() {
			// There were no statements in the buffer, clear possible used
			buffer.clear();
			return Ok(None);
		}

		match self.stack.enter(|stk| parser.parse_statement(stk)).finish() {
			Ok(x) => {
				if !parser.peek().is_eof() && !parser.eat(t!(";")) {
					let peek = parser.peek();
					let error = syntax_error!("Unexpected token `{}` expected the query to end.",peek.kind.as_str(),
						@peek.span => "maybe forgot a semicolon after the previous statement?");
					return Err(error
						.render_on_bytes(slice)
						.offset_location(self.line_offset, self.col_offset));
				}

				let eaten = buffer.split_to(parser.last_span().after_offset() as usize);
				self.accumulate_line_col(&eaten);
				Ok(Some(x))
			}
			Err(e) => {
				Err(e.render_on_bytes(slice).offset_location(self.line_offset, self.col_offset))
			}
		}
	}
}
