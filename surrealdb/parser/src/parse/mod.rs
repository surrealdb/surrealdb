//! Module implementing the parser
//!
//! # Implementation
//! The parser is implement as a hand written, recursive descent parser with up to 3 token
//! look-ahead and minimal backtracking.
//!
//! ## Working with the parser.
//!
//! The most basic functions of the parser are the `peek` method and `next` method most of the
//! other methods are implemented using these two functions.
//!
//! The general pattern for implementing parsing is peeking some token and then deciding ot advance
//! or not based on that token.
//!
//! If an unexpect token is found you should create an error with the surrealdb_common::error api.
//! Error should be constructed within the closure in the `Parser::with_error` function. This way
//! the parser can decide to not build an error when speculating for example.
//!
//! ## Parser states
//! The parser has two state flags that will change the behavior of the parser.
//!
//! First is `speculating` mode, in this mode most function won't generate normal errors but
//! instead create `ParserError::speculating` which can be used to recover from an error for
//! backtracking .
//!
//! Second is `partial` mode, in this mode none of the token producing functions will produce
//! `Ok(None)` instead when a peeking and finding that no more tokens are present the function will
//! return `ParsingError::missing_data()`. This mode is used for streaming, the error indicates
//! that the parser is missing data to determine the correct query and that if more data was
//! available it might be able to correctly parse a query.

use std::borrow::Cow;
use std::fmt::Display;
use std::ops::{Deref, DerefMut};

use ast::{Ast, Node, NodeId};
use bitflags::bitflags;
use common::TypedError;
use common::source_error::{AnnotationKind, Diagnostic, Level, Snippet};
use common::span::Span;
use logos::{Lexer, Logos};
use reblessive::{Stack, Stk};
use token::{BaseTokenKind, Joined, LexError, Token};

use crate::peekable::PeekableLexer;

mod basic;
mod error;
mod expr;
mod kind;
mod misc;
mod peek;
mod place;
mod prime;
mod range;
mod record_id;
mod special;
mod stmt;
mod top_level_expr;
mod unescape;
mod utils;

pub use error::{ParseError, ParseResult};

/// A trait for types which can be individually parsed.
pub trait Parse: Sized {
	#[allow(async_fn_in_trait)]
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self>;
}

impl<P: Parse + Node> Parse for NodeId<P> {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let p = P::parse(parser).await?;
		Ok(parser.push(p))
	}
}

/// A trait for types which can be individually parsed and require no recursion.
/// Faster to call as it doesn't require a future.
pub trait ParseSync: Sized {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self>;
}

impl<P: ParseSync + Node> ParseSync for NodeId<P> {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let p = P::parse_sync(parser)?;
		Ok(parser.push(p))
	}
}

/// Configuration struct for the parser.
#[derive(Debug, Clone, Copy)]
pub struct Config {
	/// Recursion depth limit of the parser.
	///
	/// If the parser ever recursers more times then the limit the parser will throw an error.
	pub depth_limit: usize,

	/// If the parser should generate warning diagnostics,
	/// Unused at the moment.
	pub generate_warnings: bool,

	pub feature_bearer_access: bool,
	pub feature_surrealism: bool,
}

impl Config {
	pub fn all_features() -> Self {
		Config {
			feature_bearer_access: true,
			feature_surrealism: true,
			..Default::default()
		}
	}
}

impl Default for Config {
	fn default() -> Self {
		Self {
			depth_limit: 1024,
			generate_warnings: false,
			feature_bearer_access: false,
			feature_surrealism: false,
		}
	}
}

bitflags! {
	#[derive(Clone,Copy)]
	struct ParserSettings: u8 {
		/// Is the emmiting of warnings enabled.
		const WARNINGS           = 1 << 1;
		/// Is the parser parsing a partially available query.
		const PARTIAL            = 1 << 2;
		/// Is Bearer access feature enabled
		const FEAT_BEARER_ACCESS = 1 << 3;
		/// Is surrealism enabled
		const FEAT_SURREALISM = 1 << 4;
	}
}

bitflags! {
	#[derive(Clone,Copy)]
	pub(crate) struct ParserState: u8 {
		/// Is the parser in a cancelable transaction.
		const TRANSACTION = 1 << 0;
		/// Is the parser in a control flow loop.
		/// Used to reject `break` and `continue` statements which are outside of a loop.
		const LOOP = 1 << 1;
		/// Is the parser speculativily parsing.
		const SPECULATING = 1 << 2;
	}
}

type BaseLexer<'src> = Lexer<'src, BaseTokenKind>;

/// The parser, holds the lexer, parsing state and configurations as well as some reusable buffers.
///
/// It is passed between functions and trait implementation which implement the actual parsing of
/// types.
pub struct Parser<'source, 'ast> {
	lex: PeekableLexer<'source, 4>,
	last_span: Span,
	ast: &'ast mut Ast,
	settings: ParserSettings,
	state: ParserState,
	unescape_buffer: String,
}

impl<'source, 'ast> Parser<'source, 'ast> {
	/// Parse a parsable type.
	pub fn enter_parse<P>(
		source: &str,
		config: Config,
	) -> Result<(P, Ast), TypedError<Diagnostic<'static>>>
	where
		P: Parse,
	{
		let mut ast = Ast::empty();
		let mut stack = Stack::new();
		let node = Self::enter_parse_reuse(source, &mut stack, &mut ast, config)?;
		Ok((node, ast))
	}

	/// Parse a parsable type allowing reusing of resources like an existing stack and ast.
	pub fn enter_parse_reuse<P>(
		source: &str,
		stack: &mut Stack,
		ast: &mut Ast,
		config: Config,
	) -> Result<P, TypedError<Diagnostic<'static>>>
	where
		P: Parse,
	{
		ast.clear();
		let lex = BaseTokenKind::lexer(source);
		let lex = PeekableLexer::new(lex);

		let mut settings = ParserSettings::empty();

		if config.generate_warnings {
			settings |= ParserSettings::WARNINGS;
		}
		if config.feature_surrealism {
			settings |= ParserSettings::FEAT_SURREALISM;
		}
		if config.feature_bearer_access {
			settings |= ParserSettings::FEAT_BEARER_ACCESS;
		}

		let mut parser = Parser {
			lex,
			last_span: Span::empty(),
			ast,
			settings,
			state: ParserState::empty(),
			unescape_buffer: String::new(),
		};

		if source.len() > u32::MAX as usize {
			let span = parser.peek_span();
			return Err(parser
				.error("Query length exceeds maximum length supported by the parser", span)
				.to_diagnostic()
				.expect("returned non diagnostic outside of the approriate context"));
		}

		// We ignore the stk which is mostly just to ensure the no accidental panics or infinite
		// loops because we can maintain it's savety guarentees within the parser.
		let mut runner = stack.enter(|_| parser.parse());

		loop {
			if let Some(x) = runner.step() {
				return x.map_err(|e| {
					e.to_diagnostic()
						.expect("A parser internal error was returned outside of the context where such errors should be generated.")
				});
			}

			if runner.depth() > config.depth_limit {
				std::mem::drop(runner);
				let span = parser.peek_span();
				return Err(parser
					.error("Parser hit maximum configured recursion depth", span)
					.to_diagnostic()
					.expect("returned non diagnostic outside of the approriate context"));
			}
		}
	}

	/// Parse a parsable type in partial mode.
	///
	/// In partial mode if the parser encounters an error which happend due to a sudden end of the
	/// source it will return `Ok(None)` instead of an error assuming the error can fixed by adding
	/// more data to the source.
	pub fn enter_partial_parse<P: Parse + Node>(
		source: &str,
		stack: &mut Stack,
		ast: &mut Ast,
		config: Config,
	) -> Result<Option<NodeId<P>>, TypedError<Diagnostic<'static>>> {
		let lex = BaseTokenKind::lexer(source);
		let lex = PeekableLexer::new(lex);

		let mut settings = ParserSettings::PARTIAL;

		if config.generate_warnings {
			settings |= ParserSettings::WARNINGS;
		}
		if config.feature_surrealism {
			settings |= ParserSettings::FEAT_SURREALISM;
		}
		if config.feature_bearer_access {
			settings |= ParserSettings::FEAT_BEARER_ACCESS;
		}

		let mut parser = Parser {
			lex,
			last_span: Span::empty(),
			ast,
			settings,
			state: ParserState::empty(),
			unescape_buffer: String::new(),
		};

		if source.len() > u32::MAX as usize {
			let span = parser.peek_span();
			return Err(parser
				.error("Query length exceeds maximum length supported by the parser", span)
				.to_diagnostic()
				.expect("returned non-diagnostic error outside of allowed context"));
		}

		// We ignore the stk which is mostly just to ensure the no accidental panics or infinite
		// loops because we can maintain it's savety guarentees within the parser.
		let mut runner = stack.enter(|_| parser.parse());

		loop {
			if let Some(x) = runner.step() {
				match x {
					Ok(x) => return Ok(Some(x)),
					Err(e) => {
						if e.is_missing_data() {
							return Ok(None);
						}

						return Err(e.to_diagnostic()
						.expect("A parser internal error was returned outside of the context where such errors should be generated."));
					}
				}
			}

			if runner.depth() > config.depth_limit {
				std::mem::drop(runner);
				let span = parser.peek_span();
				return Err(parser
					.error("Parser hit maximum configured recursion depth", span)
					.to_diagnostic()
					.expect("returned non-diagnostic error outside of allowed context"));
			}
		}
	}

	/// Speculativily parse a branch.
	///
	/// If the callback returns `Ok(_)` then the lexer state advances like it would normally
	/// and the function will return `Ok(Some(_))`.
	/// If the callback returns `Err(ParseError::speculate())` then it rollsback the lexer to
	/// before the function was called and will return Ok(None), otherwise it will return the
	/// error from the callback.
	///
	/// This function can be used for cases where the right branch cannot be determined from the
	/// n'th next token.
	///
	/// To avoid the cost of constructing large complicated error messages which will be discarded
	/// on recovery this function makes all `with_error` closures return `ParseError::speculative`
	/// which is much cheaper to construct.
	///
	/// However this also means that as soon as the speculative branch has turned to be
	/// non-speculative the user should make sure to exit the closure so as to generate real
	/// errors.
	///
	/// # Usage
	/// This function is very powerfull but also has the drawbacks.
	/// - First it enables ambigous grammar, when implementing new syntax using this function please
	///   first see if it is possible to implement the feature using the peek functions an otherwise
	///   maybe consider redesigning the syntax so it is `LL(n)`.
	///
	/// - Second because it doesn't provide feedback on what exactly happened it can result in
	///   errors being unpredictable
	///
	/// - Third, any parsing using speculating and then recovering is doing extra work it ideally
	///   didn't have to do.
	///
	/// Please limit the use of this function to small branches that can't recurse.
	pub async fn speculate<T, F>(&mut self, cb: F) -> ParseResult<Option<T>>
	where
		F: AsyncFnOnce(&mut Parser) -> ParseResult<T>,
	{
		let backup = self.lex.clone();
		let old_state = self.state;
		self.state |= ParserState::SPECULATING;
		let res = cb(self).await;
		self.state = old_state;
		match res {
			Ok(x) => Ok(Some(x)),
			Err(e) => {
				if e.is_speculative() && !self.state.contains(ParserState::SPECULATING) {
					self.lex = backup;
					Ok(None)
				} else {
					Err(e)
				}
			}
		}
	}

	/// Same as [`speculate`](Parser::speculate) but not asynchronous.
	pub fn speculate_sync<T, F>(&mut self, cb: F) -> ParseResult<Option<T>>
	where
		F: FnOnce(&mut Parser) -> ParseResult<T>,
	{
		let backup = self.lex.clone();
		let old_state = self.state;
		self.state |= ParserState::SPECULATING;
		let res = cb(self);
		self.state = old_state;
		match res {
			Ok(x) => Ok(Some(x)),
			Err(e) => {
				if e.is_speculative() && !self.state.contains(ParserState::SPECULATING) {
					self.lex = backup;
					Ok(None)
				} else {
					Err(e)
				}
			}
		}
	}

	/// Sub-parse a production on the given string.
	///
	/// Some parts of surrealql require parsing the same query on a different string, for example
	/// an escaped one in the case of a record-id string. This function allows parsing such
	/// productions.
	pub async fn sub_parse<P: Parse>(&mut self, sub_str: &str) -> ParseResult<P> {
		let lex = BaseTokenKind::lexer(sub_str);
		let lex = PeekableLexer::new(lex);

		let mut parser = Parser {
			lex,
			last_span: Span::empty(),
			ast: self.ast,
			settings: self.settings,
			state: self.state,
			unescape_buffer: String::new(),
		};

		parser.parse().await
	}

	/// Undoes the speculative state within it's closure.
	///
	/// Use to commit to some branching paths in a speculative context while still able to
	/// speculate in other branches.
	pub async fn commit<T, F>(&mut self, cb: F) -> ParseResult<T>
	where
		F: AsyncFnOnce(&mut Parser) -> ParseResult<T>,
	{
		let old_state = self.state;
		self.state &= !ParserState::SPECULATING;
		let res = cb(self).await;
		self.state = old_state;
		res
	}

	/// Undoes the speculative state within it's closure.
	///
	/// Use to commit to some branching paths in a speculative context while still able to
	/// speculate in other branches.
	pub fn commit_sync<T, F>(&mut self, cb: F) -> ParseResult<T>
	where
		F: FnOnce(&mut Parser) -> ParseResult<T>,
	{
		let old_state = self.state;
		self.state &= !ParserState::SPECULATING;
		let res = cb(self);
		self.state = old_state;
		res
	}

	/// Returns if the parser is in partial mode.
	pub fn partial(&self) -> bool {
		self.settings.contains(ParserSettings::PARTIAL)
	}

	/// Modifies the parser state within the given closure, reseting the parser state to the old
	/// result after the closure returns.
	pub(crate) async fn with_state<F1, F2, R>(&mut self, state_cb: F1, cb: F2) -> ParseResult<R>
	where
		F1: FnOnce(ParserState) -> ParserState,
		F2: AsyncFnOnce(&mut Parser) -> ParseResult<R>,
	{
		let old = self.state;
		self.state = state_cb(old);
		let r = cb(self).await;
		self.state = old;
		r
	}

	/// Returns the next token in the lexer without consuming it.
	pub fn peek(&mut self) -> ParseResult<Option<Token>> {
		match self.lex.peek::<0>() {
			Some(Ok(x)) => Ok(Some(x)),
			None => {
				if self.settings.contains(ParserSettings::PARTIAL) {
					Err(ParseError::missing_data())
				} else {
					Ok(None)
				}
			}
			Some(Err(e)) => Err(self.lex_error(e)),
		}
	}

	/// Returns the next token in the lexer without consuming it expected a token to be present and
	/// returning an error if the lexer has reached the end of the source.
	pub fn peek_expect(&mut self, expected: &str) -> ParseResult<Token> {
		match self.lex.peek::<0>() {
			Some(Ok(x)) => Ok(x),
			None => {
				if self.settings.contains(ParserSettings::PARTIAL) {
					Err(ParseError::missing_data())
				} else {
					let span = self.peek_span();
					Err(self.error(format!("Unexpected end of query, expected {expected}"), span))
				}
			}
			Some(Err(e)) => Err(self.lex_error(e)),
		}
	}

	/// Returns the next token after the first in the lexer without consuming it.
	pub fn peek1(&mut self) -> ParseResult<Option<Token>> {
		match self.lex.peek::<1>() {
			Some(Ok(x)) => Ok(Some(x)),
			None => {
				if self.settings.contains(ParserSettings::PARTIAL) {
					Err(ParseError::missing_data())
				} else {
					Ok(None)
				}
			}
			Some(Err(e)) => Err(self.lex_error(e)),
		}
	}

	/// Returns the next token after the first two tokens in the lexer without consuming it.
	pub fn peek2(&mut self) -> ParseResult<Option<Token>> {
		match self.lex.peek::<2>() {
			Some(Ok(x)) => Ok(Some(x)),
			None => {
				if self.settings.contains(ParserSettings::PARTIAL) {
					Err(ParseError::missing_data())
				} else {
					Ok(None)
				}
			}
			Some(Err(e)) => Err(self.lex_error(e)),
		}
	}

	/// Consumes the next token in the lexer and returns it.
	#[allow(clippy::should_implement_trait)]
	pub fn next(&mut self) -> ParseResult<Option<Token>> {
		match self.lex.next() {
			Some(Ok(x)) => {
				self.last_span = x.span;
				Ok(Some(x))
			}
			None => {
				if self.settings.contains(ParserSettings::PARTIAL) {
					Err(ParseError::missing_data())
				} else {
					Ok(None)
				}
			}
			Some(Err(e)) => Err(self.lex_error(e)),
		}
	}

	/// Returns the next token, expecting a token to be present and returning an error if the
	/// lexer has reached the end of the source.
	pub fn next_expect(&mut self, expected: &str) -> ParseResult<Token> {
		if let Some(x) = self.next()? {
			Ok(x)
		} else if self.settings.contains(ParserSettings::PARTIAL) {
			Err(ParseError::missing_data())
		} else {
			let span = self.eof_span();
			Err(self.error(format!("Unexpected end of query, expected {expected}"), span))
		}
	}

	/// Consumes the next token and returns it, if the token has the same kind as the argument.
	pub fn eat(&mut self, kind: BaseTokenKind) -> ParseResult<Option<Token>> {
		let peek = self.peek()?;
		if let Some(token) = peek
			&& token.token == kind
		{
			self.lex.pop_peek();
			self.last_span = token.span;
			return Ok(Some(token));
		}
		Ok(None)
	}

	/// Consumes the next token and returns it, if the token has the same kind as the argument and
	/// it was joined to previous token.
	pub fn eat_joined(&mut self, kind: BaseTokenKind) -> ParseResult<Option<Token>> {
		let peek = self.peek()?;
		if let Some(token) = peek
			&& token.token == kind
			&& token.joined == Joined::Joined
		{
			self.lex.pop_peek();
			self.last_span = token.span;
			return Ok(Some(token));
		}
		Ok(None)
	}

	/// Returns the next token after the first in the lexer without consuming it.
	///
	/// Also returns None if the token was not joined to the previous token.
	pub fn peek_joined(&mut self) -> ParseResult<Option<Token>> {
		match self.peek()? {
			Some(x) => {
				if let Joined::Joined = x.joined {
					Ok(Some(x))
				} else {
					Ok(None)
				}
			}
			None => Ok(None),
		}
	}

	/// Returns the next token after the first in the lexer without consuming it.
	///
	/// Also returns None if the token was not joined to the previous token.
	pub fn peek_joined1(&mut self) -> ParseResult<Option<Token>> {
		match self.peek1()? {
			Some(x) => {
				if let Joined::Joined = x.joined {
					Ok(Some(x))
				} else {
					Ok(None)
				}
			}
			None => Ok(None),
		}
	}

	/// Returns the next token after the second in the lexer without consuming it.
	///
	/// Also returns None if the token was not joined to the previous token.
	pub fn peek_joined2(&mut self) -> ParseResult<Option<Token>> {
		match self.peek2()? {
			Some(x) => {
				if let Joined::Joined = x.joined {
					Ok(Some(x))
				} else {
					Ok(None)
				}
			}
			None => Ok(None),
		}
	}

	/// Expect a specific token to be next in the lexer, returning an error if this is not the case
	/// and the token if it is.
	pub fn expect(&mut self, kind: BaseTokenKind) -> ParseResult<Token> {
		let Some(token) = self.peek()? else {
			if self.state.contains(ParserState::SPECULATING) {
				return Err(ParseError::speculate());
			}
			return Err(self.unexpected(kind.description()));
		};
		if token.token != kind {
			if self.state.contains(ParserState::SPECULATING) {
				return Err(ParseError::speculate());
			}
			return Err(self.unexpected(kind.description()));
		}
		self.lex.pop_peek();
		self.last_span = token.span;
		Ok(token)
	}

	/// Returns the error marking the next token in the parser to be an unexpected token.
	/// Expects a string specifying what was expected at this point.
	#[cold]
	pub fn unexpected(&mut self, expected: &str) -> ParseError {
		match self.peek() {
			Err(e) => e,
			Ok(Some(token)) => self.unexpected_token(expected, token),
			Ok(None) => {
				if self.state.contains(ParserState::SPECULATING) {
					return ParseError::speculate();
				}
				let span = self.peek_span();
				self.error(format!("Unexpected end of query, expected {}", expected), span)
			}
		}
	}

	/// Returns the error marking the next token in the parser to be an unexpected token.
	/// Expects a string specifying what was expected at this point.
	#[cold]
	pub fn unexpected_token(&mut self, expected: &str, token: Token) -> ParseError {
		if self.state.contains(ParserState::SPECULATING) {
			return ParseError::speculate();
		}
		self.error(
			format!("Unexpected token `{}`, expected {}", self.slice(token.span), expected),
			token.span,
		)
	}

	/// Create an unexpected error but with a given label appied to the annotation.
	#[cold]
	pub fn unexpected_label<T>(&mut self, expected: &str, label: T) -> ParseError
	where
		T: Display,
	{
		let peek = match self.peek() {
			Ok(x) => x,
			Err(e) => return e,
		};

		if self.state.contains(ParserState::SPECULATING) {
			return ParseError::speculate();
		}

		let message = match peek {
			Some(token) => {
				format!("Unexpected token `{}`, expected {}", self.slice(token.span), expected)
			}
			None => format!("Unexpected token end of query, expected {}", expected),
		};

		let span = self.peek_span();
		self.with_error(|this| {
			Level::Error
				.title(message)
				.snippet(
					this.snippet()
						.annotate(AnnotationKind::Primary.span(span).label(label.to_string())),
				)
				.to_diagnostic()
		})
	}

	/// Parse an ast node that might require recursion.
	pub async fn parse<P: Parse>(&mut self) -> ParseResult<P> {
		P::parse(self).await
	}

	/// Parses an ast node entering into a new stack frame.
	pub async fn parse_enter<P: Parse>(&mut self) -> ParseResult<P> {
		Stk::enter_run(|_| P::parse(self)).await
	}

	/// Parse an ast node that can be parsed without recursion
	pub fn parse_sync<P: ParseSync>(&mut self) -> ParseResult<P> {
		P::parse_sync(self)
	}

	/// Enter into a new stack context, use when running a recursive function.
	pub async fn enter<R, F>(&mut self, cb: F) -> R
	where
		F: AsyncFnOnce(&mut Self) -> R,
	{
		Stk::enter_run(|_| cb(self)).await
	}

	/// Access the lexer in a closure.
	///
	/// The closure should return a lexer advnced to the point that the parser can continue
	/// parsing.
	///
	/// This function is used to implement syntax which cannot be parsed with the standard lexer,
	/// for example a regex.
	pub fn lex<T, F>(&mut self, f: F) -> ParseResult<T>
	where
		F: FnOnce(BaseLexer<'source>, &mut String) -> ParseResult<(BaseLexer<'source>, T)>,
	{
		assert!(!self.lex.has_peek(), "Lexing special tokens requires the lexer to be empty");

		let lexer = self.lex.lexer().clone();
		let (lex, t) = f(lexer, &mut self.unescape_buffer)?;
		*self.lex.lexer() = lex;
		Ok(t)
	}

	/// Returns the full source the query is parsing.
	pub fn source(&self) -> &'source str {
		self.lex.source()
	}

	/// Returns sub string of full source that corresponds to the given span.
	pub fn slice(&self, span: Span) -> &'source str {
		&self.lex.source()[(span.start as usize)..(span.end as usize)]
	}

	/// Returns the snippet belonging to the current source code.
	pub fn snippet(&self) -> Snippet<'source> {
		Snippet::source(self.lex.source())
	}

	/// A tiny function, for creating an error handing the current span to a callback.
	///
	/// This function will only call the callback if the parser is not in the speculating state,
	/// otherwise this function will return `ParseError::speculate_error()`
	#[cold]
	pub fn with_error<F>(&mut self, cb: F) -> ParseError
	where
		F: FnOnce(&mut Self) -> Diagnostic<'source>,
	{
		if self.state.contains(ParserState::SPECULATING) {
			ParseError::speculate()
		} else {
			ParseError::diagnostic(cb(self).to_owned())
		}
	}

	/// Returns an error with the given message with a snippet pointing to the next token.
	#[cold]
	pub fn error<T>(&mut self, msg: T, span: Span) -> ParseError
	where
		Cow<'source, str>: From<T>,
	{
		self.with_error(|this| {
			Level::Error
				.title(msg)
				.snippet(this.snippet().annotate(AnnotationKind::Primary.span(span)))
				.to_diagnostic()
		})
	}

	/// Creates a parsing error for a lexing error.
	#[cold]
	fn lex_error(&mut self, e: LexError) -> ParseError {
		match e {
			LexError::UnexpectedEof(span) => {
				if self.settings.contains(ParserSettings::PARTIAL) {
					return ParseError::missing_data();
				}
				self.with_error(|this| {
					Level::Error
						.title("Unexpected end of query while lexing a token")
						.snippet(this.snippet().annotate(AnnotationKind::Primary.span(span)))
						.to_diagnostic()
				})
			}
			LexError::InvalidToken(span) => self.with_error(|this| {
				Level::Error
					.title("Invalid token")
					.snippet(this.snippet().annotate(AnnotationKind::Primary.span(span)))
					.to_diagnostic()
			}),
		}
	}

	/// Tries to eat the next token, returning an missing delimiter error if it is not the correct
	/// token.
	pub fn expect_closing_delimiter(
		&mut self,
		delimiter: BaseTokenKind,
		open_span: Span,
	) -> ParseResult<Token> {
		if let Some(peek) = self.peek()? {
			if peek.token != delimiter {
				return Err(self.with_error(|this| {
					Level::Error
						.title(format!(
							"Unexpected token `{}`, expected closing delimiter {}",
							this.slice(peek.span),
							delimiter.description()
						))
						.snippet(
							this.snippet()
								.annotate(
									AnnotationKind::Primary.span(peek.span).label(format!(
										"Missing {} here.",
										delimiter.description()
									)),
								)
								.annotate(
									AnnotationKind::Context
										.span(open_span)
										.label("Expected this delimiter to close"),
								),
						)
						.to_diagnostic()
				}));
			}
			let _ = self.next();
			Ok(peek)
		} else {
			Err(self.with_error(|this| {
				Level::Error
					.title(format!(
						"Unexpected end of query, expected closing delimiter {}",
						delimiter.description()
					))
					.snippet(
						this.snippet()
							.annotate(
								AnnotationKind::Primary
									.span(this.eof_span())
									.label(format!("Missing {} here.", delimiter.description())),
							)
							.annotate(
								AnnotationKind::Context
									.span(open_span)
									.label("Expected this delimiter to close"),
							),
					)
					.to_diagnostic()
			}))
		}
	}

	/// Returns the span for the next token in the lexer.
	pub fn peek_span(&mut self) -> Span {
		self.lex.peek_span()
	}

	/// Returns the span covering all eaten tokens since and including the given span.
	pub fn span_since(&self, span: Span) -> Span {
		span.extend(self.last_span)
	}

	/// Returns the span of that points to the end of the source.
	pub fn eof_span(&self) -> Span {
		self.lex.eof_span()
	}

	/// Returns the span of that points to the end of the source.
	pub fn eof(&mut self) -> bool {
		self.lex.peek::<0>().is_none()
	}
}

impl<'source, 'ast> Deref for Parser<'source, 'ast> {
	type Target = Ast;

	fn deref(&self) -> &Self::Target {
		self.ast
	}
}
impl<'source, 'ast> DerefMut for Parser<'source, 'ast> {
	fn deref_mut(&mut self) -> &mut Ast {
		self.ast
	}
}
