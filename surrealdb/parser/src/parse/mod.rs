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
mod peek;
pub mod prime;
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

/// A trait for types which can be individually parsed and require no recursion.
/// Faster to call as it doesn't require a future.
pub trait ParseSync: Sized {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self>;
}

/// Configuration struct for the parser.
#[derive(Debug, Clone, Copy)]
pub struct Config {
	pub depth_limit: usize,

	pub flexible_record_ids: bool,
	pub generate_warnings: bool,

	pub feature_references: bool,
	pub feature_bearer_access: bool,
	pub feature_define_api: bool,
	pub feature_files: bool,
	pub legacy_strands: bool,
}

impl Config {
	pub fn all_features() -> Self {
		Config {
			feature_references: true,
			feature_bearer_access: true,
			feature_define_api: true,
			feature_files: true,
			..Default::default()
		}
	}
}

impl Default for Config {
	fn default() -> Self {
		Self {
			depth_limit: 1024,
			flexible_record_ids: true,
			generate_warnings: false,
			feature_references: false,
			feature_bearer_access: false,
			feature_define_api: false,
			feature_files: false,
			legacy_strands: false,
		}
	}
}

bitflags! {
	#[derive(Clone,Copy)]
	struct ParserSettings: u8 {
		/// Are legacy_strands strands enabled.
		const LEGACY_STRAND      = 1 << 0;
		/// Is the emmiting of warnings enabled.
		const WARNINGS           = 1 << 1;
		/// Is the parser parsing a partially available query.
		const PARTIAL            = 1 << 2;
		/// Is the record-references feature enabled
		const FEAT_REFERENCES    = 1 << 3;
		/// Is Bearer access feature enabled
		const FEAT_BEARER_ACCESS = 1 << 4;
		/// Is the define API feature enabled
		const FEAT_DEFINE_API    = 1 << 5;
		/// Is the files feature enabled
		const FEAT_FILES         = 1 << 6;
	}
}

bitflags! {
	#[derive(Clone,Copy)]
	pub struct ParserState: u8 {
		/// Is the parser in a cancelable transaction.
		const TRANSACTION = 1 << 0;
		/// Is the parser in a control flow loop.
		const LOOP = 1 << 1;
		/// Is the parser speculativily parsing.
		const SPECULATING = 1 << 2;
	}
}

pub type BaseLexer<'src> = Lexer<'src, BaseTokenKind>;

/// The parser, holds the lexer, parsing state and configurations as well as some reusable buffers.
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
	) -> Result<(NodeId<P>, Ast), TypedError<Diagnostic<'static>>>
	where
		P: Parse + Node,
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
	) -> Result<NodeId<P>, TypedError<Diagnostic<'static>>>
	where
		P: Parse + Node,
	{
		ast.clear();
		let lex = BaseTokenKind::lexer(source);
		let lex = PeekableLexer::new(lex);

		let mut features = ParserSettings::empty();

		if config.legacy_strands {
			features |= ParserSettings::LEGACY_STRAND;
		}
		if config.generate_warnings {
			features |= ParserSettings::WARNINGS;
		}
		if config.feature_references {
			features |= ParserSettings::FEAT_REFERENCES;
		}
		if config.feature_define_api {
			features |= ParserSettings::FEAT_DEFINE_API;
		}
		if config.feature_files {
			features |= ParserSettings::FEAT_FILES;
		}
		if config.legacy_strands {
			features |= ParserSettings::LEGACY_STRAND;
		}

		let mut parser = Parser {
			lex,
			last_span: Span::empty(),
			ast,
			settings: features,
			state: ParserState::empty(),
			unescape_buffer: String::new(),
		};

		if source.len() > u32::MAX as usize {
			return Err(parser
				.error("Query length exceeds maximum length supported by the parser")
				.to_diagnostic()
				.unwrap());
		}

		// We ignore the stk which is mostly just to ensure the no accidental panics or infinite
		// loops because we can maintain it's savety guarentees within the parser.
		let mut runner = stack.enter(|_| parser.parse_push());

		loop {
			if let Some(x) = runner.step() {
				return x.map_err(|e| {
					e.to_diagnostic()
						.expect("A parser internal error was returned outside of the context where such errors should be generated.")
				});
			}

			if runner.depth() > config.depth_limit {
				std::mem::drop(runner);
				return Err(parser
					.error("Parser hit maximum configured recursion depth")
					.to_diagnostic()
					.unwrap());
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

		let mut features = ParserSettings::PARTIAL;

		if config.legacy_strands {
			features |= ParserSettings::LEGACY_STRAND;
		}
		if config.generate_warnings {
			features |= ParserSettings::WARNINGS;
		}
		if config.feature_references {
			features |= ParserSettings::FEAT_REFERENCES;
		}
		if config.feature_define_api {
			features |= ParserSettings::FEAT_DEFINE_API;
		}
		if config.feature_files {
			features |= ParserSettings::FEAT_FILES;
		}
		if config.legacy_strands {
			features |= ParserSettings::LEGACY_STRAND;
		}

		let mut parser = Parser {
			lex,
			last_span: Span::empty(),
			ast,
			settings: features,
			state: ParserState::empty(),
			unescape_buffer: String::new(),
		};

		if source.len() > u32::MAX as usize {
			return Err(parser
				.error("Query length exceeds maximum length supported by the parser")
				.to_diagnostic()
				.unwrap());
		}

		// We ignore the stk which is mostly just to ensure the no accidental panics or infinite
		// loops because we can maintain it's savety guarentees within the parser.
		let mut runner = stack.enter(|_| parser.parse_push());

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
				return Err(parser
					.error("Parser hit maximum configured recursion depth")
					.to_diagnostic()
					.unwrap());
			}
		}
	}

	/// Speculativily parse a branch.
	///
	/// If the callback returns `Ok(Some(_))` then the lexer state advances like it would normally.
	/// However if any other value is returned from the callback the lexer is rolled back to before
	/// the function was called.
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

	/// Returns a speculative error,
	pub fn recover(&self) -> ParseResult<()> {
		assert!(
			self.state.contains(ParserState::SPECULATING),
			"Parser::recover can only be called in a speculating context"
		);

		Err(ParseError::speculate_error())
	}

	/// Undoes the speculative state within it's closure.
	///
	/// Use to commit to some branching paths in a speculative context while still able to
	/// speulcate in other branches.
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

	/// Marks a branch as possibly happening due to missing data.
	/// This will return a missing_data_error error if the parser is in PARTIAL mode.
	///
	/// # Usage
	/// Use of this function is mostly unnessacry as most function interacting with tokens
	/// implemented on `Parser` will handle possible missing data already.
	///
	/// This function should be used in context where we stop using the parser provided function,
	/// like parsing special tokens.
	pub fn might_lack_data(&self) -> ParseResult<()> {
		if self.settings.contains(ParserSettings::PARTIAL) {
			return Err(ParseError::missing_data_error());
		}
		Ok(())
	}

	/// Returns if the parser is in partial mode.
	pub fn partial(&self) -> bool {
		self.settings.contains(ParserSettings::PARTIAL)
	}

	/// Modifies the parser state within the given closure, reseting the parser state to the old
	/// result after the closure returns.
	pub async fn with_state<F1, F2, R>(&mut self, state_cb: F1, cb: F2) -> ParseResult<R>
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
					return Err(ParseError::missing_data_error());
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
					Err(ParseError::missing_data_error())
				} else {
					Err(self.error(format!("Unexpected end of query, expected {expected}")))
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
					return Err(ParseError::missing_data_error());
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
					return Err(ParseError::missing_data_error());
				} else {
					Ok(None)
				}
			}
			Some(Err(e)) => Err(self.lex_error(e)),
		}
	}

	/// Consumes the next token in the lexer and returns it.
	pub fn next(&mut self) -> ParseResult<Option<Token>> {
		match self.lex.next() {
			Some(Ok(x)) => {
				self.last_span = x.span;
				Ok(Some(x))
			}
			None => {
				if self.settings.contains(ParserSettings::PARTIAL) {
					return Err(ParseError::missing_data_error());
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
		} else {
			if self.settings.contains(ParserSettings::PARTIAL) {
				Err(ParseError::missing_data_error())
			} else {
				Err(self.error(format!("Unexpected end of query, expected {expected}")))
			}
		}
	}

	/// Consumes the next token and returns it, if the token has the same kind as the argument.
	pub fn eat(&mut self, kind: BaseTokenKind) -> ParseResult<Option<Token>> {
		let peek = self.peek()?;
		if let Some(token) = peek {
			if token.token == kind {
				self.lex.pop_peek();
				self.last_span = token.span;
				return Ok(Some(token));
			}
		}
		Ok(None)
	}

	/// Consumes the next token and returns it, if the token has the same kind as the argument and
	/// it was joined to previous token.
	pub fn eat_joined(&mut self, kind: BaseTokenKind) -> ParseResult<Option<Token>> {
		let peek = self.peek()?;
		if let Some(token) = peek {
			if token.token == kind && token.joined == Joined::Joined {
				self.lex.pop_peek();
				self.last_span = token.span;
				return Ok(Some(token));
			}
		}
		Ok(None)
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
				return Err(ParseError::speculate_error());
			}
			return Err(self.unexpected(kind.description()));
		};
		if token.token != kind {
			if self.state.contains(ParserState::SPECULATING) {
				return Err(ParseError::speculate_error());
			}
			return Err(self.unexpected(kind.description()));
		}
		self.lex.pop_peek();
		self.last_span = token.span;
		Ok(token)
	}

	/// Returns if we reached the end of file of the source
	pub fn eof(&self) -> bool {
		self.lex.is_empty()
	}

	/// Returns the error marking the next token in the parser to be an unexpected token.
	#[cold]
	pub fn unexpected(&mut self, expected: &str) -> ParseError {
		match self.peek() {
			Err(e) => e,
			Ok(Some(token)) => {
				if self.state.contains(ParserState::SPECULATING) {
					return ParseError::speculate_error();
				}
				self.error(format!(
					"Unexpected token `{}`, expected {}",
					self.slice(token.span),
					expected
				))
			}
			Ok(None) => {
				if self.state.contains(ParserState::SPECULATING) {
					return ParseError::speculate_error();
				}
				self.error(format!("Unexpected end of query, expected {}", expected))
			}
		}
	}

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
			return ParseError::speculate_error();
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

	/// Parse
	pub async fn parse_push<P: Parse + Node>(&mut self) -> ParseResult<NodeId<P>> {
		let p = self.parse().await?;
		Ok(self.push(p))
	}

	/// Parses and then pushes a ast node into the ast returning an id entering into a new stack
	/// frame.
	pub async fn parse_enter_push<P: Parse + Node>(&mut self) -> ParseResult<NodeId<P>> {
		let p = self.parse_enter().await?;
		Ok(self.push(p))
	}

	/// Parses and then pushes a ast node into the ast returning an id.
	pub fn parse_sync_push<P: ParseSync + Node>(&mut self) -> ParseResult<NodeId<P>> {
		let p = self.parse_sync()?;
		Ok(self.push(p))
	}

	/// Enter into a new stack context, use when running a recursive function.
	pub async fn enter<R, F>(&mut self, cb: F) -> R
	where
		F: AsyncFnOnce(&mut Self) -> R,
	{
		Stk::enter_run(|_| cb(self)).await
	}

	pub fn lex<T, F>(&mut self, f: F) -> ParseResult<T>
	where
		F: FnOnce(BaseLexer<'source>) -> ParseResult<(BaseLexer<'source>, T)>,
	{
		assert!(self.lex.is_empty(), "Lexing special tokens requires the lexer to be empty");

		let lexer = self.lex.lexer().clone();
		let (lex, t) = f(lexer)?;
		*self.lex.lexer() = lex;
		Ok(t)
	}

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
			ParseError::speculate_error()
		} else {
			ParseError::diagnostic(cb(self).to_owned())
		}
	}

	/// Returns an error with the given message with a snippet pointing to the next token.
	#[cold]
	pub fn error<T>(&mut self, msg: T) -> ParseError
	where
		Cow<'source, str>: From<T>,
	{
		let span = self.peek_span();
		self.with_error(|this| {
			Level::Error
				.title(msg)
				.snippet(this.snippet().annotate(AnnotationKind::Primary.span(span)))
				.to_diagnostic()
		})
	}

	#[cold]
	fn lex_error(&mut self, e: LexError) -> ParseError {
		match e {
			LexError::UnexpectedEof(span) => {
				if self.settings.contains(ParserSettings::PARTIAL) {
					return ParseError::missing_data_error();
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
		if let Some(next) = self.next()? {
			if next.token != delimiter {
				let span = self.peek_span();
				return Err(self.with_error(|this| {
					Level::Error
						.title(format!(
							"Unexpected token `{}`, expected closing delimiter {}",
							this.slice(span),
							delimiter.description()
						))
						.snippet(
							this.snippet().annotate(AnnotationKind::Primary.span(span)).annotate(
								AnnotationKind::Context
									.span(open_span)
									.label("expected this delimiter to close"),
							),
						)
						.to_diagnostic()
				}));
			}
			Ok(next)
		} else {
			return Err(self.with_error(|this| {
				Level::Error
					.title(format!(
						"Unexpected end of query, expected closing delimiter {}",
						delimiter.description()
					))
					.snippet(
						this.snippet()
							.annotate(AnnotationKind::Primary.span(this.eof_span()))
							.annotate(
								AnnotationKind::Context
									.span(open_span)
									.label("expected this delimiter to close"),
							),
					)
					.to_diagnostic()
			}));
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
	pub fn eof_span(&mut self) -> Span {
		self.lex.eof_span()
	}

	#[track_caller]
	pub fn todo<T>(&mut self) -> ParseResult<T> {
		let loc = std::panic::Location::caller();
		Err(self.error(format!(
			"hit an unimplemented path in the parser: {}:{}",
			loc.file(),
			loc.line()
		)))
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
