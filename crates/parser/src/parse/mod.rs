mod common;
mod expr;
mod top_level_expr;

use std::fmt::Display;
use std::ops::{Deref, DerefMut};

use ::common::{
	TypedError,
	source_error::{AnnotationKind, Group, Level, Snippet, SourceDiagnostic},
	span::Span,
};
use ast::{Ast, Node, NodeId};
use bitflags::bitflags;
use logos::{Lexer, Logos};
use reblessive::{Stack, Stk};

use crate::lex::{BaseTokenKind, LexError, PeekableLexer, Token};

pub type ParseError = TypedError<SourceDiagnostic>;
pub type ParseResult<T> = Result<T, ParseError>;

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

#[derive(Debug, Clone, Copy)]
pub struct Config {
	depth_limit: usize,

	flexible_record_ids: bool,
	generate_warnings: bool,

	feature_references: bool,
	feature_bearer_access: bool,
	feature_define_api: bool,
	feature_files: bool,
	legacy_strands: bool,
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
	pub struct ParserSettings: u8 {
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
		const LOOP = 1 << 0;
	}
}

pub struct Parser<'source, 'ast> {
	lex: PeekableLexer<'source, 4>,
	ast: &'ast mut Ast,
	features: ParserSettings,
	state: ParserState,
}

impl<'source, 'ast> Parser<'source, 'ast> {
	/// Parse a parsable type.
	pub fn enter_parse<P: Parse + 'static>(source: &[u8], config: Config) -> ParseResult<(P, Ast)> {
		let mut ast = Ast::empty();
		let mut stack = Stack::new();
		let node = Self::enter_parse_reuse(source, &mut stack, &mut ast, config)?;
		Ok((node, ast))
	}

	/// Parse a parsable type allowing reusing of resources like an existing stack and ast.
	pub fn enter_parse_reuse<P: Parse + Sized + 'static>(
		source: &[u8],
		stack: &mut Stack,
		ast: &mut Ast,
		config: Config,
	) -> ParseResult<P> {
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
			ast,
			features,
			state: ParserState::empty(),
		};

		// We ignore the stk which is mostly just to ensure the no accidental panics or infinite
		// loops because we can maintain it's savety guarentees within the parser.
		let mut runner = stack.enter(|_| parser.parse());

		loop {
			if let Some(x) = runner.step() {
				return x;
			}

			if runner.depth() > config.depth_limit {
				std::mem::drop(runner);
				return Err(parser.error("Parser hit maximum configured recursion depth"));
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
		F: AsyncFnOnce(&mut Parser) -> ParseResult<Option<T>>,
	{
		let backup = self.lex.clone();
		match cb(self).await {
			Ok(Some(x)) => Ok(Some(x)),
			Ok(None) => {
				self.lex = backup;
				Ok(None)
			}
			Err(e) => {
				self.lex = backup;
				Err(e)
			}
		}
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
			None => Ok(None),
			Some(Err(e)) => Err(Self::lex_error(e)),
		}
	}

	/// Returns the next token in the lexer without consuming it expected a token to be present and
	/// returning an error if the lexer has reached the end of the source.
	pub fn peek_expect(&mut self, expected: &str) -> ParseResult<Token> {
		match self.lex.peek::<0>() {
			Some(Ok(x)) => Ok(x),
			None => Err(self.error(format_args!("Unexpected end of query, expected {expected}"))),
			Some(Err(e)) => Err(Self::lex_error(e)),
		}
	}

	/// Returns the next token after the first in the lexer without consuming it.
	pub fn peek1(&mut self) -> ParseResult<Option<Token>> {
		match self.lex.peek::<1>() {
			Some(Ok(x)) => Ok(Some(x)),
			None => Ok(None),
			Some(Err(e)) => Err(Self::lex_error(e)),
		}
	}

	/// Returns the next token after the first two tokens in the lexer without consuming it.
	pub fn peek2(&mut self) -> ParseResult<Option<Token>> {
		match self.lex.peek::<2>() {
			Some(Ok(x)) => Ok(Some(x)),
			None => Ok(None),
			Some(Err(e)) => Err(Self::lex_error(e)),
		}
	}

	/// Consumes the next token in the lexer and returns it.
	pub fn next(&mut self) -> ParseResult<Option<Token>> {
		match self.lex.next() {
			Some(Ok(x)) => Ok(Some(x)),
			None => Ok(None),
			Some(Err(e)) => Err(Self::lex_error(e)),
		}
	}

	/// Returns the next token, expecting a token to be present and returning an error if the
	/// lexer has reached the end of the source.
	pub fn next_expect(&mut self, expected: &str) -> ParseResult<Token> {
		if let Some(x) = self.next()? {
			Ok(x)
		} else {
			Err(self.error(format_args!("Unexpected end of query, expected {expected}")))
		}
	}

	/// Consumes the next token and returns it, if the token has the same kind as the argument.
	pub fn eat(&mut self, kind: BaseTokenKind) -> ParseResult<Option<Token>> {
		let peek = self.peek()?;
		if let Some(token) = peek {
			if token.token == kind {
				self.lex.pop_peek();
				return Ok(Some(token));
			}
		}
		Ok(None)
	}

	/// Expect a specific token to be next in the lexer, returning an error if this is not the case
	/// and the token if it is.
	pub fn expect(&mut self, kind: BaseTokenKind) -> ParseResult<Token> {
		let Some(token) = self.peek()? else {
			return Err(self.unexpected(kind.as_str()));
		};
		if token.token != kind {
			return Err(self.unexpected(kind.as_str()));
		}
		self.lex.pop_peek();
		Ok(token)
	}

	/// Returns the error marking the next token in the parser to be an unexpected token.
	#[cold]
	pub fn unexpected(&mut self, expected: &str) -> ParseError {
		match self.peek() {
			Err(e) => e,
			Ok(Some(token)) => self.error(format_args!(
				"Unexpected token `{}`, expected `{}`",
				self.slice(token.span),
				expected
			)),
			Ok(None) => {
				self.error(format_args!("Unexpected end of query, expected `{}`", expected))
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

		let message = match peek {
			Some(token) => {
				format!("Unexpected token `{}`, expected `{}`", self.slice(token.span), expected)
			}
			None => format!("Unexpected token end of query, expected `{}`", expected),
		};

		Level::Error
			.title(message)
			.element(
				Snippet::base().annotate(
					AnnotationKind::Primary.span(self.peek_span()).label(label.to_string()),
				),
			)
			.into()
	}

	/// Access the lexer within the parser.
	///
	/// Can be used to use the lexer in cases where the base tokens are not sufficient for parsing.
	///
	/// # Panic
	/// This function will panic if the lexer has peeked tokens and thus the lexer cannot be used
	/// currently.
	pub fn lex<F, R>(&mut self, mut f: F) -> ParseResult<R>
	where
		F: FnMut(&mut Lexer<'source, BaseTokenKind>) -> ParseResult<R>,
	{
		assert!(self.lex.is_empty(), "Tried to access lexer with active peeked tokens");
		f(self.lex.lexer())
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

	/// Returns sub string of full source that corresponds to the given span.
	pub fn slice(&self, span: Span) -> &'source str {
		self.lex.slice(span)
	}

	/// A tiny function, handing the current span to a callback.
	/// Mostly usefully because of it's cold annotation, meaning it's use will automatically flag a
	/// branch as being unlikely to be taken, and can be optimized with that assumption.
	#[cold]
	pub fn with_error<F>(&mut self, cb: F) -> ParseError
	where
		F: FnOnce(&mut Self, Span) -> Group,
	{
		let span = self.peek_span();
		cb(self, span).into()
	}

	/// Returns an error with the given message with a snippet pointing to the next token.
	#[cold]
	pub fn error<T: Display>(&mut self, msg: T) -> ParseError {
		Level::Error
			.title(msg.to_string())
			.element(Snippet::base().annotate(AnnotationKind::Primary.span(self.peek_span())))
			.into()
	}

	#[cold]
	fn lex_error(e: LexError) -> ParseError {
		match e {
			LexError::UnexpectedEof(span) => Level::Error
				.title("Unexpected end of query while lexing a token")
				.element(Snippet::base().annotate(AnnotationKind::Primary.span(span)))
				.into(),
			LexError::InvalidUtf8(span) => Level::Error
				.title("Found invalid utf-8 character code")
				.element(Snippet::base().annotate(AnnotationKind::Primary.span(span)))
				.into(),
			LexError::InvalidToken(span) => Level::Error
				.title("Invalid token")
				.element(Snippet::base().annotate(AnnotationKind::Primary.span(span)))
				.into(),
		}
	}

	/// Tries to eat the next token, returning an missing delimiter error if it is not the correct
	/// token.
	pub fn expect_closing_delimiter(
		&mut self,
		delimiter: BaseTokenKind,
		open_span: Span,
	) -> ParseResult<()> {
		let next = self.next_expect(delimiter.as_str())?;
		if next.token != delimiter {
			return Err(self.with_error(|this, span| {
				Level::Error
					.title(format!(
						"Unexpected token `{}`, expected closing delimiter `{}`",
						this.slice(span),
						delimiter
					))
					.element(
						Snippet::base().annotate(AnnotationKind::Primary.span(span)).annotate(
							AnnotationKind::Context
								.span(open_span)
								.label("expected this delimiter to close"),
						),
					)
					.into()
			}));
		}
		Ok(())
	}

	/// Returns the span for the next token in the lexer.
	pub fn peek_span(&mut self) -> Span {
		self.lex.peek_span()
	}

	/// Returns the span of that points to the end of the source.
	pub fn eof_span(&mut self) -> Span {
		todo!()
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
