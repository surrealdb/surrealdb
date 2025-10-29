mod expr;

use std::fmt::Display;
use std::ops::{Deref, DerefMut};

use ast::{Ast, Node, NodeId, Span};
use bitflags::bitflags;
use logos::{Lexer, Logos};
use reblessive::{Stack, Stk};

use crate::error::ParseError;
use crate::lex::{BaseTokenKind, PeekableLexer, Token};

pub type ParseResult<T> = Result<T, ParseError>;

/// A trait for types which can be individually parsed.
pub trait Parse: Sized {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self>;
}

impl<P: ParseSync> Parse for P {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		P::parse_sync(parser)
	}
}

/// A trait for types which can be individually parsed and require no recursion.
/// Faster to call as it doesn't require a future.
pub trait ParseSync: Sized {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self>;
}

#[derive(Debug, Clone, Copy)]
pub struct Config {
	depth_limit: usize,
	feature_references: bool,
	feature_bearer_access: bool,
	feature_define_api: bool,
	feature_files: bool,
	legacy_strands: bool,
	flexible_record_ids: bool,
}

bitflags! {
	pub struct ParserFeatures: u8 {
		const LEGACY_STRAND =      1 << 0;
		const FLEXIBLE_RECORD_ID = 1 << 1;
		const FEAT_REFERENCES =    1 << 2;
		const FEAT_BEARER_ACCESS = 1 << 3;
		const FEAT_DEFINE_API =    1 << 4;
		const FEAT_FILES =         1 << 5;
	}
}

bitflags! {
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
	config: Config,
	features: ParserFeatures,
	state: ParserState,
}

impl<'source, 'ast> Parser<'source, 'ast> {
	pub fn enter_parse<P: Parse + 'static>(source: &[u8], config: Config) -> ParseResult<(P, Ast)> {
		let mut ast = Ast::empty();
		let mut stack = Stack::new();
		let node = Self::enter_parse_reuse(source, &mut stack, &mut ast, config)?;
		Ok((node, ast))
	}

	pub fn enter_parse_reuse<P: Parse + Sized + 'static>(
		source: &[u8],
		stack: &mut Stack,
		ast: &mut Ast,
		config: Config,
	) -> ParseResult<P> {
		ast.clear();
		let lex = BaseTokenKind::lexer(source);
		let lex = PeekableLexer::new(lex);

		let mut features = ParserFeatures::empty();

		if config.legacy_strands {
			features |= ParserFeatures::LEGACY_STRAND;
		}

		// We ignore the stk which is mostly just to ensure the no accidental panics or infinite
		// loops because we can maintain it's savety guarentees within the parser.
		let mut runner = stack.enter(async |_stk| {
			Parser {
				lex,
				ast,
				config,
				features,
				state: ParserState::empty(),
			}
			.parse()
			.await
		});

		loop {
			if let Some(x) = runner.step() {
				return x;
			}

			if runner.depth() > config.depth_limit {
				todo!()
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
	/// next token.
	///
	/// # Note
	/// This function is very powerfull but also has the drawbacks.
	/// - First it enables ambigous grammer, when implementing new syntax using this function please
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

	/// Returns the next token in the lexer without consuming it.
	pub fn peek(&mut self) -> ParseResult<Option<Token>> {
		match self.lex.peek::<0>() {
			Some(Ok(x)) => Ok(Some(x)),
			None => Ok(None),
			Some(Err(e)) => e.into(),
		}
	}

	/// Returns the next token in the lexer without consuming it.
	pub fn peek_expect(&mut self, expected: &str) -> ParseResult<Token> {
		match self.lex.peek::<0>() {
			Some(Ok(x)) => Ok(x),
			None => Ok(self.error(format_args!("Unexpected end of query, expected {expected}"))),
			Some(Err(e)) => e.into(),
		}
	}

	/// Returns the next token after the first in the lexer without consuming it.
	pub fn peek1(&mut self) -> ParseResult<Option<Token>> {
		match self.lex.peek::<2>() {
			Some(Ok(x)) => Ok(Some(x)),
			None => Ok(None),
			Some(Err(_)) => todo!(),
		}
	}

	/// Returns the next token after the first two tokens in the lexer without consuming it.
	pub fn peek2(&mut self) -> ParseResult<Option<Token>> {
		match self.lex.peek::<2>() {
			Some(Ok(x)) => Ok(Some(x)),
			None => Ok(None),
			Some(Err(_)) => todo!(),
		}
	}

	/// Consumes the next token in the lexer and returns it.
	pub fn next(&mut self) -> ParseResult<Option<Token>> {
		match self.lex.next() {
			Some(Ok(x)) => Ok(Some(x)),
			None => Ok(None),
			Some(Err(_)) => todo!(),
		}
	}

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

	pub fn expect(&mut self, kind: BaseTokenKind) -> ParseResult<Token> {
		let Some(token) = self.next()? else {
			return Err(self.error(format_args!("Unexpected end of query, expected `{:?}`", kind)));
		};
		if token.token != kind {
			return Err(self.error(format_args!(
				"Unexpected token `{;?}`, expected `{:?}`",
				token.token, kind
			)));
		}
		Ok(token)
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

	/// Parse
	pub async fn parse<P: Parse>(&mut self) -> ParseResult<P> {
		P::parse(self).await
	}

	pub async fn parse_enter<P: Parse>(&mut self) -> ParseResult<P> {
		Stk::enter_run(|_| P::parse(self)).await
	}

	pub fn parse_sync<P: ParseSync>(&mut self) -> ParseResult<P> {
		P::parse_sync(self)
	}

	/// Parse
	pub async fn parse_push<P: Parse + Node>(&mut self) -> ParseResult<NodeId<P>> {
		let p = self.parse().await?;
		Ok(self.push(p))
	}

	pub async fn parse_enter_push<P: Parse + Node>(&mut self) -> ParseResult<NodeId<P>> {
		let p = self.parse_enter().await?;
		Ok(self.push(p))
	}

	pub fn parse_sync_push<P: ParseSync + Node>(&mut self) -> ParseResult<NodeId<P>> {
		let p = self.parse_sync()?;
		Ok(self.push(p))
	}

	pub fn slice(&self, span: Span) -> &'source str {
		self.lex.slice(span)
	}

	#[cold]
	pub fn error<T: Display>(&mut self, msg: T) -> ParseError {
		ParseError {
			span: self.lex.peek_span(),
			message: msg.to_string(),
		}
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
