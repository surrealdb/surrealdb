//! The GQL parser.
//!
//! A recursive descent parser over [`crate::gql::lexer::Lexer`] tokens,
//! mirroring the structure of the SurrealQL parser in [`crate::syn::parser`]:
//! most functions peek a token and decide which production to parse based on
//! it, recursive productions run on a [`reblessive`] stack so that deeply
//! nested input cannot overflow the machine stack, and nesting depth is
//! additionally bounded by [`GqlParserSettings::object_recursion_limit`].
//!
//! The parser implements the grammar subset distilled in
//! `doc/gql/REFERENCE.md` exactly, and recognises the surrounding grammar
//! (write statements, composite queries, path search prefixes, …) only to
//! reject it with precise spans and actionable messages.
//!
//! Token lookahead is at most three tokens (`peek`, `peek1` and `peek2`), the
//! latter two used to disambiguate `p = (…)` path variable declarations,
//! match modes, `IS [NOT] NULL` postfixes and function calls. The grammar
//! subset is `LL(3)`; no backtracking is used.

use reblessive::Stk;

use self::token_buffer::TokenBuffer;
use crate::gql::GqlParserSettings;
use crate::gql::ast::{GqlQuery, Ident};
use crate::gql::lexer::Lexer;
use crate::gql::token::{Span, Token, TokenKind};
use crate::syn::error::{SyntaxError, bail};

mod expr;
pub mod mac;
mod mutation;
mod pattern;
mod stmt;
mod token_buffer;

#[cfg(test)]
mod test;

use mac::unexpected;

/// The result returned by most parser functions.
pub type ParseResult<T> = Result<T, SyntaxError>;

/// The GQL parser.
pub struct Parser<'a> {
	lexer: Lexer<'a>,
	last_span: Span,
	token_buffer: TokenBuffer<4>,
	settings: GqlParserSettings,
}

impl<'a> Parser<'a> {
	/// Create a new parser for the given source.
	///
	/// # Panic
	/// Panics if the source is longer than `u32::MAX` bytes; the parse entry
	/// points in [`crate::gql`] check the length first.
	pub fn new_with_settings(source: &'a str, settings: GqlParserSettings) -> Self {
		Parser {
			lexer: Lexer::new(source),
			last_span: Span::empty(),
			token_buffer: TokenBuffer::new(),
			settings,
		}
	}

	/// Returns the next token and advances the parser one token forward.
	#[expect(clippy::should_implement_trait)]
	pub fn next(&mut self) -> Token {
		let res = self.token_buffer.pop().unwrap_or_else(|| self.lexer.next_token());
		self.last_span = res.span;
		res
	}

	/// Consume the current peeked value and advance the parser one token
	/// forward.
	///
	/// Should only be called after peeking a value.
	pub fn pop_peek(&mut self) -> Token {
		let res = self.token_buffer.pop().expect("token buffer is non-empty");
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

	/// Returns the token kind of the next token without consuming it.
	pub fn peek_kind(&mut self) -> TokenKind {
		self.peek().kind
	}

	/// Returns the next n'th token without consuming it. `peek_token_at(0)`
	/// is equivalent to `peek`.
	fn peek_token_at(&mut self, at: u8) -> Token {
		for _ in self.token_buffer.len()..=at {
			let r = self.lexer.next_token();
			self.token_buffer.push(r);
		}
		self.token_buffer.at(at).expect("token exists at index")
	}

	/// Returns the token after the next token without consuming it.
	pub fn peek1(&mut self) -> Token {
		self.peek_token_at(1)
	}

	/// Returns the second token after the next token without consuming it.
	pub fn peek2(&mut self) -> Token {
		self.peek_token_at(2)
	}

	/// Returns the span of the next token if it was already peeked, otherwise
	/// returns the span of the last consumed token.
	pub fn recent_span(&mut self) -> Span {
		self.token_buffer.first().map(|x| x.span).unwrap_or(self.last_span)
	}

	/// Returns the span of the last consumed token.
	pub fn last_span(&mut self) -> Span {
		self.last_span
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

	/// Checks that the next token is the given closing delimiter and consumes
	/// it, pointing at the opening delimiter otherwise.
	fn expect_closing_delimiter(&mut self, kind: TokenKind, should_close: Span) -> ParseResult<()> {
		let peek = self.peek();
		if peek.kind != kind {
			match peek.kind {
				TokenKind::Invalid => {
					unexpected!(self, peek, "a closing delimiter");
				}
				TokenKind::Eof => {
					bail!(
						"Unexpected end of file, expected the delimiter `{kind}`",
						@peek.span,
						@should_close => "expected this delimiter to close"
					);
				}
				x => {
					bail!(
						"Unexpected token `{x}`, expected the delimiter `{kind}`",
						@peek.span,
						@should_close => "expected this delimiter to close"
					);
				}
			}
		}
		self.pop_peek();
		Ok(())
	}

	/// Returns the source text of a given span.
	pub fn span_str(&self, span: Span) -> &'a str {
		self.lexer.span_str(span)
	}

	/// Parse a full query: the primary entry point of the parser.
	pub async fn parse_query(&mut self, stk: &mut Stk) -> ParseResult<GqlQuery> {
		let program = self.parse_statement(stk).await?;
		let token = self.peek();
		if !token.is_eof() {
			unexpected!(self, token, "the query to end");
		}
		Ok(GqlQuery {
			program,
		})
	}

	/// Returns whether a token can start an identifier: a regular identifier,
	/// a non-reserved keyword, or a `"…"`/`` `…` `` delimited identifier.
	///
	/// Double-quoted tokens are strings *and* delimited identifiers
	/// (`DOUBLE_QUOTED_CHARACTER_SEQUENCE`); in identifier positions they are
	/// identifiers, which this check is used for.
	fn token_can_be_ident(kind: TokenKind) -> bool {
		match kind {
			TokenKind::Identifier
			| TokenKind::DoubleQuoted {
				..
			}
			| TokenKind::AccentQuoted {
				..
			} => true,
			TokenKind::Keyword(keyword) => keyword.is_non_reserved(),
			_ => false,
		}
	}

	/// Parse an identifier in an identifier-required position: a variable
	/// declaration, label name, property key or `AS` alias.
	///
	/// Non-reserved keywords are valid identifiers (`regularIdentifier :
	/// REGULAR_IDENTIFIER | nonReservedWords`), with the original casing
	/// recovered from the source. Reserved and prereserved words are rejected
	/// with a dedicated error. Double-quoted and accent-quoted tokens are
	/// delimited identifiers in this position and are decoded.
	fn parse_ident(&mut self) -> ParseResult<Ident> {
		let token = self.next();
		match token.kind {
			TokenKind::Identifier => Ok(Ident {
				name: self.span_str(token.span).to_owned(),
				span: token.span,
			}),
			TokenKind::Keyword(keyword) => {
				if keyword.is_non_reserved() {
					Ok(Ident {
						name: self.span_str(token.span).to_owned(),
						span: token.span,
					})
				} else {
					bail!(
						"`{}` is a reserved word and cannot be used as an identifier",
						self.span_str(token.span),
						@token.span => "use a `\"…\"` or `` `…` `` delimited identifier instead"
					);
				}
			}
			TokenKind::DoubleQuoted {
				..
			}
			| TokenKind::AccentQuoted {
				..
			} => {
				let name = Lexer::unescape_quoted_span(self.span_str(token.span), token.span)?;
				Ok(Ident {
					name,
					span: token.span,
				})
			}
			_ => unexpected!(self, token, "an identifier"),
		}
	}
}
