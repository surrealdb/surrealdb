//! SurrealQL token lexer definitions
//!
//! This crate is split out because of the large amount of code logos tends to generate, slowing
//! down compilation as well as tooling for every crate it the generated code is included in.

use common::span::Span;
use logos::Lexer;

#[macro_use]
mod mac;
mod base;
mod uuid;
pub use uuid::UuidToken;
mod version;
pub use version::VersionToken;
mod escaped;
pub use escaped::EscapeTokenKind;

pub use crate::base::BaseTokenKind;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum LexError {
	UnexpectedEof(Span),
	InvalidToken(Span),
}

impl Default for LexError {
	fn default() -> Self {
		LexError::InvalidToken(Span::empty())
	}
}

impl LexError {
	pub fn span(&self) -> Span {
		let (LexError::UnexpectedEof(x) | LexError::InvalidToken(x)) = self;
		*x
	}

	fn from_lexer<'a>(l: &mut Lexer<'a, BaseTokenKind>) -> Self {
		let span = l.span();
		let span = Span::from_range((span.start as u32)..(span.end as u32));

		if l.remainder().is_empty() {
			LexError::UnexpectedEof(span)
		} else {
			LexError::InvalidToken(span)
		}
	}
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Joined {
	Seperated,
	#[default]
	Joined,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[must_use]
pub struct Token {
	/// The kind of token
	pub token: BaseTokenKind,
	/// Whether there was whitespace between this token and the last.
	pub joined: Joined,
	/// The span from which the token originates
	pub span: Span,
}
