use ast::Span;
use base::Joined;

#[macro_use]
mod base;
mod basic;
pub use base::{BaseTokenKind, LexError};
mod peekable;
pub use peekable::PeekableLexer;

#[cfg(test)]
mod test;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct Token {
	/// The kind of token
	pub token: BaseTokenKind,
	/// Whether there was whitespace between this token and the last.
	pub joined: Joined,
	/// The span from which the token originates
	pub span: Span,
}
