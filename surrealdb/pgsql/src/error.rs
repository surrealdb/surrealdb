//! Error types for PostgreSQL lexing and parsing.

use std::fmt;

use thiserror::Error;

use crate::span::Span;

/// An error encountered during lexing or parsing.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub struct ParseError {
	/// Span in the source text where the error occurred.
	pub span: Span,
	/// Category and details of the error.
	pub kind: ErrorKind,
}

impl ParseError {
	/// Create a new parse error.
	pub const fn new(span: Span, kind: ErrorKind) -> Self {
		Self {
			span,
			kind,
		}
	}
}

impl fmt::Display for ParseError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "at {}: {}", self.span, self.kind)
	}
}

/// Category of lexer or parser failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ErrorKind {
	/// Unexpected character encountered during lexing.
	#[error("unexpected character: '{0}'")]
	UnexpectedChar(char),

	/// Unterminated quoted identifier.
	#[error("unterminated quoted identifier")]
	UnterminatedQuotedIdent,

	/// Unterminated string literal.
	#[error("unterminated string literal")]
	UnterminatedString,

	/// Unterminated block comment.
	#[error("unterminated block comment")]
	UnterminatedBlockComment,

	/// Unterminated dollar-quoted string literal.
	#[error("unterminated dollar-quoted string literal with tag '${0}$'")]
	UnterminatedDollarString(String),

	/// Invalid dollar quote tag.
	#[error("invalid dollar quote tag")]
	InvalidDollarTag,

	/// Unterminated bit-string literal.
	#[error("unterminated bit string literal")]
	UnterminatedBitString,

	/// Invalid digit in numeric or bit-string literal.
	#[error("invalid digit in literal")]
	InvalidDigit,

	/// Numeric literal overflowed numeric bounds.
	#[error("numeric literal overflow: {0}")]
	NumericOverflow(String),

	/// Unexpected token encountered during parsing.
	#[error("unexpected token: expected {expected}, found {found}")]
	UnexpectedToken {
		/// Description of the expected token.
		expected: &'static str,
		/// Description of the actual token found.
		found: String,
	},

	/// Unexpected end of input.
	#[error("unexpected end of input: expected {0}")]
	UnexpectedEof(&'static str),

	/// Recursion depth limit exceeded.
	#[error("parser recursion depth limit exceeded")]
	RecursionLimitExceeded,

	/// Invalid operator or expression syntax.
	#[error("invalid expression syntax: {0}")]
	InvalidExpression(&'static str),
}

/// Convenience alias for lexer/parser results.
pub type ParseResult<T> = Result<T, ParseError>;
