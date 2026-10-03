//! Parser error types and result alias for surrealdb-mysql.

use crate::lexer::span::Span;

/// Result type returned by parsing operations.
pub type ParseResult<T> = Result<T, ParseError>;

/// An error encountered while parsing MySQL SQL.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("{kind} at byte range {span:?}")]
pub struct ParseError {
	pub span: Span,
	pub kind: ErrorKind,
}

impl ParseError {
	pub fn new(span: Span, kind: ErrorKind) -> Self {
		Self {
			span,
			kind,
		}
	}
}

/// Category of parsing error.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ErrorKind {
	#[error("Unexpected token: expected {expected}, found {found}")]
	UnexpectedToken {
		expected: &'static str,
		found: String,
	},

	#[error("Unexpected end of input, expected {expected}")]
	UnexpectedEof {
		expected: &'static str,
	},

	#[error("Recursion limit exceeded")]
	RecursionLimitExceeded,

	#[error("Lexer error: {0}")]
	Lexer(String),

	#[error("{0}")]
	Custom(String),
}
