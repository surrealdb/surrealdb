use crate::syn::{
	common::Location,
	error::{RenderedError, Snippet},
	v2::{
		lexer::Error as LexError,
		token::{Span, TokenKind},
	},
};
use std::{
	fmt::Write,
	num::{ParseFloatError, ParseIntError},
};

#[derive(Debug)]
pub enum IntErrorKind {
	FloatToInt,
	DecimalToInt,
	IntegerOverflow,
}

#[derive(Debug)]
pub enum ParseErrorKind {
	/// The parser encountered an unexpected token.
	Unexpected {
		found: TokenKind,
		expected: &'static str,
	},
	UnexpectedExplain {
		found: TokenKind,
		expected: &'static str,
		explain: &'static str,
	},
	/// The parser encountered an unexpected token.
	UnexpectedEof {
		expected: &'static str,
	},
	/// An error for an unclosed delimiter with a span of the token which should be closed.
	UnclosedDelimiter {
		expected: TokenKind,
		should_close: Span,
	},
	/// An error for parsing an integer
	InvalidInteger {
		error: ParseIntError,
	},
	/// An error for parsing an float
	InvalidFloat {
		error: ParseFloatError,
	},
	/// An error for parsing an decimal.
	InvalidDecimal {
		error: rust_decimal::Error,
	},
	DisallowedStatement,
	/// The parser encountered an token which could not be lexed correctly.
	InvalidToken(LexError),
	/// Matched a path which was invalid.
	InvalidPath {
		possibly: Option<&'static str>,
	},
	NoWhitespace,
	/// A path in the parser which was not yet finished.
	/// Should eventually be removed.
	Todo,
}

/// A parsing error.
#[derive(Debug)]
pub struct ParseError {
	pub kind: ParseErrorKind,
	pub at: Span,
}

impl ParseError {
	/// Create a new parse error.
	pub fn new(kind: ParseErrorKind, at: Span) -> Self {
		ParseError {
			kind,
			at,
		}
	}

	/// Create a rendered error from the string this error was generated from.
	pub fn render_on(&self, source: &str) -> RenderedError {
		match &self.kind {
			ParseErrorKind::Unexpected {
				found,
				expected,
			} => {
				let text = format!("Unexpected token '{}' expected {}", found.as_str(), expected);
				let locations = Location::range_of_span(source, self.at);
				let snippet = Snippet::from_source_location_range(source, locations, None);
				RenderedError {
					text,
					snippets: vec![snippet],
				}
			}
			ParseErrorKind::UnexpectedExplain {
				found,
				expected,
				explain,
			} => {
				let text = format!("Unexpected token '{}' expected {}", found.as_str(), expected);
				let locations = Location::range_of_span(source, self.at);
				let snippet = Snippet::from_source_location_range(source, locations, Some(explain));
				RenderedError {
					text,
					snippets: vec![snippet],
				}
			}
			ParseErrorKind::UnexpectedEof {
				expected,
			} => {
				let text = format!("Query ended early, expected {}", expected);
				let locations = Location::range_of_span(source, self.at);
				let snippet = Snippet::from_source_location_range(source, locations, None);
				RenderedError {
					text,
					snippets: vec![snippet],
				}
			}
			ParseErrorKind::UnclosedDelimiter {
				expected,
				should_close,
			} => {
				let text = format!("Expected closing delimiter {}", expected.as_str());
				let locations = Location::range_of_span(source, self.at);
				let snippet = Snippet::from_source_location_range(source, locations, None);
				let locations = Location::range_of_span(source, *should_close);
				let close_snippet = Snippet::from_source_location_range(
					source,
					locations,
					Some("Expected this delimiter to close"),
				);
				RenderedError {
					text,
					snippets: vec![snippet, close_snippet],
				}
			}
			ParseErrorKind::DisallowedStatement => {
				let text = "This statement is not allowed in this location".to_owned();
				let locations = Location::range_of_span(source, self.at);
				let snippet = Snippet::from_source_location_range(source, locations, None);
				RenderedError {
					text,
					snippets: vec![snippet],
				}
			}
			ParseErrorKind::InvalidToken(e) => {
				let text = e.to_string();
				let locations = Location::range_of_span(source, self.at);
				let snippet = Snippet::from_source_location_range(source, locations, None);
				RenderedError {
					text,
					snippets: vec![snippet],
				}
			}
			ParseErrorKind::Todo => {
				let text = "Parser hit not yet implemented path".to_string();
				let locations = Location::range_of_span(source, self.at);
				let snippet = Snippet::from_source_location_range(source, locations, None);
				RenderedError {
					text,
					snippets: vec![snippet],
				}
			}
			ParseErrorKind::InvalidPath {
				possibly,
			} => {
				let mut text = "Invalid path".to_owned();
				if let Some(p) = possibly {
					// writing into a string never causes an error.
					write!(text, ", did you maybe mean `{}`", p).unwrap();
				}
				let locations = Location::range_of_span(source, self.at);
				let snippet = Snippet::from_source_location_range(
					source,
					locations,
					Some("This path does not exist."),
				);
				RenderedError {
					text,
					snippets: vec![snippet],
				}
			}
			ParseErrorKind::InvalidInteger {
				ref error,
			} => {
				let text = format!("failed to parse integer, {error}");
				let locations = Location::range_of_span(source, self.at);
				let snippet = Snippet::from_source_location_range(source, locations, None);
				RenderedError {
					text: text.to_string(),
					snippets: vec![snippet],
				}
			}
			ParseErrorKind::InvalidFloat {
				ref error,
			} => {
				let text = format!("failed to parse floating point, {error}");
				let locations = Location::range_of_span(source, self.at);
				let snippet = Snippet::from_source_location_range(source, locations, None);
				RenderedError {
					text: text.to_string(),
					snippets: vec![snippet],
				}
			}
			ParseErrorKind::InvalidDecimal {
				ref error,
			} => {
				let text = format!("failed to parse decimal number, {error}");
				let locations = Location::range_of_span(source, self.at);
				let snippet = Snippet::from_source_location_range(source, locations, None);
				RenderedError {
					text: text.to_string(),
					snippets: vec![snippet],
				}
			}
			ParseErrorKind::NoWhitespace => {
				let text = "Whitespace is dissallowed in this position";
				let locations = Location::range_of_span(source, self.at);
				let snippet = Snippet::from_source_location_range(source, locations, None);
				RenderedError {
					text: text.to_string(),
					snippets: vec![snippet],
				}
			}
		}
	}
}
