use crate::syn::{
	common::Location,
	error::{RenderedError, Snippet},
	lexer::Error as LexError,
	token::{Span, TokenKind},
};
use std::{
	fmt::Write,
	num::{ParseFloatError, ParseIntError},
};

#[derive(Debug)]
#[non_exhaustive]
pub enum IntErrorKind {
	FloatToInt,
	DecimalToInt,
	IntegerOverflow,
}

#[derive(Debug)]
#[non_exhaustive]
pub enum MissingKind {
	Group,
	Split,
	Order,
}

#[derive(Debug)]
#[non_exhaustive]
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
	DisallowedStatement {
		found: TokenKind,
		expected: TokenKind,
		disallowed: Span,
	},
	/// The parser encountered an token which could not be lexed correctly.
	InvalidToken(LexError),
	/// Matched a path which was invalid.
	InvalidPath {
		possibly: Option<&'static str>,
	},
	MissingField {
		field: Span,
		idiom: String,
		kind: MissingKind,
	},
	ExceededObjectDepthLimit,
	ExceededQueryDepthLimit,
	NoWhitespace,
}

/// A parsing error.
#[derive(Debug)]
#[non_exhaustive]
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
	pub fn render_on(&self, source: &str) -> RenderedError {
		Self::render_on_inner(source, &self.kind, self.at)
	}

	/// Create a rendered error from the string this error was generated from.
	pub fn render_on_inner(source: &str, kind: &ParseErrorKind, at: Span) -> RenderedError {
		match &kind {
			ParseErrorKind::Unexpected {
				found,
				expected,
			} => {
				let text = format!("Unexpected token '{}' expected {}", found.as_str(), expected);
				let locations = Location::range_of_span(source, at);
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
				let locations = Location::range_of_span(source, at);
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
				let locations = Location::range_of_span(source, at);
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
				let text = format!("Expected closing delimiter '{}'", expected.as_str());
				let locations = Location::range_of_span(source, at);
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
			ParseErrorKind::DisallowedStatement {
				found,
				expected,
				disallowed,
			} => {
				let text = format!(
					"Unexpected token '{}' expected '{}'",
					found.as_str(),
					expected.as_str()
				);
				let locations = Location::range_of_span(source, at);
				let snippet = Snippet::from_source_location_range(source, locations, None);
				let locations = Location::range_of_span(source, *disallowed);
				let dissallowed_snippet = Snippet::from_source_location_range(
					source,
					locations,
					Some("this keyword is not allowed to start a statement in this position"),
				);
				RenderedError {
					text,
					snippets: vec![snippet, dissallowed_snippet],
				}
			}
			ParseErrorKind::InvalidToken(e) => {
				let text = e.to_string();
				let locations = Location::range_of_span(source, at);
				let snippet = Snippet::from_source_location_range(source, locations, None);
				RenderedError {
					text,
					snippets: vec![snippet],
				}
			}
			ParseErrorKind::InvalidPath {
				possibly,
			} => {
				let mut text = "Invalid function path".to_owned();
				if let Some(p) = possibly {
					// writing into a string never causes an error.
					write!(text, ", did you maybe mean `{}`", p).unwrap();
				}
				let locations = Location::range_of_span(source, at);
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
				let locations = Location::range_of_span(source, at);
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
				let locations = Location::range_of_span(source, at);
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
				let locations = Location::range_of_span(source, at);
				let snippet = Snippet::from_source_location_range(source, locations, None);
				RenderedError {
					text: text.to_string(),
					snippets: vec![snippet],
				}
			}
			ParseErrorKind::NoWhitespace => {
				let text = "Whitespace is dissallowed in this position";
				let locations = Location::range_of_span(source, at);
				let snippet = Snippet::from_source_location_range(source, locations, None);
				RenderedError {
					text: text.to_string(),
					snippets: vec![snippet],
				}
			}
			ParseErrorKind::ExceededObjectDepthLimit => {
				let text = "Parsing exceeded the depth limit for objects";
				let locations = Location::range_of_span(source, at);
				let snippet = Snippet::from_source_location_range(source, locations, None);
				RenderedError {
					text: text.to_string(),
					snippets: vec![snippet],
				}
			}
			ParseErrorKind::ExceededQueryDepthLimit => {
				let text = "Parsing exceeded the depth limit for queries";
				let locations = Location::range_of_span(source, at);
				let snippet = Snippet::from_source_location_range(source, locations, None);
				RenderedError {
					text: text.to_string(),
					snippets: vec![snippet],
				}
			}
			ParseErrorKind::MissingField {
				field,
				idiom,
				kind,
			} => {
				let text = match kind {
					MissingKind::Group => {
						format!("Missing group idiom `{idiom}` in statement selection")
					}
					MissingKind::Split => {
						format!("Missing split idiom `{idiom}` in statement selection")
					}
					MissingKind::Order => {
						format!("Missing order idiom `{idiom}` in statement selection")
					}
				};
				let locations = Location::range_of_span(source, at);
				let snippet_error = Snippet::from_source_location_range(source, locations, None);
				let locations = Location::range_of_span(source, *field);
				let snippet_hint = Snippet::from_source_location_range(
					source,
					locations,
					Some("Idiom missing here"),
				);
				RenderedError {
					text: text.to_string(),
					snippets: vec![snippet_error, snippet_hint],
				}
			}
		}
	}
}
