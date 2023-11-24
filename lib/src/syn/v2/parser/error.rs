use crate::syn::{
	common::Location,
	error::{RenderedError, Snippet},
	v2::{
		lexer::Error as LexError,
		token::{Span, TokenKind},
	},
};

#[derive(Debug)]
pub enum NumberParseError {
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
	/// The parser encountered an unexpected token.
	UnexpectedEof {
		expected: &'static str,
	},
	UnclosedDelimiter {
		expected: TokenKind,
		should_close: Span,
	},
	InvalidNumber {
		error: NumberParseError,
	},
	DisallowedStatement,
	/// The parser encountered an token which could not be lexed correctly.
	InvalidToken(LexError),
	/// A path in the parser which was not yet finished.
	/// Should eventually be removed.
	Todo,
}

#[derive(Debug)]
pub struct ParseError {
	pub kind: ParseErrorKind,
	pub at: Span,
	pub backtrace: std::backtrace::Backtrace,
}

impl ParseError {
	pub fn new(kind: ParseErrorKind, at: Span) -> Self {
		ParseError {
			kind,
			at,
			backtrace: std::backtrace::Backtrace::force_capture(),
		}
	}

	pub fn render_on(&self, source: &str) -> RenderedError {
		println!("FOUND ERROR: {}", self.backtrace);
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
			ParseErrorKind::InvalidNumber {
				ref error,
			} => {
				let text = match error {
					NumberParseError::FloatToInt => {
						"Found a floating point number, expected a integer"
					}
					NumberParseError::DecimalToInt => {
						"Found a large decimal number, expected a integer"
					}
					NumberParseError::IntegerOverflow => "Number exceeded maximum allowed value",
				};
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
