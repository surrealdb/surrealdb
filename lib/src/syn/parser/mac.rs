/// A macro for requiring a certain token to be next, returning an error otherwise..
macro_rules! unexpected {
	($parser:expr, $found:expr, $expected:expr) => {
		match $found {
			$crate::syn::token::TokenKind::Invalid => {
				return Err($crate::syn::parser::ParseError {
					kind: $crate::syn::parser::ParseErrorKind::InvalidToken,
					at: $parser.last_span(),
				});
			}
			$crate::syn::token::TokenKind::Eof => {
				let expected = $expected;
				return Err($crate::syn::parser::ParseError {
					kind: $crate::syn::parser::ParseErrorKind::UnexpectedEof {
						expected,
					},
					at: $parser.last_span(),
				});
			}
			x => {
				let expected = $expected;
				return Err($crate::syn::parser::ParseError {
					kind: $crate::syn::parser::ParseErrorKind::Unexpected {
						found: x,
						expected,
					},
					at: $parser.last_span(),
				});
			}
		}
	};
}

/// A macro for indicating that the parser encountered an token which it didn't expect.
macro_rules! expected {
	($parser:expr, $kind:tt) => {
		match $parser.next_token().kind {
			t!($kind) => {}
			$crate::syn::parser::TokenKind::Invalid => {
				return Err($crate::syn::parser::ParseError {
					kind: $crate::syn::parser::ParseErrorKind::InvalidToken,
					at: $parser.last_span(),
				})
			}
			$crate::syn::token::TokenKind::Eof => {
				let expected = $kind;
				return Err($crate::syn::parser::ParseError {
					kind: $crate::syn::parser::ParseErrorKind::UnexpectedEof {
						expected,
					},
					at: $parser.last_span(),
				});
			}
			x => {
				let expected = $kind;
				return Err($crate::syn::parser::ParseError {
					kind: $crate::syn::parser::ParseErrorKind::Unexpected {
						found: x,
						expected,
					},
					at: $parser.last_span(),
				});
			}
		}
	};
}

/// A macro for indicating a path in the parser which is not yet implemented.
macro_rules! to_do {
	($parser:expr) => {
		return Err($crate::syn::parser::ParseError {
			kind: $crate::syn::parser::ParseErrorKind::Todo,
			at: $parser.last_span(),
		})
	};
}

pub(super) use expected;
pub(super) use to_do;
pub(super) use unexpected;
