/// A macro for requiring a certain token to be next, returning an error otherwise..
macro_rules! unexpected {
	($parser:expr, $found:expr, $expected:expr) => {
		match $found {
			$crate::syn::token::TokenKind::Invalid => {
				let error = $parser.lexer.error.take().unwrap();
				return Err($crate::syn::parser::ParseError::new(
					$crate::syn::parser::ParseErrorKind::InvalidToken(error),
					$parser.last_span(),
				));
			}
			$crate::syn::token::TokenKind::Eof => {
				let expected = $expected;
				return Err($crate::syn::parser::ParseError::new(
					$crate::syn::parser::ParseErrorKind::UnexpectedEof {
						expected,
					},
					$parser.last_span(),
				));
			}
			x => {
				let expected = $expected;
				return Err($crate::syn::parser::ParseError::new(
					$crate::syn::parser::ParseErrorKind::Unexpected {
						found: x,
						expected,
					},
					$parser.last_span(),
				));
			}
		}
	};
}

/// A macro for indicating that the parser encountered an token which it didn't expect.
macro_rules! expected {
	($parser:expr, $kind:tt) => {{
		let token = $parser.next();
		match token.kind {
			t!($kind) => token,
			$crate::syn::parser::TokenKind::Invalid => {
				let error = $parser.lexer.error.take().unwrap();
				return Err($crate::syn::parser::ParseError::new(
					$crate::syn::parser::ParseErrorKind::InvalidToken(error),
					$parser.last_span(),
				));
			}
			x => {
				let expected = $kind;
				let kind = if let $crate::syn::token::TokenKind::Eof = x {
					$crate::syn::parser::ParseErrorKind::UnexpectedEof {
						expected,
					}
				} else {
					$crate::syn::parser::ParseErrorKind::Unexpected {
						found: x,
						expected,
					}
				};

				return Err($crate::syn::parser::ParseError::new(kind, $parser.last_span()));
			}
		}
	}};
}

/// A macro for indicating a path in the parser which is not yet implemented.
macro_rules! to_do {
	($parser:expr) => {
		return Err($crate::syn::parser::ParseError::new(
			$crate::syn::parser::ParseErrorKind::Todo,
			$parser.last_span(),
		))
	};
}

#[cfg(test)]
macro_rules! test_parse {
	($func:ident$( ( $($e:expr),* $(,)? ))? , $t:literal) => {{
		let mut parser = $crate::syn::parser::Parser::new($t.as_bytes());
		parser.$func($($($e),*)*)
	}};
}

pub(super) use expected;
pub(super) use to_do;
pub(super) use unexpected;

#[cfg(test)]
pub(super) use test_parse;
