/// A macro for requiring a certain token to be next, returning an error otherwise..
macro_rules! unexpected {
	($parser:expr, $found:expr, $expected:expr) => {
		match $found {
			$crate::syn::v2::token::TokenKind::Invalid => {
				let error = $parser.lexer.error.take().unwrap();
				return Err($crate::syn::v2::parser::ParseError::new(
					$crate::syn::v2::parser::ParseErrorKind::InvalidToken(error),
					$parser.last_span(),
				));
			}
			$crate::syn::v2::token::TokenKind::Eof => {
				let expected = $expected;
				return Err($crate::syn::v2::parser::ParseError::new(
					$crate::syn::v2::parser::ParseErrorKind::UnexpectedEof {
						expected,
					},
					$parser.last_span(),
				));
			}
			x => {
				let expected = $expected;
				return Err($crate::syn::v2::parser::ParseError::new(
					$crate::syn::v2::parser::ParseErrorKind::Unexpected {
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
	($parser:expr, $($kind:tt)*) => {{
		let token = $parser.next();
		match token.kind {
			$($kind)* => token,
			$crate::syn::v2::parser::TokenKind::Invalid => {
				let error = $parser.lexer.error.take().unwrap();
				return Err($crate::syn::v2::parser::ParseError::new(
					$crate::syn::v2::parser::ParseErrorKind::InvalidToken(error),
					$parser.last_span(),
				));
			}
			x => {
				let expected = $($kind)*.as_str();
				let kind = if let $crate::syn::v2::token::TokenKind::Eof = x {
					$crate::syn::v2::parser::ParseErrorKind::UnexpectedEof {
						expected,
					}
				} else {
					$crate::syn::v2::parser::ParseErrorKind::Unexpected {
						found: x,
						expected,
					}
				};

				return Err($crate::syn::v2::parser::ParseError::new(kind, $parser.last_span()));
			}
		}
	}};
}

#[cfg(test)]
macro_rules! test_parse {
	($func:ident$( ( $($e:expr),* $(,)? ))? , $t:literal) => {{
		let mut parser = $crate::syn::v2::parser::Parser::new($t.as_bytes());
		parser.$func($($($e),*)*)
	}};
}

pub(super) use expected;
pub(super) use unexpected;

#[cfg(test)]
pub(super) use test_parse;
