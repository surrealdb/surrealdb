/// A macro for requiring a certain token to be next, returning an error otherwise..
macro_rules! unexpected {
	(@ $span:expr, $parser:expr, $found:expr, $expected:expr $(=> $explain:expr)?) => {{
		unexpected!(@@withSpan, $span, $parser,$found, $expected $(=> $explain)?)
	}};

	($parser:expr, $found:expr, $expected:expr $(=> $explain:expr)?) => {{
		let span = $parser.recent_span();
		unexpected!(@@withSpan, span, $parser,$found, $expected $(=> $explain)?)
	}};

	(@@withSpan, $span:expr, $parser:expr, $found:expr, $expected:expr) => {
		match $found {
			$crate::syn::token::TokenKind::Invalid => {
				let error = $parser.lexer.error.take().unwrap();
				return Err($crate::syn::parser::ParseError::new(
					$crate::syn::parser::ParseErrorKind::InvalidToken(error),
					$span
				));
			}
			$crate::syn::token::TokenKind::Eof => {
				let expected = $expected;
				return Err($crate::syn::parser::ParseError::new(
					$crate::syn::parser::ParseErrorKind::UnexpectedEof {
						expected,
					},
					$span
				));
			}
			x => {
				let expected = $expected;
				return Err($crate::syn::parser::ParseError::new(
					$crate::syn::parser::ParseErrorKind::Unexpected {
						found: x,
						expected,
					},
					$span
				));
			}
		}
	};

	(@@withSpan, $span:expr, $parser:expr, $found:expr, $expected:expr => $explain:expr) => {
		match $found {
			$crate::syn::token::TokenKind::Invalid => {
				let error = $parser.lexer.error.take().unwrap();
				return Err($crate::syn::parser::ParseError::new(
					$crate::syn::parser::ParseErrorKind::InvalidToken(error),
					$span
				));
			}
			$crate::syn::token::TokenKind::Eof => {
				let expected = $expected;
				return Err($crate::syn::parser::ParseError::new(
					$crate::syn::parser::ParseErrorKind::UnexpectedEof {
						expected,
					},
					$span
				));
			}
			x => {
				let expected = $expected;
				return Err($crate::syn::parser::ParseError::new(
					$crate::syn::parser::ParseErrorKind::UnexpectedExplain {
						found: x,
						expected,
						explain: $explain,
					},
					$span
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
			$crate::syn::parser::TokenKind::Invalid => {
				let error = $parser.lexer.error.take().unwrap();
				return Err($crate::syn::parser::ParseError::new(
					$crate::syn::parser::ParseErrorKind::InvalidToken(error),
					$parser.recent_span(),
				));
			}
			x => {
				let expected = $($kind)*.as_str();
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

/// A macro for indicating that the parser encountered an token which it didn't expect.
macro_rules! expected_whitespace {
	($parser:expr, $($kind:tt)*) => {{
		let token = $parser.next_whitespace();
		match token.kind {
			$($kind)* => token,
			$crate::syn::parser::TokenKind::Invalid => {
				let error = $parser.lexer.error.take().unwrap();
				return Err($crate::syn::parser::ParseError::new(
					$crate::syn::parser::ParseErrorKind::InvalidToken(error),
					$parser.recent_span(),
				));
			}
			x => {
				let expected = $($kind)*.as_str();
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

#[cfg(test)]
#[doc(hidden)]
#[macro_export]
macro_rules! test_parse {
	($func:ident$( ( $($e:expr),* $(,)? ))? , $t:expr) => {{
		let mut parser = $crate::syn::parser::Parser::new($t.as_bytes());
		let mut stack = reblessive::Stack::new();
		stack.enter(|ctx| parser.$func(ctx,$($($e),*)*)).finish()
	}};
}

#[doc(hidden)]
#[macro_export]
macro_rules! enter_object_recursion {
	($name:ident = $this:expr => { $($t:tt)* }) => {{
		if $this.object_recursion == 0 {
			return Err($crate::syn::parser::ParseError::new(
				$crate::syn::parser::ParseErrorKind::ExceededObjectDepthLimit,
				$this.last_span(),
			));
		}
		struct Dropper<'a, 'b>(&'a mut $crate::syn::parser::Parser<'b>);
		impl Drop for Dropper<'_, '_> {
			fn drop(&mut self) {
				self.0.object_recursion += 1;
			}
		}
		impl<'a> ::std::ops::Deref for Dropper<'_,'a>{
			type Target = $crate::syn::parser::Parser<'a>;

			fn deref(&self) -> &Self::Target{
				self.0
			}
		}

		impl<'a> ::std::ops::DerefMut for Dropper<'_,'a>{
			fn deref_mut(&mut self) -> &mut Self::Target{
				self.0
			}
		}

		$this.object_recursion -= 1;
		let mut $name = Dropper($this);
		{
			$($t)*
		}
	}};
}

#[macro_export]
macro_rules! enter_query_recursion {
	($name:ident = $this:expr => { $($t:tt)* }) => {{
		if $this.query_recursion == 0 {
			return Err($crate::syn::parser::ParseError::new(
				$crate::syn::parser::ParseErrorKind::ExceededQueryDepthLimit,
				$this.last_span(),
			));
		}
		struct Dropper<'a, 'b>(&'a mut $crate::syn::parser::Parser<'b>);
		impl Drop for Dropper<'_, '_> {
			fn drop(&mut self) {
				self.0.query_recursion += 1;
			}
		}
		impl<'a> ::std::ops::Deref for Dropper<'_,'a>{
			type Target = $crate::syn::parser::Parser<'a>;

			fn deref(&self) -> &Self::Target{
				self.0
			}
		}

		impl<'a> ::std::ops::DerefMut for Dropper<'_,'a>{
			fn deref_mut(&mut self) -> &mut Self::Target{
				self.0
			}
		}

		$this.query_recursion -= 1;
        #[allow(unused_mut)]
		let mut $name = Dropper($this);
		{
			$($t)*
		}
	}};
}

pub(super) use expected;
pub(super) use expected_whitespace;
pub(super) use unexpected;

#[cfg(test)]
pub(super) use test_parse;
