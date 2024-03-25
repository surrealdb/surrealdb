/// A macro for requiring a certain token to be next, returning an error otherwise..
macro_rules! unexpected {
	($parser:expr, $found:expr, $expected:expr) => {
		match $found {
			$crate::syn::v2::token::TokenKind::Invalid => {
				let error = $parser.lexer.error.take().unwrap();
				return Err($crate::syn::v2::parser::ParseError::new(
					$crate::syn::v2::parser::ParseErrorKind::InvalidToken(error),
					$parser.recent_span(),
				));
			}
			$crate::syn::v2::token::TokenKind::Eof => {
				let expected = $expected;
				return Err($crate::syn::v2::parser::ParseError::new(
					$crate::syn::v2::parser::ParseErrorKind::UnexpectedEof {
						expected,
					},
					$parser.recent_span(),
				));
			}
			x => {
				let expected = $expected;
				return Err($crate::syn::v2::parser::ParseError::new(
					$crate::syn::v2::parser::ParseErrorKind::Unexpected {
						found: x,
						expected,
					},
					$parser.recent_span(),
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
					$parser.recent_span(),
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
#[macro_export]
macro_rules! test_parse {
	($func:ident$( ( $($e:expr),* $(,)? ))? , $t:literal) => {{
		let mut parser = $crate::syn::v2::parser::Parser::new($t.as_bytes());
		let mut stack = reblessive::Stack::new();
		stack.enter(|ctx| parser.$func(ctx,$($($e),*)*)).finish()
	}};
}

#[macro_export]
macro_rules! enter_object_recursion {
	($name:ident = $this:expr => { $($t:tt)* }) => {{
		if $this.object_recursion == 0 {
			return Err($crate::syn::v2::parser::ParseError::new(
				$crate::syn::v2::parser::ParseErrorKind::ExceededObjectDepthLimit,
				$this.last_span(),
			));
		}
		struct Dropper<'a, 'b>(&'a mut $crate::syn::v2::parser::Parser<'b>);
		impl Drop for Dropper<'_, '_> {
			fn drop(&mut self) {
				self.0.object_recursion += 1;
			}
		}
		impl<'a> ::std::ops::Deref for Dropper<'_,'a>{
			type Target = $crate::syn::v2::parser::Parser<'a>;

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

        println!("{} = {}",$this.query_recursion, std::backtrace::Backtrace::force_capture());
		if $this.query_recursion == 0 {
			return Err($crate::syn::v2::parser::ParseError::new(
				$crate::syn::v2::parser::ParseErrorKind::ExceededQueryDepthLimit,
				$this.last_span(),
			));
		}
		struct Dropper<'a, 'b>(&'a mut $crate::syn::v2::parser::Parser<'b>);
		impl Drop for Dropper<'_, '_> {
			fn drop(&mut self) {
				self.0.query_recursion += 1;
			}
		}
		impl<'a> ::std::ops::Deref for Dropper<'_,'a>{
			type Target = $crate::syn::v2::parser::Parser<'a>;

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
pub(super) use unexpected;

#[cfg(test)]
pub(super) use test_parse;
