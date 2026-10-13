//! Macros for the GQL parser, adapted from [`crate::syn::parser::mac`].

/// A macro for returning an error when an unexpected token was found.
///
/// Takes the parser, the unexpected token and an expression explaining what
/// was expected instead. Handles `Invalid` tokens by surfacing the lexer
/// error and `Eof` tokens with a dedicated message. The span of the token is
/// attached to the error.
macro_rules! unexpected {
	($parser:expr_2021, $found:expr_2021, $expected:expr_2021 $(, $($t:tt)* )?) => {{
		let __found: $crate::gql::token::Token = $found;
		match __found.kind {
			$crate::gql::token::TokenKind::Invalid => {
				// The lexer always stores an error when it produces an
				// invalid token, but fall back to a generic error rather
				// than panicking if it somehow did not.
				let __error = $parser.lexer.error.take().unwrap_or_else(|| {
					$crate::syn::error::syntax_error!("Invalid token", @__found.span)
				});
				return Err(__error);
			}
			$crate::gql::token::TokenKind::Eof => {
				let __error = $crate::syn::error::syntax_error!("Unexpected end of file, expected {}", $expected, @__found.span $( $($t)* )?);
				return Err(__error);
			}
			x => {
				$crate::syn::error::bail!("Unexpected token `{}`, expected {}", x, $expected, @__found.span $( $($t)* )?)
			}
		}
	}};
}

/// A macro asserting that the next token is of the given kind, returning the
/// token if it is and an error otherwise.
macro_rules! expected {
	($parser:expr_2021, $($kind:tt)*) => {{
		let token: $crate::gql::token::Token = $parser.next();
		if let $($kind)* = token.kind {
			token
		} else {
			$crate::gql::parser::mac::unexpected!($parser, token, $($kind)*)
		}
	}};
}

/// Guards a scope against exceeding the expression nesting depth limit,
/// returning an error when the limit is exceeded. Within the scope `$name` is
/// the parser with one less recursion budget; the budget is restored on drop.
macro_rules! enter_object_recursion {
	($name:ident = $this:expr_2021 => { $($t:tt)* }) => {{
		if $this.settings.object_recursion_limit == 0 {
			return Err($crate::syn::error::SyntaxError::new(
				"Exceeded query expression nesting depth limit",
			)
			.with_span($this.last_span(), $crate::syn::error::MessageKind::Error));
		}
		struct Dropper<'a, 'b>(&'a mut $crate::gql::parser::Parser<'b>);
		impl Drop for Dropper<'_, '_> {
			fn drop(&mut self) {
				self.0.settings.object_recursion_limit += 1;
			}
		}
		impl<'a> ::std::ops::Deref for Dropper<'_, 'a> {
			type Target = $crate::gql::parser::Parser<'a>;

			fn deref(&self) -> &Self::Target {
				self.0
			}
		}

		impl<'a> ::std::ops::DerefMut for Dropper<'_, 'a> {
			fn deref_mut(&mut self) -> &mut Self::Target {
				self.0
			}
		}

		$this.settings.object_recursion_limit -= 1;
		let mut $name = Dropper($this);
		{
			$($t)*
		}
	}};
}

pub(crate) use enter_object_recursion;
pub(crate) use expected;
pub(crate) use unexpected;
