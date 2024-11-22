/// A macro for returning an error when a unexpected token was found.
///
/// This macro handles a variety of situations, including errors related to invalid tokens and
/// unexpected `EOF` or whitespace.
///
/// This macro takes a reference to the parser, the token which was unexpected and a expression
/// which explains what should be expected instead.
///
/// This macro attaches the span from the token as an error span to the error.
macro_rules! unexpected {
	($parser:expr, $found:expr, $expected:expr $(, @$span:expr)? $(, $($t:tt)* )?) => {{
		let __found: $crate::syn::token::Token = $found;
		match __found.kind{
			$crate::syn::token::TokenKind::Invalid => {
				return Err($parser.lexer.error.take().unwrap());
			}
			$crate::syn::token::TokenKind::Eof => {
				let error = $crate::syn::error::syntax_error!("Unexpected end of file, expected {}",$expected, @__found.span $( $($t)* )?);
				return Err(error.with_data_pending())
			}
			$crate::syn::token::TokenKind::WhiteSpace => {
				$crate::syn::error::bail!("Unexpected whitespace, expected token {} to continue",$expected,  @__found.span$( $($t)* )?)
			}
			x => {
				$crate::syn::error::bail!("Unexpected token `{}`, expected {}",x,$expected, @__found.span$( $($t)* )?)
			}
		}
	}};

}

/// A macro for asserting that the next token should be of the given type, returns the token if
/// this is the case otherwise it returns an error.
macro_rules! expected {
	($parser:expr, $($kind:tt)*) => {{
		let token: crate::syn::token::Token = $parser.next();
		if let $($kind)* = token.kind{
			token
		}else{
			$crate::syn::parser::unexpected!($parser,token, $($kind)*)
		}
	}};
}

/// Pops the last token, checks if it is the desired glue value and then returns the value.
/// This will panic if the token was not correct or the value was already eat, both of which the
/// parser should make sure to uphold.
macro_rules! pop_glued {
	($parser:expr, $variant:ident) => {{
		let token = $parser.pop_peek();
		debug_assert!(matches!(
			token.kind,
			$crate::syn::token::TokenKind::Glued($crate::syn::token::Glued::$variant)
		));
		let $crate::syn::parser::GluedValue::$variant(x) =
			::std::mem::take(&mut $parser.glued_value)
		else {
			panic!("Glued value was already taken, while the glue token still in the token buffer.")
		};
		x
	}};
}

/// A macro for indicating that the parser encountered an token which it didn't expect.
macro_rules! expected_whitespace {
	($parser:expr, $($kind:tt)*) => {{
		let token: crate::syn::token::Token = $parser.next_whitespace();
		if let $($kind)* = token.kind{
			token
		}else{
			$crate::syn::parser::unexpected!($parser,token, $($kind)*)
		}
	}};
}

#[cfg(test)]
macro_rules! test_parse {
	($func:ident$( ( $($e:expr),* $(,)? ))? , $t:expr) => {{
		let mut parser = $crate::syn::parser::Parser::new($t.as_bytes());
		let mut stack = reblessive::Stack::new();
		stack.enter(|ctx| parser.$func(ctx,$($($e),*)*)).finish()
	}};
}

macro_rules! enter_object_recursion {
	($name:ident = $this:expr => { $($t:tt)* }) => {{
		if $this.object_recursion == 0 {
			return Err($crate::syn::parser::SyntaxError::new("Exceeded query recursion depth limit")
				.with_span($this.last_span(), $crate::syn::error::MessageKind::Error))
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

macro_rules! enter_query_recursion {
	($name:ident = $this:expr => { $($t:tt)* }) => {{
		if $this.query_recursion == 0 {
			return Err($crate::syn::parser::SyntaxError::new("Exceeded query recursion depth limit")
				.with_span($this.last_span(), $crate::syn::error::MessageKind::Error))
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

pub(crate) use enter_object_recursion;
pub(crate) use enter_query_recursion;
pub(crate) use expected;
pub(crate) use expected_whitespace;
pub(crate) use pop_glued;
pub(crate) use unexpected;

#[cfg(test)]
pub(crate) use test_parse;
