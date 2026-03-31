/// A macro for returning an error when a unexpected token was found.
///
/// This macro handles a variety of situations, including errors related to
/// invalid tokens and unexpected `EOF` or whitespace.
///
/// This macro takes a reference to the parser, the token which was unexpected
/// and a expression which explains what should be expected instead.
///
/// This macro attaches the span from the token as an error span to the error.
macro_rules! unexpected {
	($parser:expr_2021, $found:expr_2021, $expected:expr_2021 $(, @$span:expr_2021)? $(, $($t:tt)* )?) => {{
		let __found: $crate::syn::token::Token = $found;
		match __found.kind{
			$crate::syn::token::TokenKind::Invalid => {
				return Err($parser.lexer.error.take().unwrap());
			}
			$crate::syn::token::TokenKind::Eof => {
				let error = $crate::syn::error::syntax_error!("Unexpected end of file, expected {}",$expected, @__found.span $( $($t)* )?);
				return Err(error)
			}
			x => {
				$crate::syn::error::bail!("Unexpected token `{}`, expected {}",x,$expected, @__found.span$( $($t)* )?)
			}
		}
	}};

}

/// A macro for asserting that the next token should be of the given type,
/// returns the token if this is the case otherwise it returns an error.
macro_rules! expected {
	($parser:expr_2021, $($kind:tt)*) => {{
		let token: crate::syn::token::Token = $parser.next();
		if let $($kind)* = token.kind{
			token
		}else{
			$crate::syn::parser::unexpected!($parser,token, $($kind)*)
		}
	}};
}

/// A macro for indicating that the parser encountered an token which it didn't
/// expect.
macro_rules! expected_whitespace {
	($parser:expr_2021, $($kind:tt)*) => {{
		if let Some(token) = $parser.next_whitespace() {
			if let $($kind)* = token.kind{
				token
			}else{
				$crate::syn::parser::unexpected!($parser,token, $($kind)*)
			}
		}else{
			$crate::syn::error::bail!("Unexpected whitespace",@$parser.last_span() => "No whitespace allowed after this token")
		}
	}};
}

pub(crate) use {expected, expected_whitespace, unexpected};
