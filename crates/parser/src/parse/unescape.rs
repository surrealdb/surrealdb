use token::Token;

use crate::parse::{ParseResult, Parser};

impl<'source, 'ast> Parser<'source, 'ast> {
	pub fn unescape_ident(&mut self, token: Token) -> ParseResult<&str> {
		assert!(token.token.is_identifier());
		let slice = self.slice(token.span);
		if slice.as_bytes()[0] != b'`' {
			// Already a valid identifier.
			return Ok(slice);
		}
		self.unescape_buffer.clear();
		self.todo()
	}

	pub fn unescape_str(&mut self, s: &str) -> ParseResult<&str> {
		self.todo()
	}
}
