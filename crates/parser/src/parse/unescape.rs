use crate::parse::{ParseResult, Parser};

impl<'source, 'ast> Parser<'source, 'ast> {
	pub fn unescape_str(&mut self, s: &str) -> ParseResult<&str> {
		todo!()
	}
}
