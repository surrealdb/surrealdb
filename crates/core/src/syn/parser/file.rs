use crate::{
	sql::{File, Ident},
	syn::{error::bail, parser::mac::expected_whitespace},
};

use super::{ParseResult, Parser};
use crate::syn::token::t;

// TODO(kearfy): This is so cursed but just want to get the rest working first

impl Parser<'_> {
	/// Expects `file:/` to be parsed already
	pub(crate) async fn parse_file(&mut self) -> ParseResult<File> {
		let bucket = Ident(self.parse_file_path_segment()?);
		expected_whitespace!(self, t!("/"));
		let mut key = String::new();
		loop {
			key.push('/');
			key += &self.parse_file_path_segment()?;
			if !self.eat(t!("/")) {
				break;
			}
		}

		Ok(File {
			bucket,
			key,
		})
	}

	/// Expects to find a file path segment after `/` has been parsed
	pub(crate) fn parse_file_path_segment(&mut self) -> ParseResult<String> {
		if self.lexer.reader.peek().is_none() {
			bail!("Unexpected end of file, expected to find a filepath segment", @self.lexer.current_span())
		}

		let mut segment = String::new();
		while let Some(x) = self.lexer.reader.peek() {
			if x.is_ascii_alphanumeric() || matches!(x, b'-' | b'_' | b'.') {
				let char = self.lexer.reader.next().unwrap();
				let char = self.lexer.reader.convert_to_char(char)?;
				segment.push(char)
			} else if segment.is_empty() {
				let char = self.lexer.reader.convert_to_char(x)?;
				bail!("Unexpected character `{char}`, expected to find A-Z, a-z, 0-9, `-`, `_` or `.` in filepath segment", @self.lexer.current_span())
			} else {
				break;
			}
		}

		self.lexer.advance_span();

		Ok(segment)
	}
}
