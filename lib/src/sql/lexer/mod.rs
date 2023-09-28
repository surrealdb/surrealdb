use crate::sql::token::{DataIndex, Span, Token, TokenKind};

mod byte;
mod char;
mod ident;
mod keywords;
mod number;
mod reader;
mod unicode;

pub use reader::{BytesReader, CharError};

pub struct Lexer<'a> {
	pub reader: BytesReader<'a>,
	last_offset: u32,
	scratch: String,
	pub strings: Vec<String>,
}

impl<'a> Lexer<'a> {
	/// Create a new lexer.
	///
	/// # Panic
	///
	/// Will panic if the size of the provided slice exceeds `u32::MAX`.
	pub fn new(source: &'a str) -> Lexer<'a> {
		let reader = BytesReader::new(source.as_bytes());
		assert!(reader.len() <= u32::MAX as usize, "source code exceeded maximum size");
		Lexer {
			reader,
			last_offset: 0,
			scratch: String::new(),
			strings: Vec::new(),
		}
	}

	pub fn finish_token(&mut self, kind: TokenKind, data_index: Option<DataIndex>) -> Token {
		// We make sure that the source is no longer then u32::MAX so this can't wrap.
		let new_offset = self.reader.offset() as u32;
		let len = new_offset - self.last_offset;
		let span = Span {
			offset: self.last_offset,
			len,
		};
		self.last_offset = new_offset;
		Token {
			kind,
			span,
			data_index,
		}
	}

	pub fn finish_string_token(&mut self, kind: TokenKind) -> Token {
		let id = self.strings.len() as u32;
		let string = self.scratch.clone();
		self.scratch.clear();
		self.strings.push(string);
		self.finish_token(kind, Some(id.into()))
	}

	pub fn next_token(&mut self) -> Token {
		let Some(byte) = self.reader.next() else {
			return self.eof_token();
		};
		if byte.is_ascii() {
			self.lex_ascii(byte)
		} else {
			self.lex_char(byte)
		}
	}

	pub fn eof_token(&mut self) -> Token {
		Token {
			kind: TokenKind::Eof,
			span: Span {
				offset: self.last_offset.saturating_sub(1),
				len: 1,
			},
			data_index: None,
		}
	}

	pub fn skip_offset(&mut self) {
		self.last_offset = self.reader.offset() as u32;
	}
}

impl Iterator for Lexer<'_> {
	type Item = Token;

	fn next(&mut self) -> Option<Self::Item> {
		let token = self.next_token();
		if token.is_eof() {
			return None;
		}
		Some(token)
	}
}
