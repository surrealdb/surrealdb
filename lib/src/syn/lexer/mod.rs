use crate::{
	sql::Number,
	syn::token::{DataIndex, Span, Token, TokenKind},
};

mod byte;
mod char;
mod duration;
mod ident;
mod keywords;
mod number;
mod reader;
mod unicode;

#[cfg(test)]
mod test;

pub use reader::{BytesReader, CharError};
use std::time::Duration;

pub struct Lexer<'a> {
	pub reader: BytesReader<'a>,
	last_offset: u32,
	scratch: String,
	pub numbers: Vec<Number>,
	/// Strings build from the source.
	pub strings: Vec<String>,
	pub durations: Vec<Duration>,
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
			numbers: Vec::new(),
			strings: Vec::new(),
			durations: Vec::new(),
		}
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

	/// Skip the last consumed bytes in the reader.
	///
	/// The bytes consumed before this point won't be part of the span.
	fn skip_offset(&mut self) {
		self.last_offset = self.reader.offset() as u32;
	}

	/// Return an invalid token.
	fn invalid_token(&mut self) -> Token {
		self.finish_token(TokenKind::Invalid, None)
	}

	/// Builds a token from an TokenKind.
	///
	/// Attaches a span to the token and returns, updates the new offset.
	fn finish_token(&mut self, kind: TokenKind, data_index: Option<DataIndex>) -> Token {
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

	/// Finish a token which contains a string like value.
	///
	/// Copies out all of the values in scratch and pushes into the data array.
	/// Attaching it to the token.
	fn finish_string_token(&mut self, kind: TokenKind) -> Token {
		let id = self.strings.len() as u32;
		let string = self.scratch.clone();
		self.scratch.clear();
		self.strings.push(string);
		self.finish_token(kind, Some(id.into()))
	}

	fn finish_number_token(&mut self, number: Number) -> Token {
		let id = self.strings.len() as u32;
		self.numbers.push(number);
		self.finish_token(TokenKind::Number, Some(id.into()))
	}

	pub fn backup_before(&mut self, span: Span) {
		self.reader.backup(span.offset as usize);
	}

	pub fn backup_after(&mut self, span: Span) {
		self.reader.backup(span.offset as usize + span.len as usize);
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
