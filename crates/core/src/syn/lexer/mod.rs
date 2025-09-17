mod byte;
mod char;
pub mod compound;
mod ident;
pub mod keywords;
mod reader;
mod unicode;

#[cfg(test)]
mod test;

pub use reader::{BytesReader, CharError};

use crate::syn::error::{SyntaxError, bail};
use crate::syn::token::{Span, Token, TokenKind};

/// The SurrealQL lexer.
/// Takes a slice of bytes and turns it into tokens. The lexer is designed with
/// possible invalid utf-8 in mind and will handle bytes which are invalid utf-8
/// with an error.
///
/// The lexer generates tokens lazily. whenever [`Lexer::next_token`] is called
/// on the lexer it will try to lex the next bytes in the give source as a
/// token. The lexer always returns a token, even if the source contains invalid
/// tokens or as at the end of the source. In both cases a specific
/// type of token is returned.
///
/// Note that SurrealQL syntax cannot be lexed in advance. For example, record
/// strings and regexes, both cannot be parsed correctly without knowledge of
/// previous tokens as they are both ambigious with other tokens.
pub struct Lexer<'a> {
	/// The reader for reading the source bytes.
	pub(super) reader: BytesReader<'a>,
	/// The one past the last character of the previous token.
	last_offset: u32,
	/// A buffer used to build the value of tokens which can't be read straight
	/// from the source. like for example strings with escape characters.
	scratch: String,

	// below are a collection of storage for values produced by tokens.
	// For performance reasons we wan't to keep the tokens as small as possible.
	// As only some tokens have an additional value associated with them we don't store that value
	// in the token itself but, instead, in the lexer ensureing a smaller size for each individual
	// token.
	//
	// This does result in some additional state to keep track of as peeking a token while a token
	// value is still in the variables below will overwrite the previous value.
	//
	// Both numbers and actual strings are stored as a string value.
	// The parser can, depending on position in syntax, decide to parse a number in a variety of
	// different precisions or formats. The only way to support all is to delay parsing the
	// actual number value to when the parser can decide on a format.
	pub(super) string: Option<String>,
	pub(super) error: Option<SyntaxError>,
}

impl<'a> Lexer<'a> {
	/// Create a new lexer.
	/// # Panic
	/// This function will panic if the source is longer then u32::MAX.
	pub fn new(source: &'a [u8]) -> Lexer<'a> {
		let reader = BytesReader::new(source);
		assert!(reader.len() <= u32::MAX as usize, "source code exceeded maximum size");
		Lexer {
			reader,
			last_offset: 0,
			scratch: String::new(),
			string: None,
			error: None,
		}
	}

	/// Reset the state of the lexer.
	///
	/// Doesn't change the state of the reader.
	pub fn reset(&mut self) {
		self.last_offset = 0;
		self.scratch.clear();
		self.string = None;
		self.error = None;
	}

	/// Change the used source from the lexer to a new buffer.
	///
	/// Usefull for reusing buffers.
	///
	/// # Panic
	/// This function will panic if the source is longer then u32::MAX.
	pub fn change_source<'b>(self, source: &'b [u8]) -> Lexer<'b> {
		let reader = BytesReader::<'b>::new(source);
		assert!(reader.len() <= u32::MAX as usize, "source code exceeded maximum size");
		Lexer {
			reader,
			last_offset: 0,
			scratch: self.scratch,
			string: self.string,
			error: self.error,
		}
	}

	/// Returns the next token, driving the lexer forward.
	///
	/// If the lexer is at the end the source it will always return the Eof
	/// token.
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

	/// Creates the eof token.
	///
	/// An eof token has tokenkind Eof and an span which points to the last
	/// character of the source.
	fn eof_token(&mut self) -> Token {
		Token {
			kind: TokenKind::Eof,
			span: Span {
				offset: self.last_offset,
				len: 0,
			},
		}
	}

	/// Return an invalid token.
	fn invalid_token(&mut self, error: SyntaxError) -> Token {
		self.error = Some(error);
		self.finish_token(TokenKind::Invalid)
	}

	// Returns the span for the current token being lexed.
	pub(crate) fn current_span(&self) -> Span {
		// We make sure that the source is no longer then u32::MAX so this can't
		// overflow.
		let new_offset = self.reader.offset() as u32;
		let len = new_offset - self.last_offset;
		Span {
			offset: self.last_offset,
			len,
		}
	}

	pub(crate) fn span_since(&self, offset: usize) -> Span {
		let new_offset = self.reader.offset() as u32;
		let len = new_offset - offset as u32;
		Span {
			offset: offset as u32,
			len,
		}
	}

	fn advance_span(&mut self) -> Span {
		let span = self.current_span();
		self.last_offset = self.reader.offset() as u32;
		span
	}

	/// Builds a token from an TokenKind.
	///
	/// Attaches a span to the token and returns, updates the new offset.
	fn finish_token(&mut self, kind: TokenKind) -> Token {
		Token {
			kind,
			span: self.advance_span(),
		}
	}

	/// Moves the lexer state back to before the give span.
	///
	/// # Warning
	/// Moving the lexer into a state where the next byte is within a multibyte
	/// character will result in spurious errors.
	pub(crate) fn backup_before(&mut self, span: Span) {
		self.reader.backup(span.offset as usize);
		self.last_offset = span.offset;
	}

	/// Moves the lexer state to after the give span.
	///
	/// # Warning
	/// Moving the lexer into a state where the next byte is within a multibyte
	/// character will result in spurious errors.
	pub(crate) fn backup_after(&mut self, span: Span) {
		let offset = span.offset + span.len;
		self.reader.backup(offset as usize);
		self.last_offset = offset;
	}

	/// Checks if the next byte is the given byte, if it is it consumes the byte
	/// and returns true. Otherwise returns false.
	///
	/// Also returns false if there is no next character.
	fn eat(&mut self, byte: u8) -> bool {
		if self.reader.peek() == Some(byte) {
			self.reader.next();
			true
		} else {
			false
		}
	}

	/// Checks if the closure returns true when given the next byte, if it is it
	/// consumes the byte and returns true. Otherwise returns false.
	///
	/// Also returns false if there is no next character.
	fn eat_when<F: FnOnce(u8) -> bool>(&mut self, f: F) -> bool {
		let Some(x) = self.reader.peek() else {
			return false;
		};
		if f(x) {
			self.reader.next();
			true
		} else {
			false
		}
	}

	fn expect(&mut self, c: char) -> Result<(), SyntaxError> {
		match self.reader.peek() {
			Some(x) => {
				let offset = self.reader.offset() as u32;
				self.reader.next();
				let char = self.reader.convert_to_char(x)?;
				if char == c {
					return Ok(());
				}
				let len = self.reader.offset() as u32 - offset;
				bail!(
					"Unexpected character `{char}` expected `{c}`",
					@Span {
						offset,
						len
					}
				)
			}
			None => {
				bail!("Unexpected end of file, expected character `{c}`", @self.current_span())
			}
		}
	}

	/// Returns the string for a given span of the source.
	/// Will panic if the given span was not valid for the source, or invalid
	/// utf8
	pub fn span_str(&self, span: Span) -> &'a str {
		std::str::from_utf8(self.span_bytes(span)).expect("invalid span segment for source")
	}

	/// Returns the string for a given span of the source.
	/// Will panic if the given span was not valid for the source, or invalid
	/// utf8
	pub fn span_bytes(&self, span: Span) -> &'a [u8] {
		self.reader.span(span)
	}

	/// Returns an error if not all bytes were consumed.
	pub fn assert_finished(&self) -> Result<(), SyntaxError> {
		if !self.reader.is_empty() {
			let offset = self.reader.offset() as u32;
			let len = self.reader.remaining().len() as u32;
			let span = Span {
				offset,
				len,
			};
			bail!("Trailing characters", @span)
		}
		Ok(())
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
