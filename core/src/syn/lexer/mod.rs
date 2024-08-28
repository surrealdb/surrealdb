use std::time::Duration;

use chrono::{DateTime, Utc};
use thiserror::Error;

mod byte;
mod char;
mod ident;
mod js;
pub mod keywords;
mod number;
mod reader;
mod strand;
mod unicode;

#[cfg(test)]
mod test;

pub use reader::{BytesReader, CharError};
use uuid::Uuid;

use crate::syn::token::{Span, Token, TokenKind};

/// A error returned by the lexer when an invalid token is encountered.
///
/// Can be retrieved from the `Lexer::error` field whenever it returned a [`TokenKind::Invalid`]
/// token.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
	#[error("Lexer encountered unexpected character {0:?}")]
	UnexpectedCharacter(char),
	#[error("invalid escape character {0:?}")]
	InvalidEscapeCharacter(char),
	#[error("Lexer encountered unexpected end of source characters")]
	UnexpectedEof,
	#[error("source was not valid utf-8")]
	InvalidUtf8,
	#[error("expected next character to be '{0}'")]
	ExpectedEnd(char),
}

impl From<CharError> for Error {
	fn from(value: CharError) -> Self {
		match value {
			CharError::Eof => Self::UnexpectedEof,
			CharError::Unicode => Self::InvalidUtf8,
		}
	}
}

/// The SurrealQL lexer.
/// Takes a slice of bytes and turns it into tokens. The lexer is designed with possible invalid utf-8
/// in mind and will handle bytes which are invalid utf-8 with an error.
///
/// The lexer generates tokens lazily. whenever [`Lexer::next_token`] is called on the lexer it will
/// try to lex the next bytes in the give source as a token. The lexer always returns a token, even
/// if the source contains invalid tokens or as at the end of the source. In both cases a specific
/// type of token is returned.
///
/// Note that SurrealQL syntax cannot be lexed in advance. For example, record strings and regexes,
/// both cannot be parsed correctly without knowledge of previous tokens as they are both ambigious
/// with other tokens.
#[non_exhaustive]
pub struct Lexer<'a> {
	/// The reader for reading the source bytes.
	pub reader: BytesReader<'a>,
	/// The one past the last character of the previous token.
	last_offset: u32,
	/// A buffer used to build the value of tokens which can't be read straight from the source.
	/// like for example strings with escape characters.
	scratch: String,

	// below are a collection of storage for values produced by tokens.
	// For performance reasons we want to keep the tokens as small as possible.
	// As only some tokens have an additional value associated with them, we don't store that value
	// in the token itself but, instead, in the lexer ensuring a smaller size for each individual
	// token.
	//
	// This does result in some additional state to keep track of as peeking a token while a token
	// value is still in the variables below will overwrite the previous value.
	//
	// Both numbers and actual strings are stored as a string value.
	// The parser can, depending on position in syntax, decide to parse a number in a variety of
	// different precisions or formats. The only way to support all is to delay parsing the
	// actual number value to when the parser can decide on a format.
	pub string: Option<String>,
	pub duration: Option<Duration>,
	pub datetime: Option<DateTime<Utc>>,
	pub error: Option<Error>,
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
			duration: None,
			datetime: None,
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
			duration: self.duration,
			datetime: self.datetime,
		}
	}

	/// Returns the next token, driving the lexer forward.
	///
	/// If the lexer is at the end the source it will always return the Eof token.
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
	/// An eof token has tokenkind Eof and an span which points to the last character of the
	/// source.
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
	fn invalid_token(&mut self, error: Error) -> Token {
		self.error = Some(error);
		self.finish_token(TokenKind::Invalid)
	}

	// Returns the span for the current token being lexed.
	pub fn current_span(&self) -> Span {
		// We make sure that the source is no longer then u32::MAX so this can't overflow.
		let new_offset = self.reader.offset() as u32;
		let len = new_offset - self.last_offset;
		Span {
			offset: self.last_offset,
			len,
		}
	}

	/// Builds a token from an TokenKind.
	///
	/// Attaches a span to the token and returns, updates the new offset.
	fn finish_token(&mut self, kind: TokenKind) -> Token {
		let span = self.current_span();
		// We make sure that the source is no longer then u32::MAX so this can't overflow.
		self.last_offset = self.reader.offset() as u32;
		Token {
			kind,
			span,
		}
	}

	/// Moves the lexer state back to before the give span.
	///
	/// # Warning
	/// Moving the lexer into a state where the next byte is within a multibyte character will
	/// result in spurious errors.
	pub fn backup_before(&mut self, span: Span) {
		self.reader.backup(span.offset as usize);
		self.last_offset = span.offset;
	}

	/// Moves the lexer state to after the give span.
	///
	/// # Warning
	/// Moving the lexer into a state where the next byte is within a multibyte character will
	/// result in spurious errors.
	pub fn backup_after(&mut self, span: Span) {
		let offset = span.offset + span.len;
		self.reader.backup(offset as usize);
		self.last_offset = offset;
	}

	/// Checks if the next byte is the given byte, if it is it consumes the byte and returns true.
	/// Otherwise returns false.
	///
	/// Also returns false if there is no next character.
	pub fn eat(&mut self, byte: u8) -> bool {
		if self.reader.peek() == Some(byte) {
			self.reader.next();
			true
		} else {
			false
		}
	}

	/// Checks if the closure returns true when given the next byte, if it is it consumes the byte
	/// and returns true. Otherwise returns false.
	///
	/// Also returns false if there is no next character.
	pub fn eat_when<F: FnOnce(u8) -> bool>(&mut self, f: F) -> bool {
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
