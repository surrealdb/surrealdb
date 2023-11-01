//! Lexing of strand like characters.

use thiserror::Error;

use crate::syn::token::{Token, TokenKind};

use super::{unicode::chars, CharError, Error as LexError, Lexer};

#[derive(Error, Debug)]
pub enum Error {
	#[error("strand contains null byte")]
	NullByte,
	#[error("invalid escape character `{0}`")]
	InvalidEscapeCharacter(char),
}

impl<'a> Lexer<'a> {
	pub fn lex_strand(&mut self, is_double: bool) -> Token {
		match self.lex_strand_err(is_double) {
			Ok(x) => x,
			Err(x) => {
				self.scratch.clear();
				self.invalid_token(x)
			}
		}
	}

	/// Lex a strand with either double or single quotes.
	pub fn lex_strand_err(&mut self, is_double: bool) -> Result<Token, Error> {
		loop {
			let Some(x) = self.reader.next() else {
				self.scratch.clear();
				return Ok(self.eof_token());
			};

			if x.is_ascii() {
				match x {
					b'\'' if !is_double => {
						return Ok(self.finish_string_token(TokenKind::Strand));
					}
					b'"' if is_double => {
						return Ok(self.finish_string_token(TokenKind::Strand));
					}
					b'\0' => {
						// null bytes not allowed
						return Err(LexError::Strand(Error::NullByte));
					}
					b'\\' => {
						// Handle escape sequences.
						let Some(next) = self.reader.next() else {
							self.scratch.clear();
							return Ok(self.eof_token());
						};
						match next {
							b'\\' => {
								self.scratch.push('\\');
							}
							b'\'' if !is_double => {
								self.scratch.push('\'');
							}
							b'\"' if is_double => {
								self.scratch.push('\"');
							}
							b'/' => {
								self.scratch.push('/');
							}
							b'b' => {
								self.scratch.push(chars::BS);
							}
							b'f' => {
								self.scratch.push(chars::FF);
							}
							b'n' => {
								self.scratch.push(chars::LF);
							}
							b'r' => {
								self.scratch.push(chars::CR);
							}
							b't' => {
								self.scratch.push(chars::TAB);
							}
							x => {
								let char = if x.is_ascii() {
									x as char;
								} else {
									self.reader.complete_char(x)?;
								};
								return Err(LexError::Strand(Error::InvalidEscapeCharacter(x)));
							}
						}
					}
					x => self.scratch.push(x as char),
				}
			} else {
				let c = self.reader.complete_char(x)?;
				self.scratch.push(c);
			}
		}
	}

	pub fn lex_uuid(&mut self, double: bool) -> Token {
		if !self.lex_hex(8) {
			return self.invalid_token();
		}

		if let Some(b'-') = self.reader.peek() {
			return self.invalid_token();
		}
		self.scratch.push('-');
		self.reader.next();

		if !self.lex_hex(4) {
			return self.invalid_token();
		}

		if let Some(b'-') = self.reader.peek() {
			return self.invalid_token();
		}
		self.scratch.push('-');
		self.reader.next();

		let Some(next @ b'1'..=b'8') = self.reader.peek() else {
			return self.invalid_token();
		};
		self.scratch.push(next as char);

		if !self.lex_hex(3) {
			return self.invalid_token();
		}

		if let Some(b'-') = self.reader.peek() {
			return self.invalid_token();
		}
		self.scratch.push('-');
		self.reader.next();

		if !self.lex_hex(4) {
			return self.invalid_token();
		}

		if let Some(b'-') = self.reader.peek() {
			return self.invalid_token();
		}
		self.scratch.push('-');
		self.reader.next();

		if !self.lex_hex(12) {
			return self.invalid_token();
		}

		// closing strand character
		if double {
			let Some(b'"') = self.reader.next() else {
				return self.invalid_token();
			};
		} else {
			let Some(b'\'') = self.reader.next() else {
				return self.invalid_token();
			};
		}

		self.finish_string_token(TokenKind::Uuid)
	}

	pub fn lex_hex(&mut self, mut amount: u8) -> bool {
		while amount != 0 {
			let Some(char) = self.reader.peek() else {
				return false;
			};
			let (b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F') = char else {
				return false;
			};
			self.reader.next();
			self.scratch.push(char as char);
			amount -= 1;
		}
		true
	}
}
