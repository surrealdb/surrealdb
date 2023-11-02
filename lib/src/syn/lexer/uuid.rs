use crate::{
	sql::Uuid,
	syn::token::{DataIndex, Token, TokenKind},
};

use super::{Error as LexError, Lexer};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
	#[error("missing digits")]
	MissingDigits,
	#[error("digit was not in allowed range")]
	InvalidRange,
	#[error("expected uuid-strand to end")]
	ExpectedStrandEnd,
	#[error("missing a uuid seperator")]
	MissingSeperator,
}

impl<'a> Lexer<'a> {
	pub fn lex_uuid(&mut self, double: bool) -> Token {
		match self.lex_uuid_err(double) {
			Ok(x) => x,
			Err(e) => self.invalid_token(LexError::Uuid(Error::MissingDigits)),
		}
	}

	pub fn lex_uuid_err(&mut self, double: bool) -> Result<Token, Error> {
		if !self.lex_hex(8) {
			return Err(Error::MissingDigits);
		}

		if !self.eat(b'-') {
			return Err(Error::MissingSeperator);
		}

		if !self.lex_hex(4) {
			return Err(Error::MissingDigits);
		}

		if !self.eat(b'-') {
			return Err(Error::MissingSeperator);
		}

		if !self.eat_when(|x| (b'1'..=b'8').contains(&x)) {
			if self.peek().map(|x| x.is_ascii_digit()).unwrap_or(false) {
				// bute wasan ascii digit but not in the valid range.
				return Err(Error::InvalidRange);
			}
			return Err(Error::MissingDigits);
		};

		if !self.lex_hex(3) {
			return Err(Error::MissingDigits);
		}

		if !self.eat(b'-') {
			return Err(Error::MissingSeperator);
		}

		if !self.lex_hex(4) {
			return Err(Error::MissingDigits);
		}

		if !self.eat(b'-') {
			return Err(Error::MissingSeperator);
		}

		if !self.lex_hex(12) {
			return Err(Error::MissingDigits);
		}

		let end_char = if double {
			b'"'
		} else {
			b'\''
		};
		// closing strand character
		if !self.eat(end_char) {
			return Err(Error::ExpectedStrandEnd);
		}

		let mut span = self.current_span();
		// subtract the first `u` and both `"`.
		span.len -= 3;
		// move over the first `u"`
		span.offset += 2;
		// The lexer ensures that the section of bytes is valid utf8 so this should never panic.
		let uuid_str = std::str::from_utf8(self.reader.span(span)).unwrap();
		// The lexer ensures that the bytes are a valid uuid so this should never panic.
		let uuid = uuid::Uuid::try_from(uuid_str).unwrap();

		let id = self.datetime.len() as u32;
		let id = DataIndex::from(id);
		self.uuid.push(Uuid(uuid));
		Ok(self.finish_token(TokenKind::Uuid, Some(id)))
	}

	/// lexes a given amount of hex characters. returns true if the lexing was successfull, false
	/// otherwise.
	pub fn lex_hex(&mut self, mut amount: u8) -> bool {
		for _ in 0..amount {
			if !self.eat_when(|x| matches!(x,b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F')) {
				return false;
			}
		}
		true
	}
}
