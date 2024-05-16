use crate::{
	sql::Uuid,
	syn::token::{Token, TokenKind},
};

use super::{Error as LexError, Lexer};
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
	#[error("missing digits")]
	MissingDigits,
	#[error("digit was not in allowed range")]
	InvalidRange,
	#[error("expected uuid-strand to end")]
	ExpectedStrandEnd,
	#[error("missing a uuid separator")]
	MissingSeperator,
}

impl<'a> Lexer<'a> {
	/// Lex a uuid strand with either double or single quotes.
	///
	/// Expects the first delimiter to already have been eaten.
	pub fn lex_uuid(&mut self, double: bool) -> Token {
		match self.lex_uuid_err(double) {
			Ok(x) => {
				debug_assert!(self.uuid.is_none());
				self.uuid = Some(x);
				self.finish_token(TokenKind::Uuid)
			}
			Err(_) => self.invalid_token(LexError::Uuid(Error::MissingDigits)),
		}
	}

	/// Lex a uuid strand with either double or single quotes but return an result instead of a
	/// token.
	///
	/// Expects the first delimiter to already have been eaten.
	pub fn lex_uuid_err(&mut self, double: bool) -> Result<Uuid, Error> {
		let uuid = self.lex_uuid_err_inner()?;

		let end_char = if double {
			b'"'
		} else {
			b'\''
		};
		// closing strand character
		if !self.eat(end_char) {
			return Err(Error::ExpectedStrandEnd);
		}

		Ok(uuid)
	}

	/// Lex a uuid strand without delimiting quotes but return an result instead of a
	/// token.
	///
	/// Expects the first delimiter to already have been eaten.
	pub fn lex_uuid_err_inner(&mut self) -> Result<Uuid, Error> {
		let start = self.reader.offset();

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
			if self.reader.peek().map(|x| x.is_ascii_digit()).unwrap_or(false) {
				// byte was an ascii digit but not in the valid range.
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

		let end = self.reader.offset();
		// The lexer ensures that the section of bytes is valid utf8 so this should never panic.
		let uuid_str = std::str::from_utf8(&self.reader.full()[start..end]).unwrap();
		// The lexer ensures that the bytes are a valid uuid so this should never panic.
		Ok(Uuid(uuid::Uuid::try_from(uuid_str).unwrap()))
	}

	/// lexes a given amount of hex characters. returns true if the lexing was successfull, false
	/// otherwise.
	pub fn lex_hex(&mut self, amount: u8) -> bool {
		for _ in 0..amount {
			if !self.eat_when(|x| x.is_ascii_hexdigit()) {
				return false;
			}
		}
		true
	}
}
