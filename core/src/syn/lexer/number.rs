use crate::syn::{
	lexer::{unicode::U8Ext, Error as LexError, Lexer},
	token::{NumberKind, Token, TokenKind},
};
use std::mem;
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
	#[error("invalid number suffix")]
	InvalidSuffix,
	#[error("expected atleast a single digit in the exponent")]
	DigitExpectedExponent,
}

impl Lexer<'_> {
	/// Lex only an integer.
	/// Use when a number can be followed immediatly by a `.` like in a model version.
	pub fn lex_only_integer(&mut self) -> Token {
		match self.lex_only_integer_err() {
			Ok(x) => x,
			Err(e) => self.invalid_token(LexError::Number(e)),
		}
	}

	fn lex_only_integer_err(&mut self) -> Result<Token, Error> {
		let Some(next) = self.reader.peek() else {
			return Ok(self.eof_token());
		};

		// not a number, return a different token kind, for error reporting.
		if !next.is_ascii_digit() {
			return Ok(self.next_token());
		}

		self.scratch.push(next as char);
		self.reader.next();

		// eat all the ascii digits
		while let Some(x) = self.reader.peek() {
			if x == b'_' {
				self.reader.next();
			} else if !x.is_ascii_digit() {
				break;
			} else {
				self.scratch.push(x as char);
				self.reader.next();
			}
		}

		// test for a suffix.
		match self.reader.peek() {
			Some(b'd' | b'f') => {
				// not an integer but parse anyway for error reporting.
				return self.lex_suffix(false, true);
			}
			Some(x) if x.is_ascii_alphabetic() => return Err(self.invalid_suffix()),
			_ => {}
		}

		self.string = Some(mem::take(&mut self.scratch));
		Ok(self.finish_token(TokenKind::Number(NumberKind::Integer)))
	}

	pub fn lex_number(&mut self, start: u8) -> Token {
		match self.lex_number_err(start) {
			Ok(x) => x,
			Err(e) => self.invalid_token(LexError::Number(e)),
		}
	}
	/// Lex a number.
	///
	/// Expects the digit which started the number as the start argument.
	pub fn lex_number_err(&mut self, start: u8) -> Result<Token, Error> {
		debug_assert!(start.is_ascii_digit());
		debug_assert_eq!(self.scratch, "");
		self.scratch.push(start as char);
		loop {
			let Some(x) = self.reader.peek() else {
				self.string = Some(mem::take(&mut self.scratch));
				return Ok(self.finish_token(TokenKind::Number(NumberKind::Integer)));
			};
			match x {
				b'0'..=b'9' => {
					// next digits.
					self.reader.next();
					self.scratch.push(x as char);
				}
				b'e' | b'E' => {
					// scientific notation
					self.reader.next();
					self.scratch.push('e');
					return self.lex_exponent(false);
				}
				b'.' => {
					// mantissa
					let backup = self.reader.offset();
					self.reader.next();
					let next = self.reader.peek();
					if let Some(b'0'..=b'9') = next {
						self.scratch.push('.');
						return self.lex_mantissa();
					} else {
						// indexing a number
						self.reader.backup(backup);
						self.string = Some(mem::take(&mut self.scratch));
						return Ok(self.finish_token(TokenKind::Number(NumberKind::Integer)));
					}
				}
				b'f' | b'd' => return self.lex_suffix(false, true),
				// Oxc2 is the start byte of 'µ'
				0xc2 | b'n' | b'u' | b'm' | b'h' | b'w' | b'y' | b's' => {
					// duration suffix, switch to lexing duration.
					return Ok(self.lex_duration());
				}
				b'_' => {
					self.reader.next();
				}
				b'a'..=b'z' | b'A'..=b'Z' => {
					return Err(self.invalid_suffix());
					// invalid token, unexpected identifier character immediatly after number.
					// Eat all remaining identifier like characters.
				}
				_ => {
					self.string = Some(mem::take(&mut self.scratch));
					return Ok(self.finish_token(TokenKind::Number(NumberKind::Integer)));
				}
			}
		}
	}

	fn invalid_suffix(&mut self) -> Error {
		// eat the whole suffix.
		while let Some(x) = self.reader.peek() {
			if !x.is_ascii_alphanumeric() {
				break;
			}
			self.reader.next();
		}
		self.scratch.clear();
		Error::InvalidSuffix
	}

	/// Lex a number suffix, either 'f' or 'dec'.
	fn lex_suffix(&mut self, had_exponent: bool, can_be_duration: bool) -> Result<Token, Error> {
		match self.reader.peek() {
			Some(b'f') => {
				// float suffix
				self.reader.next();
				if let Some(true) = self.reader.peek().map(|x| x.is_identifier_continue()) {
					Err(self.invalid_suffix())
				} else {
					self.string = Some(mem::take(&mut self.scratch));
					Ok(self.finish_token(TokenKind::Number(NumberKind::Float)))
				}
			}
			Some(b'd') => {
				// decimal suffix
				self.reader.next();
				let checkpoint = self.reader.offset();
				if !self.eat(b'e') {
					if can_be_duration {
						self.reader.backup(checkpoint - 1);
						return Ok(self.lex_duration());
					} else {
						return Err(self.invalid_suffix());
					}
				}

				if !self.eat(b'c') {
					return Err(self.invalid_suffix());
				}

				if let Some(true) = self.reader.peek().map(|x| x.is_identifier_continue()) {
					Err(self.invalid_suffix())
				} else {
					self.string = Some(mem::take(&mut self.scratch));
					if had_exponent {
						Ok(self.finish_token(TokenKind::Number(NumberKind::DecimalExponent)))
					} else {
						Ok(self.finish_token(TokenKind::Number(NumberKind::Decimal)))
					}
				}
			}
			_ => unreachable!(),
		}
	}

	/// Lexes the mantissa of a number, i.e. `.8` in `1.8`
	pub fn lex_mantissa(&mut self) -> Result<Token, Error> {
		loop {
			// lex_number already checks if there exists a digit after the dot.
			// So this will never fail the first iteration of the loop.
			let Some(x) = self.reader.peek() else {
				self.string = Some(mem::take(&mut self.scratch));
				return Ok(self.finish_token(TokenKind::Number(NumberKind::Mantissa)));
			};
			match x {
				b'0'..=b'9' => {
					// next digit.
					self.reader.next();
					self.scratch.push(x as char);
				}
				b'e' | b'E' => {
					// scientific notation
					self.reader.next();
					self.scratch.push('e');
					return self.lex_exponent(true);
				}
				b'_' => {
					self.reader.next();
				}
				b'f' | b'd' => return self.lex_suffix(false, false),
				b'a'..=b'z' | b'A'..=b'Z' => {
					// invalid token, random identifier characters immediately after number.
					self.scratch.clear();
					return Err(Error::InvalidSuffix);
				}
				_ => {
					self.string = Some(mem::take(&mut self.scratch));
					return Ok(self.finish_token(TokenKind::Number(NumberKind::Mantissa)));
				}
			}
		}
	}

	/// Lexes the exponent of a number, i.e. `e10` in `1.1e10`;
	fn lex_exponent(&mut self, had_mantissa: bool) -> Result<Token, Error> {
		loop {
			match self.reader.peek() {
				Some(x @ b'-' | x @ b'+') => {
					self.reader.next();
					self.scratch.push(x as char);
				}
				Some(x @ b'0'..=b'9') => {
					self.scratch.push(x as char);
					break;
				}
				_ => {
					// random other character, expected atleast one digit.
					return Err(Error::DigitExpectedExponent);
				}
			}
		}
		self.reader.next();
		loop {
			match self.reader.peek() {
				Some(x @ b'0'..=b'9') => {
					self.reader.next();
					self.scratch.push(x as char);
				}
				Some(b'_') => {
					self.reader.next();
				}
				Some(b'f' | b'd') => return self.lex_suffix(true, false),
				_ => {
					let kind = if had_mantissa {
						NumberKind::MantissaExponent
					} else {
						NumberKind::Exponent
					};
					self.string = Some(mem::take(&mut self.scratch));
					return Ok(self.finish_token(TokenKind::Number(kind)));
				}
			}
		}
	}
}
