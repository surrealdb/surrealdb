use crate::sql::Number;
use crate::syn::lexer::{unicode::U8Ext, Error as LexError, Lexer};
use crate::syn::token::{Token, TokenKind};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
	#[error("invalid number suffix")]
	InvalidSuffix,
	#[error("expected atleast a single digit in the exponent")]
	DigitExpectedExponent,
	#[error("number overflows max allowed value")]
	Overflow,
}

impl Lexer<'_> {
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
		self.scratch.push(start as char);
		loop {
			let Some(x) = self.reader.peek() else {
				return Ok(self.finish_token(TokenKind::Number, None));
			};
			match x {
				b'0'..=b'9' => {
					// next digits.
					self.reader.next();
					self.scratch.push(x as char);
				}
				b'.' => {
					// mantissa
					let backup = self.reader.offset();
					self.reader.next();
					let next = self.reader.peek();
					if let Some(x @ b'0'..=b'9') = next {
						self.scratch.push(x as char);
						return self.lex_mantissa();
					} else {
						// indexing a number
						self.reader.backup(backup);
						return Ok(self.finish_int_token());
					}
				}
				b'f' | b'd' => return self.lex_suffix(true),
				// Oxc2 is the start byte of 'µ'
				0xc2 | b'n' | b'u' | b'm' | b'h' | b'w' | b'y' | b's' => {
					// duration suffix, switch to lexing duration.
					return Ok(self.lex_duration());
				}
				b'_' => {
					self.reader.next();
				}
				b'a'..=b'z' | b'A'..=b'Z' => {
					return Err(Error::InvalidSuffix);
					// invalid token, unexpected identifier character immediatly after number.
					// Eat all remaining identifier like characters.
				}
				_ => {
					return Ok(self.finish_int_token());
				}
			}
		}
	}

	fn lex_suffix(&mut self, can_be_duration: bool) -> Result<Token, Error> {
		match self.reader.peek() {
			Some(b'f') => {
				// float suffix
				self.reader.next();
				if let Some(true) = self.reader.peek().map(|x| x.is_identifier_continue()) {
					Err(Error::InvalidSuffix)
				} else {
					Ok(self.finish_float_token())
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
						return Err(Error::InvalidSuffix);
					}
				}

				if !self.eat(b'c') {
					return Err(Error::InvalidSuffix);
				}

				if let Some(true) = self.reader.peek().map(|x| x.is_identifier_continue()) {
					Err(Error::InvalidSuffix)
				} else {
					Ok(self.finish_int_token())
				}
			}
			_ => unreachable!(),
		}
	}

	/// Lexes the mantissa of a number, i.e. `.8` in `1.8`
	pub fn lex_mantissa(&mut self) -> Result<Token, Error> {
		self.scratch.push('.');
		loop {
			// lex_number already checks if there exists a digit after the dot.
			// So this will never fail the first iteration of the loop.
			let Some(x) = self.reader.peek() else {
				return Ok(self.finish_float_token());
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
					return self.lex_exponent();
				}
				b'_' => {
					self.reader.next();
				}
				b'f' | b'd' => return self.lex_suffix(false),
				b'a'..=b'z' | b'A'..=b'Z' => {
					// invalid token, random identifier characters immediately after number.
					self.scratch.clear();
					return Err(Error::InvalidSuffix);
				}
				_ => {
					return Ok(self.finish_float_token());
				}
			}
		}
	}

	/// Lexes the exponent of a number, i.e. `e10` in `1.1e10`;
	fn lex_exponent(&mut self) -> Result<Token, Error> {
		let mut atleast_one = false;
		match self.reader.peek() {
			Some(b'-' | b'+') => {}
			Some(b'0'..=b'9') => {
				atleast_one = true;
			}
			_ => {
				// random other character, expected atleast one digit.
				return Err(Error::DigitExpectedExponent);
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
				Some(b'f' | b'd') => return self.lex_suffix(false),
				_ => {
					if atleast_one {
						return Ok(self.finish_float_token());
					} else {
						return Err(Error::DigitExpectedExponent);
					}
				}
			}
		}
	}

	/// Parse the float in the scratch buffer and return it as a token
	pub fn finish_float_token(&mut self) -> Token {
		// Lexer should ensure this never panics.
		let result = self.scratch.parse::<f64>().unwrap();
		self.scratch.clear();
		self.finish_number_token(Number::Float(result))
	}

	/// Parse the integer in the scratch buffer and return it as a token
	pub fn finish_int_token(&mut self) -> Token {
		// TODO: Improve parsing of numbers so that we can use full range of number.
		let result = self.scratch.parse::<f64>().unwrap() as i64;
		self.scratch.clear();
		self.finish_number_token(Number::Int(result))
	}
}
