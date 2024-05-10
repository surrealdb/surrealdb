use std::time::Duration as StdDuration;
use thiserror::Error;

use crate::{
	sql::duration::{
		Duration, SECONDS_PER_DAY, SECONDS_PER_HOUR, SECONDS_PER_MINUTE, SECONDS_PER_WEEK,
		SECONDS_PER_YEAR,
	},
	syn::token::{Token, TokenKind},
};

use super::{Error as LexError, Lexer};

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
	#[error("invalid duration suffix")]
	InvalidSuffix,
	#[error("duration value overflowed")]
	Overflow,
}

impl<'a> Lexer<'a> {
	/// Lex a duration.
	///
	/// Expect the lexer to have already eaten the digits starting the duration.
	pub fn lex_duration(&mut self) -> Token {
		let backup = self.reader.offset();
		match self.lex_duration_err() {
			Ok(x) => {
				self.scratch.clear();
				self.duration = Some(x);
				self.finish_token(TokenKind::Duration)
			}
			Err(e) => {
				if self.flexible_ident {
					self.reader.backup(backup);
					return self.lex_ident();
				}
				self.scratch.clear();
				self.invalid_token(LexError::Duration(e))
			}
		}
	}

	fn invalid_suffix_duration(&mut self) -> Error {
		// eat the whole suffix.
		while let Some(x) = self.reader.peek() {
			if !x.is_ascii_alphanumeric() {
				break;
			}
			self.reader.next();
		}
		Error::InvalidSuffix
	}

	/// Lex a duration,
	///
	/// Should only be called from lexing a number.
	///
	/// Expects any number but at least one numeric characters be pushed into scratch.
	pub fn lex_duration_err(&mut self) -> Result<Duration, Error> {
		let mut duration = StdDuration::ZERO;

		let mut current_value = 0u64;
		// use the existing eat span to generate the current value.
		// span already contains
		let mut span = self.current_span();
		span.len -= 1;
		for b in self.scratch.as_bytes() {
			debug_assert!(b.is_ascii_digit(), "`{}` is not a digit", b);
			current_value = current_value.checked_mul(10).ok_or(Error::Overflow)?;
			current_value = current_value.checked_add((b - b'0') as u64).ok_or(Error::Overflow)?;
		}

		loop {
			let Some(next) = self.reader.peek() else {
				return Err(Error::InvalidSuffix);
			};

			// Match the suffix.
			let new_duration = match next {
				x @ (b'n' | b'u') => {
					// Nano or micro suffix
					self.reader.next();
					if !self.eat(b's') {
						return Err(Error::InvalidSuffix);
					};

					if x == b'n' {
						StdDuration::from_nanos(current_value)
					} else {
						StdDuration::from_micros(current_value)
					}
				}
				// Starting byte of 'µ'
				0xc2 => {
					self.reader.next();
					// Second byte of 'µ'.
					// Always consume as the next byte will always be part of a two byte character.
					if !self.eat(0xb5) {
						return Err(self.invalid_suffix_duration());
					}

					if !self.eat(b's') {
						return Err(self.invalid_suffix_duration());
					}

					StdDuration::from_micros(current_value)
				}
				b'm' => {
					self.reader.next();
					// Either milli or minute
					let is_milli = self.eat(b's');

					if is_milli {
						StdDuration::from_millis(current_value)
					} else {
						let Some(number) = current_value.checked_mul(SECONDS_PER_MINUTE) else {
							return Err(Error::Overflow);
						};
						StdDuration::from_secs(number)
					}
				}
				x @ (b's' | b'h' | b'd' | b'w' | b'y') => {
					self.reader.next();
					// second, hour, day, week or year.

					let new_duration = match x {
						b's' => Some(StdDuration::from_secs(current_value)),
						b'h' => {
							current_value.checked_mul(SECONDS_PER_HOUR).map(StdDuration::from_secs)
						}
						b'd' => {
							current_value.checked_mul(SECONDS_PER_DAY).map(StdDuration::from_secs)
						}
						b'w' => {
							current_value.checked_mul(SECONDS_PER_WEEK).map(StdDuration::from_secs)
						}
						b'y' => {
							current_value.checked_mul(SECONDS_PER_YEAR).map(StdDuration::from_secs)
						}
						_ => unreachable!(),
					};

					let Some(new_duration) = new_duration else {
						return Err(Error::Overflow);
					};
					new_duration
				}
				_ => {
					return Err(self.invalid_suffix_duration());
				}
			};

			duration = duration.checked_add(new_duration).ok_or(Error::Overflow)?;

			let next = self.reader.peek();
			match next {
				// there was some remaining alphabetic characters after the valid suffix, so the
				// suffix is invalid.
				Some(b'a'..=b'z' | b'A'..=b'Z' | b'_') => {
					return Err(self.invalid_suffix_duration())
				}
				Some(b'0'..=b'9') => {} // Duration continues.
				_ => return Ok(Duration(duration)),
			}

			current_value = 0;
			// Eat all the next numbers
			while let Some(b @ b'0'..=b'9') = self.reader.peek() {
				self.reader.next();
				current_value = current_value.checked_mul(10).ok_or(Error::Overflow)?;
				current_value =
					current_value.checked_add((b - b'0') as u64).ok_or(Error::Overflow)?;
			}
		}
	}
}
