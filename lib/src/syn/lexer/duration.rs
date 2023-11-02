use std::time::Duration;
use thiserror::Error;

use crate::{
	sql::duration::{
		SECONDS_PER_DAY, SECONDS_PER_HOUR, SECONDS_PER_MINUTE, SECONDS_PER_WEEK, SECONDS_PER_YEAR,
	},
	syn::token::{Token, TokenKind},
};

use super::{Error as LexError, Lexer};

#[derive(Error, Debug)]
pub enum Error {
	#[error("invalid duration suffix")]
	InvalidSuffix,
	#[error("duration value overflowed")]
	Overflow,
}

impl<'a> Lexer<'a> {
	pub fn lex_duration(&mut self) -> Token {
		match self.lex_duration_err() {
			Ok(x) => x,
			Err(e) => self.invalid_token(LexError::Duration(e)),
		}
	}

	/// Lex a duration,
	///
	/// Should only be called from lexing a number.
	///
	/// Expects any number but at least one numeric characters be pushed into scratch.
	pub fn lex_duration_err(&mut self) -> Result<Token, Error> {
		let mut duration = Duration::ZERO;

		let mut current_value = 0u64;
		// use the existing eat span to generate the current value.
		// span already contains
		let mut span = self.current_span();
		span.len -= 1;
		for b in self.reader.span(self.current_span()).iter().copied() {
			debug_assert!(b.is_ascii_digit(), "`{}` is not a digit", b as char);
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
						Duration::from_nanos(current_value)
					} else {
						Duration::from_micros(current_value)
					}
				}
				// Starting byte of 'µ'
				0xc2 => {
					self.reader.next();
					// Second byte of 'µ'.
					// Always consume as the next byte will always be part of a two byte character.
					if !self.eat(0xb5) {
						return Err(Error::InvalidSuffix);
					}

					if !self.eat(b's') {
						return Err(Error::InvalidSuffix);
					}

					Duration::from_micros(current_value)
				}
				b'm' => {
					self.reader.next();
					// Either milli or minute
					let is_milli = self.eat(b's');

					if is_milli {
						Duration::from_millis(current_value)
					} else {
						let Some(number) = current_value.checked_mul(SECONDS_PER_MINUTE) else {
							return Err(Error::Overflow);
						};
						Duration::from_secs(number)
					}
				}
				x @ (b's' | b'h' | b'd' | b'w' | b'y') => {
					self.reader.next();
					// second, hour, day, week or year.

					let new_duration = match x {
						b's' => Some(Duration::from_secs(current_value)),
						b'h' => {
							current_value.checked_mul(SECONDS_PER_HOUR).map(Duration::from_secs)
						}
						b'd' => current_value.checked_mul(SECONDS_PER_DAY).map(Duration::from_secs),
						b'w' => {
							current_value.checked_mul(SECONDS_PER_WEEK).map(Duration::from_secs)
						}
						b'y' => {
							current_value.checked_mul(SECONDS_PER_YEAR).map(Duration::from_secs)
						}
						_ => unreachable!(),
					};

					let Some(new_duration) = new_duration else {
						return Err(Error::Overflow);
					};
					new_duration
				}
				_ => {
					return Err(Error::InvalidSuffix);
				}
			};

			duration = duration.checked_add(new_duration).ok_or(Error::Overflow)?;

			let next = self.reader.peek();
			match next {
				// there was some remaining alphabetic characters after the valid suffix, so the
				// suffix is invalid.
				Some(b'a'..=b'z' | b'A'..=b'Z' | b'_') => return Err(Error::InvalidSuffix),
				Some(b'0'..=b'9') => {} // Duration continues.
				_ => {
					// Duration done.
					let index = (self.durations.len() as u32).into();
					self.durations.push(duration);
					self.scratch.clear();
					return Ok(self.finish_token(TokenKind::Duration, Some(index)));
				}
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
