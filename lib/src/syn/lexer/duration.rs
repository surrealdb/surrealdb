use std::time::Duration;

use crate::{
	sql::duration::{
		SECONDS_PER_DAY, SECONDS_PER_HOUR, SECONDS_PER_MINUTE, SECONDS_PER_WEEK, SECONDS_PER_YEAR,
	},
	syn::token::{Token, TokenKind},
};

use super::Lexer;

impl<'a> Lexer<'a> {
	/// Lex a duration,
	///
	/// Should only be called from lexing a number.
	///
	/// Expects any number but at least one numeric characters be pushed into scratch.
	pub fn lex_duration(&mut self) -> Token {
		let mut duration = Duration::ZERO;

		loop {
			let Some(next) = self.reader.peek() else {
				self.scratch.clear();
				return self.eof_token();
			};

			// Match the suffix.
			match next {
				x @ (b'n' | b'u') => {
					// Nano or micro suffix
					self.reader.next();
					let Some(b's') = self.reader.peek() else {
						self.eat_remaining_identifier();
						self.scratch.clear();
						return self.invalid_token();
					};
					self.reader.next();

					let Ok(number) = self.scratch.parse() else {
						// Can only happen if the number is too big.
						// TODO: Should probably handle to big numbers with a specific error.
						self.scratch.clear();
						return self.invalid_token();
					};

					duration += if x == b'n' {
						Duration::from_nanos(number)
					} else {
						Duration::from_micros(number)
					};
					self.scratch.clear();
				}
				// Starting byte of 'µ'
				0xc2 => {
					self.reader.next();
					// Second byte of 'µ'.
					// Always consume as the next byte will always be part of a two byte character.
					let Some(0xb5) = self.reader.next() else {
						// can no longer be a valid identifier
						self.eat_remaining_identifier();
						self.scratch.clear();
						return self.invalid_token();
					};

					let Some(b's') = self.reader.peek() else {
						// can no longer be a valid identifier
						self.eat_remaining_identifier();
						self.scratch.clear();
						return self.invalid_token();
					};
					self.reader.next();

					let Ok(number) = self.scratch.parse() else {
						// Can only happen if the number is too big.
						// TODO: Should probably handle to big numbers with a specific error.
						self.scratch.clear();
						return self.invalid_token();
					};

					duration += Duration::from_micros(number);
					self.scratch.clear();
				}
				b'm' => {
					self.reader.next();
					// Either milli or minute
					let is_milli = self.reader.peek() == Some(b's');
					if is_milli {
						self.reader.next();
					}

					let Ok(number) = self.scratch.parse() else {
						// Can only happen if the number is too big.
						// TODO: Should probably handle to big numbers with a specific error.
						self.scratch.clear();
						return self.invalid_token();
					};

					duration += if is_milli {
						Duration::from_millis(number)
					} else {
						let Some(number) = number.checked_mul(SECONDS_PER_MINUTE) else {
							self.scratch.clear();
							return self.invalid_token();
						};
						Duration::from_secs(number)
					};
					self.scratch.clear();
				}
				x @ (b's' | b'h' | b'd' | b'w' | b'y') => {
					self.reader.next();
					// second, hour, day, week or year.

					let Ok(number) = self.scratch.parse() else {
						// Can only happen if the number is too big.
						// TODO: Should probably handle to big numbers with a specific error.
						self.scratch.clear();
						return self.invalid_token();
					};

					let new_duration = match x {
						b's' => Some(Duration::from_secs(number)),
						b'h' => number.checked_mul(SECONDS_PER_HOUR).map(Duration::from_secs),
						b'd' => number.checked_mul(SECONDS_PER_DAY).map(Duration::from_secs),
						b'w' => number.checked_mul(SECONDS_PER_WEEK).map(Duration::from_secs),
						b'y' => number.checked_mul(SECONDS_PER_YEAR).map(Duration::from_secs),
						_ => unreachable!(),
					};

					let Some(new_duration) = new_duration else {
						self.scratch.clear();
						return self.invalid_token();
					};
					duration += new_duration;
					self.scratch.clear();
				}

				_ => {
					self.eat_remaining_identifier();
					self.scratch.clear();
					return self.invalid_token();
				}
			}

			let next = self.reader.peek();

			match next {
				Some(b'a'..=b'z' | b'A'..=b'Z' | b'_') => {
					// Alphabetic character after a suffix.
					// Not a duration.
					self.eat_remaining_identifier();
					self.scratch.clear();
					return self.invalid_token();
				}
				Some(b'0'..=b'9') => {} // Duration continues.
				_ => {
					// Duration done.
					let index = (self.durations.len() as u32).into();
					self.durations.push(duration);
					self.scratch.clear();
					return self.finish_token(TokenKind::Duration, Some(index));
				}
			}

			// Eat all the next numbers
			while let Some(x @ b'0'..=b'9') = self.reader.peek() {
				self.reader.next();
				self.scratch.push(x as char);
			}
		}
	}
}
