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
		// NOTE: The complexity of this function is the result of duration mostly being covered by ident.
		// All durations except for the duration with 'µs` are valid identifiers. This means that
		// the lexer must be ready at any point to move from lexing a duration to lexing a
		// identifier. Therefore we keep updating the scratch buffer so we can move back to parsing
		// an identifier if necessary.

		let mut valid_identifier = true;
		let mut duration = Duration::ZERO;
		let mut number_offset = 0;

		loop {
			let Some(next) = self.reader.peek() else {
				return self.lex_ident();
			};

			// Match the suffix.
			match next {
				x @ (b'n' | b'u') => {
					// Nano or micro
					self.reader.next();
					let Some(b's') = self.reader.peek() else {
						if valid_identifier {
							return self.lex_ident_from_next_byte(x);
						} else {
							return self.invalid_token();
						}
					};
					self.reader.next();

					let Ok(number) = self.scratch[number_offset..].parse() else {
						// Can only happen if the number is too big.
						// TODO: Should probably handle to big numbers with a specific error.
						return self.invalid_token();
					};

					duration += if x == b'n' {
						Duration::from_nanos(number)
					} else {
						Duration::from_micros(number)
					};

					// Update scratch in case we need to backup to lexing an ident.
					self.scratch.push(x as char);
					self.scratch.push('s');
					// The next number starts after the newly push suffix.
					number_offset = self.scratch.len();
				}
				// Starting byte of 'µ'
				0xc2 => {
					self.reader.next();
					// Second byte of 'µ'.
					let Some(0xb5) = self.reader.peek() else {
						// can no longer be a valid identifier
						return self.invalid_token();
					};

					let Some(b's') = self.reader.peek() else {
						// can no longer be a valid identifier
						return self.invalid_token();
					};

					let Ok(number) = self.scratch[number_offset..].parse() else {
						// Can only happen if the number is too big.
						// TODO: Should probably handle to big numbers with a specific error.
						return self.invalid_token();
					};

					duration += Duration::from_micros(number);

					valid_identifier = false;
					// Don't need to update scratch as it can't be a valid identifier anymore..
					// The next number starts after the newly push suffix.
					number_offset = self.scratch.len();
				}
				b'm' => {
					// Either milli or minute
					let is_milli = self.reader.peek() == Some(b's');
					if is_milli {
						self.reader.next();
					}

					let Ok(number) = self.scratch[number_offset..].parse() else {
						// Can only happen if the number is too big.
						// TODO: Should probably handle to big numbers with a specific error.
						return self.invalid_token();
					};

					duration += if is_milli {
						Duration::from_millis(number)
					} else {
						let Some(number) = number.checked_mul(SECONDS_PER_MINUTE) else {
							return self.invalid_token();
						};
						Duration::from_secs(number)
					};

					// Update scratch incase we need to backup to lexing an ident.
					self.scratch.push('m');
					if is_milli {
						self.scratch.push('s');
					}
					// The next number starts after the newly push suffix.
					number_offset = self.scratch.len();
				}
				x @ (b's' | b'h' | b'd' | b'w' | b'y') => {
					// second, hour, day, week or year.

					let Ok(number) = self.scratch[number_offset..].parse() else {
						// Can only happen if the number is too big.
						// TODO: Should probably handle to big numbers with a specific error.
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
						return self.invalid_token();
					};
					duration += new_duration;

					// Update scratch incase we need to backup to lexing an ident.
					self.scratch.push(x as char);
					number_offset = self.scratch.len();
				}

				_ => {
					if valid_identifier {
						return self.lex_ident();
					} else {
						return self.invalid_token();
					}
				}
			}

			let next = self.reader.peek();

			match next {
				Some(x @ (b'a'..=b'z' | b'A'..=b'Z' | b'_')) => {
					// Alphabetic character after a suffix.
					// Not a duration.
					if valid_identifier {
						self.reader.next();
						return self.lex_ident_from_next_byte(x);
					} else {
						return self.invalid_token();
					}
				}
				Some(b'0'..=b'9') => {} // Duration continues.
				_ => {
					// Duration done.
					let index = (self.durations.len() as u32).into();
					self.durations.push(duration);
					return self.finish_token(
						TokenKind::Duration {
							valid_identifier,
						},
						Some(index),
					);
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
