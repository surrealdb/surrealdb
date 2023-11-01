use crate::syn::token::{Token, TokenKind};

use super::{unicode::chars, CharError, Lexer};

impl<'a> Lexer<'a> {
	/// Lex a strand with either double or single quotes.
	pub fn lex_strand(&mut self, is_double: bool) -> Token {
		loop {
			let Some(x) = self.reader.next() else {
				self.scratch.clear();
				return self.eof_token();
			};

			if x.is_ascii() {
				match x {
					b'\'' if !is_double => {
						return self.finish_string_token(TokenKind::Strand);
					}
					b'"' if is_double => {
						return self.finish_string_token(TokenKind::Strand);
					}
					b'\0' => {
						// null bytes not allowed
						self.scratch.clear();
						return self.invalid_token();
					}
					b'\\' => {
						// Handle escape sequences.
						let Some(next) = self.reader.next() else {
							self.scratch.clear();
							return self.eof_token();
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
							_ => {
								self.scratch.clear();
								return self.invalid_token();
							}
						}
					}
					x => self.scratch.push(x as char),
				}
			} else {
				let c = match self.reader.complete_char(x) {
					Ok(x) => x,
					Err(CharError::Eof) => {
						self.scratch.clear();
						return self.eof_token();
					}
					Err(CharError::Unicode) => {
						self.scratch.clear();
						return self.invalid_token();
					}
				};
				self.scratch.push(c);
			}
		}
	}

	pub fn lex_date_time(&mut self, double: bool) -> Token {
		match self.reader.peek() {
			Some(b'+') => {
				self.scratch.push('+');
				self.reader.next();
			}
			Some(b'-') => {
				self.scratch.push('-');
				self.reader.next();
			}
			_ => {}
		}
		// year
		if !self.lex_digits(4) {
			return self.invalid_token();
		}
		let Some(b'-') = self.reader.next() else {
			return self.invalid_token();
		};
		self.scratch.push('-');
		// month
		if !self.lex_digits(2) {
			return self.invalid_token();
		}
		let Some(b'-') = self.reader.next() else {
			return self.invalid_token();
		};
		self.scratch.push('-');
		// day
		if !self.lex_digits(2) {
			return self.invalid_token();
		}
		let Some(b'T') = self.reader.next() else {
			return self.invalid_token();
		};
		self.scratch.push('T');
		// hour
		if !self.lex_digits(2) {
			return self.invalid_token();
		}
		let Some(b'-') = self.reader.next() else {
			return self.invalid_token();
		};
		self.scratch.push('-');
		// minutes
		if !self.lex_digits(2) {
			return self.invalid_token();
		}
		let Some(b'-') = self.reader.next() else {
			return self.invalid_token();
		};
		self.scratch.push('-');
		// seconds
		if !self.lex_digits(2) {
			return self.invalid_token();
		}

		// nano seconds
		if let Some(b'.') = self.reader.peek() {
			self.reader.next();

			loop {
				let Some(char) = self.reader.peek() else {
					break;
				};
				if !char.is_ascii_digit() {
					break;
				}
				self.reader.next();
				self.scratch.push(char as char);
			}
		}

		// time zone
		match self.reader.peek() {
			Some(b'Z') => {
				self.reader.next();
				self.scratch.push('Z');
			}
			Some(x @ (b'-' | b'+')) => {
				self.reader.next();
				self.scratch.push(x as char);

				if !self.lex_digits(2) {
					return self.invalid_token();
				}
				let Some(b':') = self.reader.next() else {
					return self.invalid_token();
				};
				self.scratch.push(':');
				if !self.lex_digits(2) {
					return self.invalid_token();
				}
			}
			_ => return self.invalid_token(),
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

		self.finish_string_token(TokenKind::DateTime)
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

	pub fn lex_digits(&mut self, mut amount: u8) -> bool {
		while amount != 0 {
			let Some(char) = self.reader.peek() else {
				return false;
			};
			if !char.is_ascii_digit() {
				return false;
			}
			self.reader.next();
			self.scratch.push(char as char);
			amount -= 1;
		}
		true
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
