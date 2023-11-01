use crate::sql::Number;
use crate::syn::lexer::{unicode::U8Ext, Lexer};
use crate::syn::token::{Token, TokenKind};

impl Lexer<'_> {
	pub fn lex_number(&mut self, start: u8) -> Token {
		self.scratch.push(start as char);
		loop {
			let Some(x) = self.reader.peek() else {
				return self.finish_token(TokenKind::Number, None);
			};
			match x {
				b'0'..=b'9' => {
					// next digits.
					self.reader.next();
					self.scratch.push(start as char);
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
						return self.finish_int_token();
					}
				}
				b'f' => {
					// float suffix
					self.reader.next();
					if let Some(true) = self.reader.peek().map(|x| x.is_identifier_continue()) {
						self.eat_remaining_identifier();
						return self.invalid_token();
					} else {
						return self.finish_float_token();
					}
				}
				b'd' => {
					// decimal suffix
					let checkpoint = self.reader.offset();
					self.reader.next();
					let Some(b'e') = self.reader.peek() else {
						// 'e' isn't next so it could be a duration
						self.reader.backup(checkpoint);
						return self.lex_duration();
					};
					self.reader.next();

					let Some(b'c') = self.reader.peek() else {
						// 'de' isn't a valid suffix,
						self.eat_remaining_identifier();
						self.scratch.clear();
						return self.invalid_token();
					};
					self.reader.next();

					if let Some(true) = self.reader.peek().map(|x| x.is_identifier_continue()) {
						// random identifier like tokens after suffix, invalid token.
						self.eat_remaining_identifier();
						self.scratch.clear();
						return self.invalid_token();
					} else {
						return self.finish_int_token();
					}
				}
				// Oxc2 is the start byte of 'µ'
				0xc2 | b'n' | b'u' | b'm' | b'h' | b'w' | b'y' | b's' => {
					// duration suffix, switch to lexing duration.
					return self.lex_duration();
				}
				b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
					// invalid token, unexpected identifier character immediatly after number.
					// Eat all remaining identifier like characters.
					self.eat_remaining_identifier();
					self.scratch.clear();
					return self.invalid_token();
				}
				_ => {
					return self.finish_int_token();
				}
			}
		}
	}

	/// Eats all remaining identifier like character.
	pub fn eat_remaining_identifier(&mut self) {
		while let Some(true) = self.reader.peek().map(|x| x.is_identifier_continue()) {
			self.reader.next();
		}
	}

	/// Lexes the mantissa of a number, i.e. `.8` in `1.8`
	pub fn lex_mantissa(&mut self) -> Token {
		self.scratch.push('.');
		loop {
			let Some(x) = self.reader.peek() else {
				return self.finish_float_token();
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
				b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
					// invalid token, random identifier characters immediately after number.
					self.scratch.clear();
					return self.invalid_token();
				}
				_ => {
					return self.finish_float_token();
				}
			}
		}
	}

	pub fn lex_exponent(&mut self) -> Token {
		let mut atleast_one = false;
		match self.reader.peek() {
			Some(b'-' | b'+') => {}
			Some(b'0'..=b'9') => {
				atleast_one = true;
			}
			_ => {
				// random other character, expected atleast one digit.
				return self.invalid_token();
			}
		}
		self.reader.next();
		loop {
			match self.reader.peek() {
				Some(x @ b'0'..=b'9') => {
					self.scratch.push(x as char);
				}
				_ => {
					if atleast_one {
						return self.finish_float_token();
					} else {
						return self.invalid_token();
					}
				}
			}
		}
	}

	/// Parse the float in the scratch buffer and return it as a token
	pub fn finish_float_token(&mut self) -> Token {
		let result = self.scratch.parse::<f64>();
		self.scratch.clear();
		match result {
			Ok(x) => self.finish_number_token(Number::Float(x)),
			Err(_) => self.invalid_token(),
		}
	}

	/// Parse the integer in the scratch buffer and return it as a token
	pub fn finish_int_token(&mut self) -> Token {
		let result = self.scratch.parse::<i64>();
		self.scratch.clear();
		match result {
			Ok(x) => self.finish_number_token(Number::Int(x)),
			Err(_) => self.invalid_token(),
		}
	}
}
