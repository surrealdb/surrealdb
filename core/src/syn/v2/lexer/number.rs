use crate::syn::v2::{
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
	pub fn finish_number_token(&mut self, kind: NumberKind) -> Token {
		let mut str = mem::take(&mut self.scratch);
		str.retain(|x| x != '_');
		self.string = Some(str);
		self.finish_token(TokenKind::Number(kind))
	}
	/// Lex only an integer.
	/// Use when a number can be followed immediatly by a `.` like in a model version.
	pub fn lex_only_integer(&mut self) -> Token {
		let Some(next) = self.reader.peek() else {
			return self.eof_token();
		};

		// not a number, return a different token kind, for error reporting.
		if !next.is_ascii_digit() {
			return self.next_token();
		}

		self.scratch.push(next as char);
		self.reader.next();

		// eat all the ascii digits
		while let Some(x) = self.reader.peek() {
			if !x.is_ascii_digit() && x != b'_' {
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
				return self.lex_suffix(false, false);
			}
			Some(x) if x.is_ascii_alphabetic() => return self.invalid_suffix_token(),
			_ => {}
		}

		self.finish_number_token(NumberKind::Integer)
	}

	/// Lex a number.
	///
	/// Expects the digit which started the number as the start argument.
	pub fn lex_number(&mut self, start: u8) -> Token {
		debug_assert!(start.is_ascii_digit());
		debug_assert_eq!(self.scratch, "");
		self.scratch.push(start as char);
		loop {
			let Some(x) = self.reader.peek() else {
				return self.finish_number_token(NumberKind::Integer);
			};
			match x {
				b'0'..=b'9' => {
					// next digits.
					self.reader.next();
					self.scratch.push(x as char);
				}
				x @ (b'e' | b'E') => {
					// scientific notation
					self.reader.next();
					self.scratch.push(x as char);
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
						return self.finish_number_token(NumberKind::Integer);
					}
				}
				b'f' | b'd' => return self.lex_suffix(false, false),
				// Oxc2 is the start byte of 'µ'
				0xc2 | b'n' | b'u' | b'm' | b'h' | b'w' | b'y' | b's' => {
					// duration suffix, switch to lexing duration.
					return self.lex_duration();
				}
				b'_' => {
					self.reader.next();
				}
				b'a'..=b'z' | b'A'..=b'Z' => {
					if self.flexible_ident {
						return self.lex_ident();
					} else {
						return self.invalid_suffix_token();
					}
				}
				_ => {
					return self.finish_number_token(NumberKind::Integer);
				}
			}
		}
	}

	fn invalid_suffix_token(&mut self) -> Token {
		// eat the whole suffix.
		while let Some(x) = self.reader.peek() {
			if !x.is_ascii_alphanumeric() {
				break;
			}
			self.reader.next();
		}
		self.scratch.clear();
		self.invalid_token(LexError::Number(Error::InvalidSuffix))
	}

	/// Lex a number suffix, either 'f' or 'dec'.
	fn lex_suffix(&mut self, had_mantissa: bool, had_exponent: bool) -> Token {
		match self.reader.peek() {
			Some(b'f') => {
				// float suffix
				self.reader.next();
				if let Some(true) = self.reader.peek().map(|x| x.is_identifier_continue()) {
					if self.flexible_ident && !had_mantissa {
						self.scratch.push('f');
						self.lex_ident()
					} else {
						self.invalid_suffix_token()
					}
				} else {
					let kind = if had_mantissa {
						NumberKind::FloatMantissa
					} else {
						NumberKind::Float
					};
					self.finish_number_token(kind)
				}
			}
			Some(b'd') => {
				// decimal suffix
				self.reader.next();
				let checkpoint = self.reader.offset();
				if !self.eat(b'e') {
					if !had_mantissa && !had_exponent {
						self.reader.backup(checkpoint - 1);
						return self.lex_duration();
					} else if !had_mantissa && self.flexible_ident {
						self.scratch.push('d');
						return self.lex_ident();
					} else {
						return self.invalid_suffix_token();
					}
				}

				if !self.eat(b'c') {
					if self.flexible_ident {
						self.scratch.push('d');
						self.scratch.push('e');
						return self.lex_ident();
					} else {
						return self.invalid_suffix_token();
					}
				}

				if let Some(true) = self.reader.peek().map(|x| x.is_identifier_continue()) {
					self.invalid_suffix_token()
				} else {
					let kind = if had_exponent {
						NumberKind::DecimalExponent
					} else {
						NumberKind::Decimal
					};
					self.finish_number_token(kind)
				}
			}
			// Caller should ensure this is unreachable
			_ => unreachable!(),
		}
	}

	/// Lexes the mantissa of a number, i.e. `.8` in `1.8`
	pub fn lex_mantissa(&mut self) -> Token {
		loop {
			// lex_number already checks if there exists a digit after the dot.
			// So this will never fail the first iteration of the loop.
			let Some(x) = self.reader.peek() else {
				return self.finish_number_token(NumberKind::Mantissa);
			};
			match x {
				b'0'..=b'9' | b'_' => {
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
				b'f' | b'd' => return self.lex_suffix(true, false),
				b'a'..=b'z' | b'A'..=b'Z' => {
					// invalid token, random identifier characters immediately after number.
					self.scratch.clear();
					return self.invalid_suffix_token();
				}
				_ => {
					return self.finish_number_token(NumberKind::Mantissa);
				}
			}
		}
	}

	/// Lexes the exponent of a number, i.e. `e10` in `1.1e10`;
	fn lex_exponent(&mut self, had_mantissa: bool) -> Token {
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
					if self.flexible_ident && !had_mantissa {
						return self.lex_ident();
					}
					// random other character, expected atleast one digit.
					return self.invalid_token(LexError::Number(Error::DigitExpectedExponent));
				}
			}
		}
		self.reader.next();
		loop {
			match self.reader.peek() {
				Some(x @ (b'0'..=b'9' | b'_')) => {
					self.reader.next();
					self.scratch.push(x as char);
				}
				Some(b'f' | b'd') => return self.lex_suffix(had_mantissa, true),
				_ => {
					let kind = if had_mantissa {
						NumberKind::MantissaExponent
					} else {
						NumberKind::Exponent
					};
					return self.finish_number_token(kind);
				}
			}
		}
	}

	#[test]
	fn weird_things() {
		use crate::sql;

		fn assert_ident_parses_correctly(ident: &str) {
			let thing = format!("t:{}", ident);
			let mut parser = Parser::new(thing.as_bytes());
			parser.allow_fexible_record_id(true);
			let mut stack = Stack::new();
			let r = stack
				.enter(|ctx| async move { parser.parse_thing(ctx).await })
				.finish()
				.expect(&format!("failed on {}", ident))
				.id;
			assert_eq!(r, Id::String(ident.to_string()),);

			let mut parser = Parser::new(thing.as_bytes());
			let r = stack
				.enter(|ctx| async move { parser.parse_query(ctx).await })
				.finish()
				.expect(&format!("failed on {}", ident));

			assert_eq!(
				r,
				sql::Query(sql::Statements(vec![sql::Statement::Value(sql::Value::Thing(
					sql::Thing {
						tb: "t".to_string(),
						id: Id::String(ident.to_string())
					}
				))]))
			)
		}

		assert_ident_parses_correctly("123abc");
		assert_ident_parses_correctly("123d");
		assert_ident_parses_correctly("123de");
		assert_ident_parses_correctly("123dec");
		assert_ident_parses_correctly("1e23dec");
		assert_ident_parses_correctly("1e23f");
		assert_ident_parses_correctly("123f");
		assert_ident_parses_correctly("1ns");
		assert_ident_parses_correctly("1ns1");
		assert_ident_parses_correctly("1ns1h");
	}
}
