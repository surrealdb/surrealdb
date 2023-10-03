use crate::syn::lexer::Lexer;
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
					self.reader.next();
					self.scratch.push(start as char);
				}
				b'.' => {
					let backup = self.reader.offset();
					self.reader.next();
					let next = self.reader.peek();
					if let Some(b'0'..=b'9') = next {
						self.scratch.push(next.unwrap() as char);
						return self.lex_mantissa(backup);
					} else {
						self.reader.backup(backup);
						self.scratch.clear();
						return self.finish_token(TokenKind::Number, None);
					}
				}
				b'f' => {
					self.reader.next();
					if let Some(b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9') = self.reader.peek() {
						return self.lex_ident_from_next_byte(b'f');
					} else {
						self.scratch.clear();
						return self.finish_token(TokenKind::Number, None);
					}
				}
				b'd' => {
					let checkpoint = self.reader.offset();
					self.reader.next();
					let Some(b'e') = self.reader.peek() else {
						// 'e' isn't next so it could be a duration
						self.reader.backup(checkpoint);
						return self.lex_duration();
					};
					self.reader.next();
					let Some(b'c') = self.reader.peek() else {
						self.scratch.push('d');
						return self.lex_ident_from_next_byte(b'e');
					};
					self.reader.next();

					if let Some(b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9') = self.reader.peek() {
						self.scratch.push('d');
						self.scratch.push('e');
						return self.lex_ident_from_next_byte(b'c');
					} else {
						self.scratch.clear();
						return self.finish_token(TokenKind::Number, None);
					}
				}
				// Oxc2 is the start byte of 'µ'
				0xc2 | b'n' | b'u' | b'm' | b'h' | b'w' | b'y' | b's' => {
					return self.lex_duration()
				}
				b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
					return self.lex_ident_from_next_byte(x);
				}
				_ => {
					self.scratch.clear();
					return self.finish_token(TokenKind::Number, None);
				}
			}
		}
	}

	pub fn lex_mantissa(&mut self, backup: usize) -> Token {
		let len = self.scratch.len();
		self.scratch.push('.');
		loop {
			let Some(x) = self.reader.peek() else {
				return self.finish_token(TokenKind::Number, None);
			};
			match x {
				b'0'..=b'9' => {
					self.reader.next();
					self.scratch.push(x as char);
				}
				b'a'..=b'z' | b'A'..=b'Z' => {
					self.reader.backup(backup);
					self.scratch.truncate(len);
					return self.finish_token(TokenKind::Number, None);
				}
				_ => {
					self.scratch.clear();
					return self.finish_token(TokenKind::Number, None);
				}
			}
		}
	}
}
