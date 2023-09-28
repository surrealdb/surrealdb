use crate::sql::lexer::Lexer;
use crate::sql::token::{Token, TokenKind};

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
				b'f' => {
					self.reader.next();
					if let Some(b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9') = self.reader.peek() {
						return self.lex_ident(b'f');
					} else {
						self.scratch.clear();
						return self.finish_token(TokenKind::Number, None);
					}
				}
				b'd' => {
					self.reader.next();
					if let Some(b'e') = self.reader.peek() {
						self.reader.next();
						if let Some(b'c') = self.reader.peek() {
							self.reader.next();
							if let Some(b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9') =
								self.reader.peek()
							{
								self.scratch.push('d');
								self.scratch.push('e');
								return self.lex_ident(b'c');
							} else {
								self.scratch.clear();
								return self.finish_token(TokenKind::Number, None);
							}
						}
						self.scratch.push('d');
						return self.lex_ident(b'e');
					}
					return self.lex_ident(b'd');
				}
				b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
					return self.lex_ident(x);
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
