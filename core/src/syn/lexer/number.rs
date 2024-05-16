use std::mem;

use crate::syn::token::{Token, TokenKind};

use super::Lexer;

impl Lexer<'_> {
	pub fn lex_digits(&mut self, start: u8) -> Token {
		self.scratch.push(start as char);

		while let Some(x @ (b'0'..=b'9' | b'_')) = self.reader.peek() {
			self.reader.next();
			if x != b'_' {
				self.scratch.push(x as char);
			}
		}

		self.string = Some(mem::take(&mut self.scratch));
		self.finish_token(TokenKind::Digits)
	}

	pub fn lex_exponent(&mut self, start: u8) -> Token {
		self.scratch.push(start as char);

		let Some(b'0'..=b'9') = self.reader.peek() else {
			return self.lex_ident();
		};

		self.scratch.push(self.reader.next().unwrap() as char);

		while let Some(x @ (b'0'..=b'9' | b'_')) = self.reader.peek() {
			self.reader.next();
			if x != b'_' {
				self.scratch.push(x as char);
			}
		}

		self.string = Some(mem::take(&mut self.scratch));
		self.finish_token(TokenKind::Exponent)
	}
}
