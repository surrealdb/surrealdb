use crate::syn::token::{Token, TokenKind};

use super::Lexer;

impl Lexer<'_> {
	pub fn lex_digits(&mut self, start: u8) -> Token {
		while let Some(b'0'..=b'9' | b'_') = self.reader.peek() {
			self.reader.next();
		}

		self.finish_token(TokenKind::Digits)
	}

	pub fn lex_exponent(&mut self, start: u8) -> Token {
		let Some(b'0'..=b'9') = self.reader.peek() else {
			self.scratch.push(start as char);
			return self.lex_ident();
		};

		while let Some(b'0'..=b'9' | b'_') = self.reader.peek() {
			self.reader.next();
		}

		self.finish_token(TokenKind::Exponent)
	}
}
