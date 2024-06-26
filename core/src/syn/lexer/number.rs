use crate::syn::token::{Token, TokenKind};

use super::Lexer;

impl Lexer<'_> {
	pub fn lex_digits(&mut self) -> Token {
		while let Some(b'0'..=b'9' | b'_') = self.reader.peek() {
			self.reader.next();
		}

		self.finish_token(TokenKind::Digits)
	}

	pub fn lex_exponent(&mut self, start: u8) -> Token {
		if let Some(x) = self.reader.peek() {
			if x.is_ascii_alphabetic() || x == b'_' {
				self.scratch.push(start as char);
				return self.lex_ident();
			}
		};

		self.finish_token(TokenKind::Exponent)
	}
}
