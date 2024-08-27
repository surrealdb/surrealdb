use crate::syn::{
	lexer::{CharError, Lexer},
	token::{t, Token},
};

use super::Error;

impl<'a> Lexer<'a> {
	/// lex non-ascii characters.
	///
	/// Should only be called after determining that the byte is not a valid ascii character.
	pub fn lex_char(&mut self, byte: u8) -> Token {
		let c = match self.reader.complete_char(byte) {
			Ok(x) => x,
			Err(CharError::Eof) => return self.invalid_token(Error::InvalidUtf8),
			Err(CharError::Unicode) => return self.invalid_token(Error::InvalidUtf8),
		};
		let kind = match c {
			'⟨' => return self.lex_surrounded_ident(false),
			'…' => t!("..."),
			'∋' => t!("∋"),
			'∌' => t!("∌"),
			'∈' => t!("∈"),
			'∉' => t!("∉"),
			'⊇' => t!("⊇"),
			'⊃' => t!("⊃"),
			'⊅' => t!("⊅"),
			'⊆' => t!("⊆"),
			'⊂' => t!("⊂"),
			'⊄' => t!("⊄"),
			'×' => t!("×"),
			'÷' => t!("÷"),
			'µ' => {
				let Some(b's') = self.reader.peek() else {
					return self.invalid_token(Error::UnexpectedCharacter('µ'));
				};
				self.reader.next();

				if self.reader.peek().map(|x| x.is_ascii_alphabetic()).unwrap_or(false) {
					return self.invalid_token(Error::UnexpectedCharacter('µ'));
				}

				t!("µs")
			}
			x => return self.invalid_token(Error::UnexpectedCharacter(x)),
		};
		self.finish_token(kind)
	}
}
