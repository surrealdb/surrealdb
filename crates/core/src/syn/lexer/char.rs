use crate::syn::{
	error::syntax_error,
	lexer::Lexer,
	token::{t, Token},
};

impl<'a> Lexer<'a> {
	/// lex non-ascii characters.
	///
	/// Should only be called after determining that the byte is not a valid ascii character.
	pub(super) fn lex_char(&mut self, byte: u8) -> Token {
		let c = match self.reader.complete_char(byte) {
			Ok(x) => x,
			Err(e) => return self.invalid_token(e.into()),
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
			x => {
				let err = syntax_error!("Invalid token `{x}`", @self.current_span());
				return self.invalid_token(err);
			}
		};
		self.finish_token(kind)
	}
}
