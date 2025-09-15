use crate::syn::error::syntax_error;
use crate::syn::lexer::Lexer;
use crate::syn::token::{Token, TokenKind, t};

impl Lexer<'_> {
	/// lex non-ascii characters.
	///
	/// Should only be called after determining that the byte is not a valid
	/// ascii character.
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
			'\u{00A0}' | '\u{1680}' | '\u{2000}' | '\u{2001}' | '\u{2002}' | '\u{2003}'
			| '\u{2004}' | '\u{2005}' | '\u{2006}' | '\u{2007}' | '\u{2008}' | '\u{2009}'
			| '\u{200A}' | '\u{202F}' | '\u{205F}' | '\u{3000}' => {
				self.eat_whitespace();
				TokenKind::WhiteSpace
			}
			x => {
				let err = syntax_error!("Invalid token `{x}`", @self.current_span());
				return self.invalid_token(err);
			}
		};
		self.finish_token(kind)
	}
}
