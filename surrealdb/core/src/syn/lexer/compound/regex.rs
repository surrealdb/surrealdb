use crate::syn::error::{SyntaxError, bail};
use crate::syn::lexer::Lexer;
use crate::syn::token::{Token, t};

pub fn regex(lexer: &mut Lexer, start: Token) -> Result<(), SyntaxError> {
	assert_eq!(start.kind, t!("/"), "Invalid start token of regex compound");

	loop {
		match lexer.reader.next() {
			Some(b'\\') => {
				if let Some(x) = lexer.reader.next() {
					lexer.reader.convert_to_char(x)?;
				}
			}
			Some(b'/') => break,
			Some(x) if x.is_ascii() => {}
			Some(x) => {
				lexer.reader.complete_char(x)?;
			}
			None => {
				let span = lexer.current_span();
				bail!("Failed to lex regex, unexpected eof", @span);
			}
		}
	}

	Ok(())
}
