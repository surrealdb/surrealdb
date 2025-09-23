use crate::syn::error::{SyntaxError, bail, syntax_error};
use crate::syn::lexer::Lexer;
use crate::syn::token::{Token, t};
use crate::val::Regex;

pub fn regex(lexer: &mut Lexer, start: Token) -> Result<Regex, SyntaxError> {
	assert_eq!(start.kind, t!("/"), "Invalid start token of regex compound");

	loop {
		match lexer.reader.next() {
			Some(b'\\') => {
				if let Some(x) = lexer.reader.next() {
					lexer.reader.complete_char(x)?;
				}
			}
			Some(b'/') => break,
			Some(x) if x.is_ascii() => {}
			Some(x) => {
				if let Err(e) = lexer.reader.complete_char(x) {
					let span = lexer.current_span();
					bail!("Invalid token: {e}", @span);
				}
			}
			None => {
				let span = lexer.current_span();
				bail!("Failed to lex regex, unexpected eof", @span);
			}
		}
	}

	let mut span = lexer.current_span();
	// the `\`
	span.len -= 2;
	span.offset += 1;

	// Safety: We checked the bytes for utf-8 validity so this is sound.
	let s = unsafe { std::str::from_utf8_unchecked(lexer.span_bytes(span)) };

	let regex = s.parse().map_err(|e| syntax_error!("Invalid regex: {e}", @span))?;
	Ok(regex)
}
