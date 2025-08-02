use crate::syn::error::{SyntaxError, bail, syntax_error};
use crate::syn::lexer::Lexer;
use crate::syn::token::{Token, t};
use crate::val::Regex;

pub fn regex(lexer: &mut Lexer, start: Token) -> Result<Regex, SyntaxError> {
	assert_eq!(start.kind, t!("/"), "Invalid start token of regex compound");
	lexer.scratch.clear();

	loop {
		match lexer.reader.next() {
			Some(b'\\') => {
				// We can't just eat all bytes after a \ because a byte might be non-ascii.
				if lexer.eat(b'/') {
					lexer.scratch.push('/');
				} else {
					lexer.scratch.push('\\');
				}
			}
			Some(b'/') => break,
			Some(x) => {
				if !x.is_ascii() {
					match lexer.reader.complete_char(x) {
						Err(e) => {
							let span = lexer.current_span();
							bail!("Invalid token: {e}", @span);
						}
						Ok(x) => {
							lexer.scratch.push(x);
						}
					}
				} else {
					lexer.scratch.push(x as char);
				}
			}
			None => {
				let span = lexer.current_span();
				bail!("Failed to lex regex, unexpected eof", @span);
			}
		}
	}

	let span = lexer.current_span();
	let regex = lexer.scratch.parse().map_err(|e| syntax_error!("Invalid regex: {e}", @span))?;
	lexer.scratch.clear();
	Ok(regex)
}
