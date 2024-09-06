use std::mem;

use crate::syn::{
	error::{bail, error, SyntaxError},
	lexer::{unicode::chars, Lexer},
	token::{t, Token},
};

pub fn strand(lexer: &mut Lexer, start: Token) -> Result<String, SyntaxError> {
	let is_double = match start.kind {
		t!("\"") => true,
		t!("'") => false,
		_ => panic!("Invalid start of strand compound token"),
	};

	loop {
		let Some(x) = lexer.reader.next() else {
			lexer.scratch.clear();
			let err =
				error!("Unexpected end of file, expected strand to end",@lexer.current_span());
			return Err(err.with_data_pending());
		};

		if x.is_ascii() {
			match x {
				b'\'' if !is_double => {
					let res = mem::take(&mut lexer.scratch);
					return Ok(res);
				}
				b'"' if is_double => {
					let res = mem::take(&mut lexer.scratch);
					return Ok(res);
				}
				b'\0' => {
					bail!("Invalid null byte in source, null bytes are not valid SurrealQL characters",@lexer.current_span());
				}
				b'\\' => {
					// Handle escape sequences.
					let Some(next) = lexer.reader.next() else {
						lexer.scratch.clear();
						let err = error!("Unexpected end of file, expected strand to end",@lexer.current_span());
						return Err(err.with_data_pending());
					};
					match next {
						b'\\' => {
							lexer.scratch.push('\\');
						}
						b'\'' if !is_double => {
							lexer.scratch.push('\'');
						}
						b'\"' if is_double => {
							lexer.scratch.push('\"');
						}
						b'/' => {
							lexer.scratch.push('/');
						}
						b'b' => {
							lexer.scratch.push(chars::BS);
						}
						b'f' => {
							lexer.scratch.push(chars::FF);
						}
						b'n' => {
							lexer.scratch.push(chars::LF);
						}
						b'r' => {
							lexer.scratch.push(chars::CR);
						}
						b't' => {
							lexer.scratch.push(chars::TAB);
						}
						x => match lexer.reader.convert_to_char(x) {
							Ok(char) => {
								let valid_escape = if is_double {
									'"'
								} else {
									'\''
								};
								bail!("Invalid escape character `{char}`, valid characters are `\\`, `{valid_escape}`, `/`, `b`, `f`, `n`, `r`, or `t`", @lexer.current_span());
							}
							Err(e) => {
								return Err(e.into());
							}
						},
					}
				}
				x => lexer.scratch.push(x as char),
			}
		} else {
			match lexer.reader.complete_char(x) {
				Ok(x) => lexer.scratch.push(x),
				Err(e) => return Err(e.into()),
			}
		}
	}
}
