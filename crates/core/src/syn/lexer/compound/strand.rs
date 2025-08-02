use std::ops::RangeInclusive;
use std::{char, mem};

use crate::syn::error::{SyntaxError, bail, syntax_error};
use crate::syn::lexer::Lexer;
use crate::syn::lexer::unicode::chars;
use crate::syn::token::{Token, t};

pub fn strand(lexer: &mut Lexer, start: Token) -> Result<String, SyntaxError> {
	let is_double = match start.kind {
		t!("\"") => true,
		t!("'") => false,
		_ => panic!("Invalid start of strand compound token"),
	};

	loop {
		let Some(x) = lexer.reader.next() else {
			lexer.scratch.clear();
			let err = syntax_error!("Unexpected end of file, expected strand to end",@lexer.current_span());
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
						let err = syntax_error!("Unexpected end of file, expected strand to end",@lexer.current_span());
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
						b'u' => {
							let c = lex_unicode_sequence(lexer)?;
							lexer.scratch.push(c);
						}
						x => match lexer.reader.convert_to_char(x) {
							Ok(char) => {
								let valid_escape = if is_double {
									'"'
								} else {
									'\''
								};
								bail!("Invalid escape character `{char}`, valid characters are `\\`, `{valid_escape}`, `/`, `b`, `f`, `n`, `r`, `t`, or `u` ", @lexer.current_span());
							}
							Err(e) => {
								lexer.scratch.clear();
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
				Err(e) => {
					lexer.scratch.clear();
					return Err(e.into());
				}
			}
		}
	}
}

const LEADING_SURROGATES: RangeInclusive<u16> = 0xD800..=0xDBFF;
const TRAILING_SURROGATES: RangeInclusive<u16> = 0xDC00..=0xDFFF;

fn lex_unicode_sequence(lexer: &mut Lexer) -> Result<char, SyntaxError> {
	if let Some(b'{') = lexer.reader.peek() {
		lexer.reader.next();
		return lex_bracket_unicode_sequence(lexer);
	}

	let leading = lex_bare_unicode_sequence(lexer)?;
	if LEADING_SURROGATES.contains(&leading) {
		if !(lexer.reader.next() == Some(b'\\') && lexer.reader.next() == Some(b'u')) {
			bail!("Unicode escape sequence encoding a leading surrogate needs to be followed by a trailing surrogate", @lexer.current_span());
		}
		let trailing = lex_bare_unicode_sequence(lexer)?;
		// Compute the codepoint.
		// https://datacadamia.com/data/type/text/surrogate#from_surrogate_to_character_code
		let codepoint = 0x10000
			+ ((leading as u32 - *LEADING_SURROGATES.start() as u32) << 10)
			+ trailing as u32
			- *TRAILING_SURROGATES.start() as u32;

		return char::from_u32(codepoint).ok_or_else(|| {
			syntax_error!("Unicode escape sequences encode invalid character codepoint", @lexer.current_span())
		});
	}

	let c = char::from_u32(leading as u32)
		.ok_or_else(|| syntax_error!("Unicode escape sequences encode invalid character codepoint", @lexer.current_span()))?;

	if c == '\0' {
		return Err(syntax_error!("Null bytes are not allowed in strings",@lexer.current_span()));
	}

	Ok(c)
}

fn lex_bracket_unicode_sequence(lexer: &mut Lexer) -> Result<char, SyntaxError> {
	let mut accum = 0u32;
	for _ in 0..6 {
		let c = lexer.reader.peek().ok_or_else(
			|| syntax_error!("Unexpected end of file, expected strand to end", @lexer.current_span()),
		)?;

		match c {
			b'a'..=b'f' => {
				accum <<= 4;
				accum += (c - b'a') as u32 + 10;
			}
			b'A'..=b'F' => {
				accum <<= 4;
				accum += (c - b'A') as u32 + 10;
			}
			b'0'..=b'9' => {
				accum <<= 4;
				accum += (c - b'0') as u32;
			}
			_ => break,
		}
		lexer.reader.next();
	}

	let Some(b'}') = lexer.reader.next() else {
		bail!("Missing end brace `}}` of unicode escape sequence", @lexer.current_span())
	};

	let c = char::from_u32(accum)
		.ok_or_else(|| syntax_error!("Unicode escape sequences encode invalid character codepoint", @lexer.current_span()))?;

	if c == '\0' {
		return Err(syntax_error!("Null bytes are not allowed in strings",@lexer.current_span()));
	}

	Ok(c)
}

fn lex_bare_unicode_sequence(lexer: &mut Lexer) -> Result<u16, SyntaxError> {
	let mut accum: u16 = 0;
	for _ in 0..4 {
		let Some(c) = lexer.reader.next() else {
			bail!("Missing characters after unicode escape sequence", @lexer.current_span());
		};

		accum <<= 4;
		match c {
			b'a'..=b'f' => {
				accum += (c - b'a') as u16 + 10;
			}
			b'A'..=b'F' => {
				accum += (c - b'A') as u16 + 10;
			}
			b'0'..=b'9' => {
				accum += (c - b'0') as u16;
			}
			_ => {
				bail!("Invalid character `{}` in unicode escape sequence, must be a hex digit.",c as char, @lexer.current_span());
			}
		}
	}
	Ok(accum)
}
