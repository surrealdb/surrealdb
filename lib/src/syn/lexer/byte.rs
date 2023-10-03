use crate::syn::lexer::{
	unicode::{byte, chars},
	CharError, Lexer,
};
use crate::syn::token::{t, Token, TokenKind};

impl<'a> Lexer<'a> {
	/// Eats a single line comment and returns the next token.
	fn eat_single_line_comment(&mut self) -> Token {
		loop {
			let Some(byte) = self.reader.next() else {
				return self.eof_token();
			};
			match byte {
				byte::CR => {
					self.reader.peek();
					if let Some(byte::LF) = self.reader.peek() {
						self.reader.next();
					}
					break;
				}
				byte::LF => {
					break;
				}
				x if !x.is_ascii() => {
					let char = match self.reader.complete_char(x) {
						Ok(x) => x,
						Err(CharError::Eof) => return self.eof_token(),
						Err(CharError::Unicode) => {
							return self.finish_token(TokenKind::Invalid, None)
						}
					};

					match char {
						chars::LS | chars::PS => break,
						_ => {}
					}
				}
				_ => {}
			}
		}
		self.skip_offset();
		self.next_token()
	}

	/// Eats a multi line comment and returns the next token.
	fn eat_multi_line_comment(&mut self) -> Token {
		loop {
			let Some(byte) = self.reader.next() else {
				return self.eof_token();
			};
			if let b'*' = byte {
				let Some(byte) = self.reader.next() else {
					return self.eof_token();
				};
				if b'/' == byte {
					self.skip_offset();
					return self.next_token();
				}
			}
		}
	}

	/// Eats a whitespace and returns the next token.
	fn eat_whitespace(&mut self) -> Token {
		loop {
			let Some(byte) = self.reader.peek() else {
				return self.eof_token();
			};
			match byte {
				byte::CR | byte::FF | byte::LF | byte::SP | byte::VT | byte::TAB => {
					self.reader.next();
				}
				x if !x.is_ascii() => {
					let backup = self.reader.offset();
					self.reader.next();
					let char = match self.reader.complete_char(x) {
						Ok(x) => x,
						Err(CharError::Eof) => return self.eof_token(),
						Err(CharError::Unicode) => {
							return self.finish_token(TokenKind::Invalid, None)
						}
					};

					match char {
						'\u{00A0}' | '\u{1680}' | '\u{2000}' | '\u{2001}' | '\u{2002}'
						| '\u{2003}' | '\u{2004}' | '\u{2005}' | '\u{2006}' | '\u{2007}'
						| '\u{2008}' | '\u{2009}' | '\u{200A}' | '\u{202F}' | '\u{205F}'
						| '\u{3000}' => {}
						_ => {
							self.reader.backup(backup);
							break;
						}
					}
				}
				_ => break,
			}
		}
		self.skip_offset();
		self.next_token()
	}

	/// Lex the next token, starting from the given byte.
	pub fn lex_ascii(&mut self, byte: u8) -> Token {
		let kind = match byte {
			b'{' => t!("{"),
			b'}' => t!("}"),
			b'[' => t!("["),
			b']' => t!("]"),
			b')' => t!(")"),
			b'(' => t!("("),
			b';' => t!(";"),
			b',' => t!(","),
			byte::CR | byte::FF | byte::LF | byte::SP | byte::VT | byte::TAB => {
				return self.eat_whitespace()
			}
			b'|' => match self.reader.peek() {
				Some(b'|') => {
					self.reader.next();
					t!("||")
				}
				_ => t!("|"),
			},
			b'&' => match self.reader.peek() {
				Some(b'&') => {
					self.reader.next();
					t!("&&")
				}
				_ => TokenKind::Invalid,
			},
			b'.' => match self.reader.peek() {
				Some(b'.') => {
					self.reader.next();
					match self.reader.peek() {
						Some(b'.') => {
							self.reader.next();
							t!("...")
						}
						_ => t!(".."),
					}
				}
				_ => t!("."),
			},
			b'!' => match self.reader.peek() {
				Some(b'=') => {
					self.reader.next();
					t!("!=")
				}
				Some(b'~') => {
					self.reader.next();
					t!("!~")
				}
				_ => t!("!"),
			},
			b'?' => match self.reader.peek() {
				Some(b'?') => {
					self.reader.next();
					t!("??")
				}
				Some(b':') => {
					self.reader.next();
					t!("?:")
				}
				Some(b'~') => {
					self.reader.next();
					t!("?~")
				}
				Some(b'=') => {
					self.reader.next();
					t!("?=")
				}
				_ => t!("?"),
			},
			b'<' => match self.reader.peek() {
				Some(b'=') => {
					self.reader.next();
					t!("<=")
				}
				Some(b'-') => {
					self.reader.next();
					match self.reader.peek() {
						Some(b'>') => {
							self.reader.next();
							t!("<->")
						}
						_ => t!("<-"),
					}
				}
				_ => t!("<"),
			},
			b'>' => match self.reader.peek() {
				Some(b'=') => {
					self.reader.next();
					t!(">=")
				}
				_ => t!(">"),
			},
			b'-' => match self.reader.peek() {
				Some(b'>') => {
					self.reader.next();
					t!("->")
				}
				Some(b'-') => {
					self.reader.next();
					return self.eat_single_line_comment();
				}
				Some(b'=') => {
					self.reader.next();
					t!("-=")
				}
				_ => t!("-"),
			},
			b'+' => match self.reader.peek() {
				Some(b'=') => {
					self.reader.next();
					t!("+=")
				}
				Some(b'?') => {
					self.reader.next();
					match self.reader.peek() {
						Some(b'=') => {
							self.reader.next();
							t!("+?=")
						}
						_ => TokenKind::Invalid,
					}
				}
				_ => t!("+"),
			},
			b'/' => match self.reader.peek() {
				Some(b'*') => {
					self.reader.next();
					return self.eat_multi_line_comment();
				}
				Some(b'/') => {
					self.reader.next();
					return self.eat_single_line_comment();
				}
				_ => t!("/"),
			},
			b'*' => match self.reader.peek() {
				Some(b'*') => {
					self.reader.next();
					t!("**")
				}
				Some(b'=') => {
					self.reader.next();
					t!("*=")
				}
				Some(b'~') => {
					self.reader.next();
					t!("*~")
				}
				_ => t!("*"),
			},
			b'=' => match self.reader.peek() {
				Some(b'=') => {
					self.reader.next();
					t!("==")
				}
				_ => t!("="),
			},
			b':' => match self.reader.peek() {
				Some(b':') => {
					self.reader.next();
					t!("::")
				}
				_ => t!(":"),
			},
			b'$' => return self.lex_param(),
			b'#' => {
				return self.eat_single_line_comment();
			}
			b'`' => return self.lex_surrounded_ident(true),
			b'"' => return self.lex_strand(true),
			b'\'' => return self.lex_strand(false),
			b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
				return self.lex_ident_from_next_byte(byte);
			}
			b'0'..=b'9' => return self.lex_number(byte),
			_ => TokenKind::Invalid,
		};

		self.finish_token(kind, None)
	}
}
