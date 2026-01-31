use crate::syn::error::{SyntaxError, bail, syntax_error};
use crate::syn::lexer::Lexer;
use crate::syn::lexer::unicode::{byte, chars};
use crate::syn::token::{Token, TokenKind, t};

impl Lexer<'_> {
	/// Eats a single line comment.
	pub(super) fn eat_single_line_comment(&mut self) {
		loop {
			let Some(byte) = self.reader.next() else {
				break;
			};
			match byte {
				byte::CR => {
					self.eat(byte::LF);
					break;
				}
				byte::LF => {
					break;
				}
				x if !x.is_ascii() => {
					// -1 because we already ate the byte.
					let backup = self.reader.offset() - 1;
					let char = match self.reader.complete_char(x) {
						Ok(x) => x,
						Err(_) => {
							// let the next token handle the error.
							self.reader.backup(backup);
							break;
						}
					};

					match char {
						chars::LS | chars::PS | chars::NEL => break,
						_ => {}
					}
				}
				_ => {}
			}
		}
	}

	fn eat_string_like(&mut self, is_double: bool) -> Result<(), SyntaxError> {
		let start_span = self.current_span();
		loop {
			let Some(byte) = self.reader.next() else {
				bail!("Unexpected end of file, expected string to end.", @start_span => "String starting here.");
			};
			match byte {
				b'"' if is_double => break,
				b'\'' if !is_double => break,
				b'\\' => {
					// Don't bother interpreting the escape code, just skip it and continue
					// lexing.
					// Parser should validate escape codes.
					let Some(x) = self.reader.next() else {
						bail!("Unexpected end of file, expected string to end.", @start_span => "String starting here.");
					};
					if !x.is_ascii() {
						self.reader.complete_char(x)?;
					}
				}
				x => {
					if !x.is_ascii() {
						self.reader.complete_char(x)?;
					}
				}
			}
		}
		Ok(())
	}

	/// Eats a multi line comment and returns an error if `*/` would be missing.
	pub(super) fn eat_multi_line_comment(&mut self) -> Result<(), SyntaxError> {
		let start_span = self.current_span();
		loop {
			let Some(byte) = self.reader.next() else {
				bail!("Unexpected end of file, expected multi-line comment to end.", @start_span => "Comment starts here.");
			};
			if let b'*' = byte {
				let Some(byte) = self.reader.peek() else {
					bail!("Unexpected end of file, expected multi-line comment to end.", @start_span => "Comment starts here.");
				};
				if b'/' == byte {
					self.reader.next();
					return Ok(());
				}
			}
		}
	}

	/// Eat whitespace like spaces tables and new-lines.
	pub(super) fn eat_whitespace(&mut self) {
		loop {
			let Some(byte) = self.reader.peek() else {
				return;
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
						Err(_) => {
							self.reader.backup(backup);
							break;
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
	}

	/// Lex digits tokens
	pub(super) fn lex_digits(&mut self) -> Token {
		while let Some(b'0'..=b'9' | b'_') = self.reader.peek() {
			self.reader.next();
		}

		self.finish_token(TokenKind::Digits)
	}

	/// Lex the next token, starting from the given byte.
	pub(super) fn lex_ascii(&mut self, byte: u8) -> Token {
		let kind = match byte {
			b'{' => t!("{"),
			b'}' => t!("}"),
			b'[' => t!("["),
			b']' => t!("]"),
			b')' => t!(")"),
			b'(' => t!("("),
			b';' => t!(";"),
			b',' => t!(","),
			b'~' => t!("~"),
			b'@' => t!("@"),
			byte::CR | byte::FF | byte::LF | byte::SP | byte::VT | byte::TAB => {
				self.eat_whitespace();
				self.advance_span();
				return self.next_token();
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
				_ => {
					let error = syntax_error!("Invalid token `&`, single `&` are not a valid token, did you mean `&&`?",@self.current_span());
					return self.invalid_token(error);
				}
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
				Some(b'|') => {
					self.reader.next();
					t!("<|")
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
					self.eat_single_line_comment();
					self.advance_span();
					return self.next_token();
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
						_ => {
							let error = syntax_error!("Invalid token `+?` did you maybe mean `+?=` ?", @self.current_span());
							return self.invalid_token(error);
						}
					}
				}
				_ => t!("+"),
			},
			b'/' => match self.reader.peek() {
				Some(b'*') => {
					self.reader.next();
					// A `*/` could be missing which would be invalid.
					if let Err(e) = self.eat_multi_line_comment() {
						return self.invalid_token(e);
					}
					self.advance_span();
					return self.next_token();
				}
				Some(b'/') => {
					self.reader.next();
					self.eat_single_line_comment();
					self.advance_span();
					return self.next_token();
				}
				_ => t!("/"),
			},
			b'%' => t!("%"),
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
			b'$' => match self.reader.peek() {
				Some(b'_') => return self.lex_param(),
				Some(b'`') => {
					self.reader.next();
					return self.lex_surrounded_param(true);
				}
				Some(x) if x.is_ascii_alphabetic() => return self.lex_param(),
				Some(x) if !x.is_ascii() => {
					let backup = self.reader.offset();
					self.reader.next();
					match self.reader.complete_char(x) {
						Ok('⟨') => return self.lex_surrounded_param(false),
						Err(e) => return self.invalid_token(e.into()),
						_ => {
							self.reader.backup(backup);
							t!("$")
						}
					}
				}
				_ => t!("$"),
			},
			b'#' => {
				self.eat_single_line_comment();
				self.advance_span();
				return self.next_token();
			}
			b'`' => return self.lex_surrounded_ident(true),
			b'"' => {
				if let Err(e) = self.eat_string_like(true) {
					return self.invalid_token(e);
				}
				t!("\"")
			}
			b'\'' => {
				if let Err(e) = self.eat_string_like(false) {
					return self.invalid_token(e);
				}
				t!("'")
			}
			b'd' => match self.reader.peek() {
				Some(b'"') => {
					self.reader.next();
					if let Err(e) = self.eat_string_like(true) {
						return self.invalid_token(e);
					}
					t!("d\"")
				}
				Some(b'\'') => {
					self.reader.next();
					if let Err(e) = self.eat_string_like(false) {
						return self.invalid_token(e);
					}
					t!("d'")
				}
				_ => {
					return self.lex_ident_from_next_byte(b'd');
				}
			},
			b's' => match self.reader.peek() {
				Some(b'"') => {
					self.reader.next();
					if let Err(e) = self.eat_string_like(true) {
						return self.invalid_token(e);
					}
					t!("\"")
				}
				Some(b'\'') => {
					self.reader.next();
					if let Err(e) = self.eat_string_like(false) {
						return self.invalid_token(e);
					}
					t!("'")
				}
				_ => {
					return self.lex_ident_from_next_byte(b's');
				}
			},
			b'u' => match self.reader.peek() {
				Some(b'"') => {
					self.reader.next();
					if let Err(e) = self.eat_string_like(true) {
						return self.invalid_token(e);
					}
					t!("u\"")
				}
				Some(b'\'') => {
					self.reader.next();
					if let Err(e) = self.eat_string_like(false) {
						return self.invalid_token(e);
					}
					t!("u'")
				}
				_ => {
					return self.lex_ident_from_next_byte(b'u');
				}
			},
			b'b' => match self.reader.peek() {
				Some(b'"') => {
					self.reader.next();
					if let Err(e) = self.eat_string_like(true) {
						return self.invalid_token(e);
					}
					t!("b\"")
				}
				Some(b'\'') => {
					self.reader.next();
					if let Err(e) = self.eat_string_like(false) {
						return self.invalid_token(e);
					}
					t!("b'")
				}
				_ => {
					return self.lex_ident_from_next_byte(b'b');
				}
			},
			b'f' => match self.reader.peek() {
				Some(b'"') => {
					self.reader.next();
					if let Err(e) = self.eat_string_like(true) {
						return self.invalid_token(e);
					}
					t!("f\"")
				}
				Some(b'\'') => {
					self.reader.next();
					if let Err(e) = self.eat_string_like(false) {
						return self.invalid_token(e);
					}
					t!("f'")
				}
				_ => {
					return self.lex_ident_from_next_byte(b'f');
				}
			},
			b'r' => match self.reader.peek() {
				Some(b'"') => {
					self.reader.next();
					if let Err(e) = self.eat_string_like(true) {
						return self.invalid_token(e);
					}
					t!("r\"")
				}
				Some(b'\'') => {
					self.reader.next();
					if let Err(e) = self.eat_string_like(false) {
						return self.invalid_token(e);
					}
					t!("r'")
				}
				_ => {
					return self.lex_ident_from_next_byte(b'r');
				}
			},
			b'0'..=b'9' => return self.lex_digits(),
			b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
				return self.lex_ident_from_next_byte(byte);
			}
			x => {
				let err = syntax_error!("Invalid token `{}`", x as char, @self.current_span());
				return self.invalid_token(err);
			}
		};

		self.finish_token(kind)
	}
}
