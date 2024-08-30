use crate::syn::{
	error::{bail, error, SyntaxError},
	lexer::{
		unicode::{byte, chars},
		Lexer,
	},
	token::{t, DatetimeChars, Token, TokenKind},
};

impl<'a> Lexer<'a> {
	/// Eats a single line comment.
	pub fn eat_single_line_comment(&mut self) {
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

	/// Eats a multi line comment and returns an error if `*/` would be missing.
	pub fn eat_multi_line_comment(&mut self) -> Result<(), SyntaxError> {
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
	pub fn eat_whitespace(&mut self) {
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
			b'~' => t!("~"),
			b'@' => t!("@"),
			byte::CR | byte::FF | byte::LF | byte::SP | byte::VT | byte::TAB => {
				self.eat_whitespace();
				TokenKind::WhiteSpace
			}
			b'|' => match self.reader.peek() {
				Some(b'|') => {
					self.reader.next();
					t!("||")
				}
				Some(b'>') => {
					self.reader.next();
					t!("|>")
				}
				_ => t!("|"),
			},
			b'&' => match self.reader.peek() {
				Some(b'&') => {
					self.reader.next();
					t!("&&")
				}
				_ => {
					let error = error!("Invalid token `&`, single `&` are not a valid token, did you mean `&&`?",@self.current_span());
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
				Some(b'|') => {
					self.reader.next();
					t!("<|")
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
					self.eat_single_line_comment();
					TokenKind::WhiteSpace
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
							let error = error!("Invalid token `+?` did you maybe mean `+?=` ?", @self.current_span());
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
					if let Err(e) = self.eat_multi_line_comment()? {
						return self.invalid_token(e);
					}
					TokenKind::WhiteSpace
				}
				Some(b'/') => {
					self.reader.next();
					self.eat_single_line_comment();
					TokenKind::WhiteSpace
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
						Err(e) => return e.into(),
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
				TokenKind::WhiteSpace
			}
			b'`' => return self.lex_surrounded_ident(true),
			b'"' => t!("\""),
			b'\'' => t!("'"),
			b'd' => match self.reader.peek() {
				Some(b'"') => {
					self.reader.next();
					t!("d\"")
				}
				Some(b'\'') => {
					self.reader.next();
					t!("d'")
				}
				Some(b'e') => {
					self.reader.next();

					let Some(b'c') = self.reader.peek() else {
						self.scratch.push('d');
						return self.lex_ident_from_next_byte(b'e');
					};

					self.reader.next();

					if self.reader.peek().map(|x| x.is_ascii_alphanumeric()).unwrap_or(false) {
						self.scratch.push('d');
						self.scratch.push('e');
						return self.lex_ident_from_next_byte(b'c');
					}

					t!("dec")
				}
				Some(x) if !x.is_ascii_alphabetic() => {
					t!("d")
				}
				None => {
					t!("d")
				}
				_ => {
					return self.lex_ident_from_next_byte(b'd');
				}
			},
			b'f' => match self.reader.peek() {
				Some(x) if !x.is_ascii_alphanumeric() => {
					t!("f")
				}
				None => t!("f"),
				_ => {
					return self.lex_ident_from_next_byte(b'f');
				}
			},
			b'n' => match self.reader.peek() {
				Some(b's') => {
					self.reader.next();
					if self.reader.peek().map(|x| x.is_ascii_alphabetic()).unwrap_or(false) {
						self.scratch.push('n');
						return self.lex_ident_from_next_byte(b's');
					}
					t!("ns")
				}
				_ => {
					return self.lex_ident_from_next_byte(b'n');
				}
			},
			b'm' => match self.reader.peek() {
				Some(b's') => {
					self.reader.next();
					if self.reader.peek().map(|x| x.is_ascii_alphabetic()).unwrap_or(false) {
						self.scratch.push('m');
						return self.lex_ident_from_next_byte(b's');
					}
					t!("ms")
				}
				Some(x) if !x.is_ascii_alphabetic() => {
					t!("m")
				}
				None => {
					t!("m")
				}
				_ => {
					return self.lex_ident_from_next_byte(b'm');
				}
			},
			b's' => match self.reader.peek() {
				Some(b'"') => {
					self.reader.next();
					t!("\"")
				}
				Some(b'\'') => {
					self.reader.next();
					t!("'")
				}
				Some(x) if x.is_ascii_alphabetic() => {
					return self.lex_ident_from_next_byte(b's');
				}
				_ => t!("s"),
			},
			b'h' => {
				if self.reader.peek().map(|x| x.is_ascii_alphabetic()).unwrap_or(false) {
					return self.lex_ident_from_next_byte(b'h');
				} else {
					t!("h")
				}
			}
			b'w' => {
				if self.reader.peek().map(|x| x.is_ascii_alphabetic()).unwrap_or(false) {
					return self.lex_ident_from_next_byte(b'w');
				} else {
					t!("w")
				}
			}
			b'y' => {
				if self.reader.peek().map(|x| x.is_ascii_alphabetic()).unwrap_or(false) {
					return self.lex_ident_from_next_byte(b'y');
				} else {
					t!("y")
				}
			}
			b'u' => match self.reader.peek() {
				Some(b'"') => {
					self.reader.next();
					t!("u\"")
				}
				Some(b'\'') => {
					self.reader.next();
					t!("u'")
				}
				Some(b's') => {
					self.reader.next();
					if self.reader.peek().map(|x| x.is_ascii_alphabetic()).unwrap_or(false) {
						self.scratch.push('u');
						return self.lex_ident_from_next_byte(b's');
					}
					t!("us")
				}
				_ => {
					return self.lex_ident_from_next_byte(b'u');
				}
			},
			b'r' => match self.reader.peek() {
				Some(b'"') => {
					self.reader.next();
					t!("r\"")
				}
				Some(b'\'') => {
					self.reader.next();
					t!("r'")
				}
				_ => {
					return self.lex_ident_from_next_byte(b'r');
				}
			},
			b'Z' => match self.reader.peek() {
				Some(x) if x.is_ascii_alphabetic() => {
					return self.lex_ident_from_next_byte(b'Z');
				}
				_ => TokenKind::DatetimeChars(DatetimeChars::Z),
			},
			b'T' => match self.reader.peek() {
				Some(x) if x.is_ascii_alphabetic() => {
					return self.lex_ident_from_next_byte(b'T');
				}
				_ => TokenKind::DatetimeChars(DatetimeChars::T),
			},
			b'e' => {
				return self.lex_exponent(b'e');
			}
			b'E' => {
				return self.lex_exponent(b'E');
			}
			b'0'..=b'9' => return self.lex_digits(),
			b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
				return self.lex_ident_from_next_byte(byte);
			}
			//b'0'..=b'9' => return self.lex_number(byte),
			x => {
				let err = error!("Invalid token `{}`", x as char, @self.current_span());
				return self.invalid_token(err);
			}
		};

		self.finish_token(kind)
	}
}
