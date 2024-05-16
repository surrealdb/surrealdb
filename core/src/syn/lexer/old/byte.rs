use crate::syn::{
	lexer::{
		unicode::{byte, chars},
		Error, Lexer,
	},
	token::{t, Token, TokenKind},
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
		self.set_whitespace_span(self.current_span());
		self.skip_offset();
	}

	/// Eats a multi line comment and returns an error if `*/` would be missing.
	pub fn eat_multi_line_comment(&mut self) -> Result<(), Error> {
		loop {
			let Some(byte) = self.reader.next() else {
				return Err(Error::UnexpectedEof);
			};
			if let b'*' = byte {
				let Some(byte) = self.reader.peek() else {
					return Err(Error::UnexpectedEof);
				};
				if b'/' == byte {
					self.reader.next();
					self.set_whitespace_span(self.current_span());
					self.skip_offset();
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
		self.set_whitespace_span(self.current_span());
		self.skip_offset();
	}

	// re-lexes a `/` token to a regex token.
	pub fn relex_regex(&mut self, token: Token) -> Token {
		debug_assert_eq!(token.kind, t!("/"));
		debug_assert_eq!(token.span.offset + 1, self.last_offset);
		debug_assert_eq!(token.span.len, 1);
		debug_assert_eq!(self.scratch, "");

		self.last_offset = token.span.offset;
		loop {
			match self.reader.next() {
				Some(b'\\') => {
					if let Some(b'/') = self.reader.peek() {
						self.reader.next();
						self.scratch.push('/')
					} else {
						self.scratch.push('\\')
					}
				}
				Some(b'/') => break,
				Some(x) => {
					if x.is_ascii() {
						self.scratch.push(x as char);
					} else {
						match self.reader.complete_char(x) {
							Ok(x) => {
								self.scratch.push(x);
							}
							Err(e) => return self.invalid_token(e.into()),
						}
					}
				}
				None => return self.invalid_token(Error::UnexpectedEof),
			}
		}

		match self.scratch.parse() {
			Ok(x) => {
				self.scratch.clear();
				self.regex = Some(x);
				self.finish_token(TokenKind::Regex)
			}
			Err(e) => self.invalid_token(Error::Regex(e)),
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
			b'@' => t!("@"),
			byte::CR | byte::FF | byte::LF | byte::SP | byte::VT | byte::TAB => {
				self.eat_whitespace();
				return self.next_token_inner();
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
				_ => return self.invalid_token(Error::ExpectedEnd('&')),
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
					return self.next_token_inner();
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
						_ => return self.invalid_token(Error::ExpectedEnd('=')),
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
					return self.next_token_inner();
				}
				Some(b'/') => {
					self.reader.next();
					self.eat_single_line_comment();
					return self.next_token_inner();
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
			b'$' => {
				if self.reader.peek().map(|x| x.is_ascii_alphabetic()).unwrap_or(false) {
					return self.lex_param();
				}
				t!("$")
			}
			b'#' => {
				self.eat_single_line_comment();
				return self.next_token_inner();
			}
			b'`' => return self.lex_surrounded_ident(true),
			b'"' => return self.lex_strand(true),
			b'\'' => return self.lex_strand(false),
			b'd' => {
				match self.reader.peek() {
					Some(b'"') => {
						self.reader.next();
						return self.lex_datetime(true);
					}
					Some(b'\'') => {
						self.reader.next();
						return self.lex_datetime(false);
					}
					_ => {}
				}
				return self.lex_ident_from_next_byte(b'd');
			}
			b'u' => {
				match self.reader.peek() {
					Some(b'"') => {
						self.reader.next();
						return self.lex_uuid(true);
					}
					Some(b'\'') => {
						self.reader.next();
						return self.lex_uuid(false);
					}
					_ => {}
				}
				return self.lex_ident_from_next_byte(b'u');
			}
			b'e' => {}
			b'r' => match self.reader.peek() {
				Some(b'\"') => {
					self.reader.next();
					t!("r\"")
				}
				Some(b'\'') => {
					self.reader.next();
					t!("r'")
				}
				_ => return self.lex_ident_from_next_byte(byte),
			},
			b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
				return self.lex_ident_from_next_byte(byte);
			}
			b'0'..=b'9' => return self.lex_number(byte),
			x => return self.invalid_token(Error::UnexpectedCharacter(x as char)),
		};

		self.finish_token(kind)
	}
}
