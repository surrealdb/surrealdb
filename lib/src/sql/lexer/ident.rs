use unicase::UniCase;

use crate::sql::lexer::{keywords::KEYWORDS, CharError, Lexer};
use crate::sql::token::{Token, TokenKind};

impl<'a> Lexer<'a> {
	/// Lex a parameter in the form of `$[a-zA-Z0-9_]*`
	pub fn lex_param(&mut self) -> Token {
		loop {
			if let Some(x) = self.reader.peek() {
				if x.is_ascii_alphanumeric() || x == b'_' {
					self.scratch.push(x as char);
					self.reader.next();
					continue;
				}
			}
			return self.finish_string_token(TokenKind::Parameter);
		}
	}

	/// Lex an not surrounded identifier in the form of `[a-zA-Z0-9_]*`
	///
	/// When calling the caller should already know that the token can't be a number..
	pub fn lex_ident(&mut self, start: u8) -> Token {
		if start.is_ascii() {
			self.scratch.push(start as char);
		} else {
			return self.finish_string_token(TokenKind::Identifier);
		}
		loop {
			if let Some(x) = self.reader.peek() {
				if x.is_ascii_alphanumeric() || x == b'_' {
					self.scratch.push(x as char);
					self.reader.next();
					continue;
				}
			}
			// When finished parsing the identifier, try to match it to an keyword.
			// If there is one, return it as the keyword. Original identifier can be reconstructed
			// from the token.
			if let Some(x) = KEYWORDS.get(&UniCase::ascii(&self.scratch)).copied() {
				return self.finish_string_token(x);
			}
			return self.finish_string_token(TokenKind::Identifier);
		}
	}

	/// Lex an ident surrounded either by `⟨⟩` or `\`\``
	pub fn lex_surrounded_ident(&mut self, is_backtick: bool) -> Token {
		loop {
			if let Some(x) = self.reader.next() {
				if x.is_ascii() {
					match x {
						b'`' if is_backtick => {
							return self.finish_string_token(TokenKind::Identifier);
						}
						b'\0' => {
							self.scratch.clear();
							return self.finish_token(TokenKind::Invalid, None);
						}
						b'\\' if is_backtick => {
							// handle escape sequences.
							// This is compliant with the orignal parser which didn't permit
							// escape sequences in `⟨⟩` surrounded idents.
							let Some(next) = self.reader.next() else {
								self.scratch.clear();
								return self.eof_token();
							};
							match next {
								b'\\' => {
									self.scratch.push('\\');
								}
								b'`' => {
									self.scratch.push('`');
								}
								b'/' => {
									self.scratch.push('/');
								}
								b'b' => {
									self.scratch.push('\u{08}');
								}
								b'f' => {
									self.scratch.push('\u{0c}');
								}
								b'n' => {
									self.scratch.push('\u{0a}');
								}
								b'r' => {
									self.scratch.push('\u{0d}');
								}
								b't' => {
									self.scratch.push('\u{09}');
								}
								_ => {
									self.scratch.clear();
									return self.finish_token(TokenKind::Invalid, None);
								}
							}
						}
						x => self.scratch.push(x as char),
					}
				} else {
					let c = match self.reader.complete_char(x) {
						Ok(x) => x,
						Err(CharError::Eof) => {
							self.scratch.clear();
							return self.eof_token();
						}
						Err(CharError::Unicode) => {
							self.scratch.clear();
							return self.finish_token(TokenKind::Invalid, None);
						}
					};
					if !is_backtick && c == '⟩' {
						return self.finish_string_token(TokenKind::Identifier);
					}
					self.scratch.push(c);
				}
			}
		}
	}

	/// Lex a strand with either double or single quotes.
	pub fn lex_strand(&mut self, is_double: bool) -> Token {
		loop {
			let Some(x) = self.reader.next() else {
				self.scratch.clear();
				return self.finish_token(TokenKind::Invalid, None);
			};

			if x.is_ascii() {
				match x {
					b'\'' if !is_double => {
						return self.finish_string_token(TokenKind::Strand);
					}
					b'"' if is_double => {
						return self.finish_string_token(TokenKind::Strand);
					}
					b'\0' => {
						self.scratch.clear();
						return self.finish_token(TokenKind::Invalid, None);
					}
					b'\\' => {
						// Handle escape sequences.
						let Some(next) = self.reader.next() else {
							self.scratch.clear();
							return self.eof_token();
						};
						match next {
							b'\\' => {
								self.scratch.push('\\');
							}
							b'\'' if !is_double => {
								self.scratch.push('\'');
							}
							b'\"' if is_double => {
								self.scratch.push('\"');
							}
							b'/' => {
								self.scratch.push('/');
							}
							b'b' => {
								self.scratch.push('\u{08}');
							}
							b'f' => {
								self.scratch.push('\u{0c}');
							}
							b'n' => {
								self.scratch.push('\u{0a}');
							}
							b'r' => {
								self.scratch.push('\u{0d}');
							}
							b't' => {
								self.scratch.push('\u{09}');
							}
							_ => {
								self.scratch.clear();
								return self.finish_token(TokenKind::Invalid, None);
							}
						}
					}
					x => self.scratch.push(x as char),
				}
			} else {
				let c = match self.reader.complete_char(x) {
					Ok(x) => x,
					Err(CharError::Eof) => {
						self.scratch.clear();
						return self.eof_token();
					}
					Err(CharError::Unicode) => {
						self.scratch.clear();
						return self.finish_token(TokenKind::Invalid, None);
					}
				};
				self.scratch.push(c);
			}
		}
	}
}
