use unicase::UniCase;

use crate::syn::lexer::{keywords::KEYWORDS, CharError, Lexer};
use crate::syn::token::{Token, TokenKind};

use super::unicode::{chars, U8Ext};

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
	/// The start byte should already a valid byte of the identifier.
	///
	/// When calling the caller should already know that the token can't be any other token covered
	/// by `[a-zA-Z0-9_]*`.
	pub fn lex_ident_from_next_byte(&mut self, start: u8) -> Token {
		debug_assert!(matches!(start, b'a'..=b'z' | b'A'..=b'Z' | b'_'));
		self.scratch.push(start as char);
		self.lex_ident()
	}

	/// Lex a not surrounded identfier.
	///
	/// The scratch should contain only identifier valid chars.
	pub fn lex_ident(&mut self) -> Token {
		loop {
			if let Some(x) = self.reader.peek() {
				if x.is_identifier_continue() {
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
			let Some(x) = self.reader.next() else {
				self.scratch.clear();
				return self.eof_token();
			};
			if x.is_ascii() {
				match x {
					b'`' if is_backtick => {
						return self.finish_string_token(TokenKind::Identifier);
					}
					b'\0' => {
						// null bytes not allowed
						self.scratch.clear();
						return self.invalid_token();
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
								self.scratch.push(chars::BS);
							}
							b'f' => {
								self.scratch.push(chars::FF);
							}
							b'n' => {
								self.scratch.push(chars::LF);
							}
							b'r' => {
								self.scratch.push(chars::CR);
							}
							b't' => {
								self.scratch.push(chars::TAB);
							}
							_ => {
								self.scratch.clear();
								return self.invalid_token();
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
						return self.invalid_token();
					}
				};
				if !is_backtick && c == '⟩' {
					return self.finish_string_token(TokenKind::Identifier);
				}
				self.scratch.push(c);
			}
		}
	}

	/// Lex a strand with either double or single quotes.
	pub fn lex_strand(&mut self, is_double: bool) -> Token {
		loop {
			let Some(x) = self.reader.next() else {
				self.scratch.clear();
				return self.eof_token();
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
						// null bytes not allowed
						self.scratch.clear();
						return self.invalid_token();
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
								self.scratch.push(chars::BS);
							}
							b'f' => {
								self.scratch.push(chars::FF);
							}
							b'n' => {
								self.scratch.push(chars::LF);
							}
							b'r' => {
								self.scratch.push(chars::CR);
							}
							b't' => {
								self.scratch.push(chars::TAB);
							}
							_ => {
								self.scratch.clear();
								return self.invalid_token();
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
						return self.invalid_token();
					}
				};
				self.scratch.push(c);
			}
		}
	}
}
