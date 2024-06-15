use std::mem;

use unicase::UniCase;

use crate::syn::{
	lexer::{keywords::KEYWORDS, Error, Lexer},
	token::{Token, TokenKind},
};

use super::unicode::chars;

fn is_identifier_continue(x: u8) -> bool {
	matches!(x, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_')
}

impl<'a> Lexer<'a> {
	/// Lex a parameter in the form of `$[a-zA-Z0-9_]*`
	///
	/// # Lexer State
	/// Expected the lexer to have already eaten the param starting `$`
	pub fn lex_param(&mut self) -> Token {
		debug_assert_eq!(self.scratch, "");
		loop {
			if let Some(x) = self.reader.peek() {
				if x.is_ascii_alphanumeric() || x == b'_' {
					self.scratch.push(x as char);
					self.reader.next();
					continue;
				}
			}
			self.string = Some(mem::take(&mut self.scratch));
			return self.finish_token(TokenKind::Parameter);
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
				if is_identifier_continue(x) {
					self.scratch.push(x as char);
					self.reader.next();
					continue;
				}
			}
			// When finished parsing the identifier, try to match it to an keyword.
			// If there is one, return it as the keyword. Original identifier can be reconstructed
			// from the token.
			if let Some(x) = KEYWORDS.get(&UniCase::ascii(&self.scratch)).copied() {
				if x != TokenKind::Identifier {
					self.scratch.clear();
					return self.finish_token(x);
				}
			}

			if self.scratch == "NaN" {
				self.scratch.clear();
				return self.finish_token(TokenKind::NaN);
			} else {
				self.string = Some(mem::take(&mut self.scratch));
				return self.finish_token(TokenKind::Identifier);
			}
		}
	}

	/// Lex an ident which is surround by delimiters.
	pub fn lex_surrounded_ident(&mut self, is_backtick: bool) -> Token {
		match self.lex_surrounded_ident_err(is_backtick) {
			Ok(x) => x,
			Err(e) => {
				self.scratch.clear();
				self.invalid_token(e)
			}
		}
	}

	/// Lex an ident surrounded either by `⟨⟩` or `\`\``
	pub fn lex_surrounded_ident_err(&mut self, is_backtick: bool) -> Result<Token, Error> {
		loop {
			let Some(x) = self.reader.next() else {
				let end_char = if is_backtick {
					'`'
				} else {
					'⟩'
				};
				return Err(Error::ExpectedEnd(end_char));
			};
			if x.is_ascii() {
				match x {
					b'`' if is_backtick => {
						self.string = Some(mem::take(&mut self.scratch));
						return Ok(self.finish_token(TokenKind::Identifier));
					}
					b'\0' => {
						// null bytes not allowed
						return Err(Error::UnexpectedCharacter('\0'));
					}
					b'\\' if is_backtick => {
						// handle escape sequences.
						// This is compliant with the orignal parser which didn't permit
						// escape sequences in `⟨⟩` surrounded idents.
						let Some(next) = self.reader.next() else {
							let end_char = if is_backtick {
								'`'
							} else {
								'⟩'
							};
							return Err(Error::ExpectedEnd(end_char));
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
								let char = if x.is_ascii() {
									x as char
								} else {
									self.reader.complete_char(x)?
								};
								return Err(Error::InvalidEscapeCharacter(char));
							}
						}
					}
					x => self.scratch.push(x as char),
				}
			} else {
				let c = self.reader.complete_char(x)?;
				if !is_backtick && c == '⟩' {
					self.string = Some(mem::take(&mut self.scratch));
					return Ok(self.finish_token(TokenKind::Identifier));
				}
				self.scratch.push(c);
			}
		}
	}
}
