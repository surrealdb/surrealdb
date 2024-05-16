//! Lexing of strand like characters.

use std::mem;

use crate::syn::token::{Token, TokenKind};

use super::{unicode::chars, Error, Lexer};

impl<'a> Lexer<'a> {
	/// Lex a plain strand with either single or double quotes.
	pub fn lex_strand(&mut self, is_double: bool) -> Token {
		match self.lex_strand_err(is_double) {
			Ok(x) => x,
			Err(x) => {
				self.scratch.clear();
				self.invalid_token(x)
			}
		}
	}

	/// Lex a strand with either double or single quotes but return an result instead of a token.
	pub fn lex_strand_err(&mut self, is_double: bool) -> Result<Token, Error> {
		loop {
			let Some(x) = self.reader.next() else {
				self.scratch.clear();
				return Ok(self.eof_token());
			};

			if x.is_ascii() {
				match x {
					b'\'' if !is_double => {
						self.string = Some(mem::take(&mut self.scratch));
						return Ok(self.finish_token(TokenKind::Strand));
					}
					b'"' if is_double => {
						self.string = Some(mem::take(&mut self.scratch));
						return Ok(self.finish_token(TokenKind::Strand));
					}
					b'\0' => {
						// null bytes not allowed
						return Err(Error::UnexpectedCharacter('\0'));
					}
					b'\\' => {
						// Handle escape sequences.
						let Some(next) = self.reader.next() else {
							self.scratch.clear();
							return Ok(self.eof_token());
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
							x => {
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
				self.scratch.push(c);
			}
		}
	}
}
