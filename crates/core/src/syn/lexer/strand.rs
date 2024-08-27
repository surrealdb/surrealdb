//! Lexing of strand like characters.

use std::mem;

use crate::syn::token::{QouteKind, Token, TokenKind};

use super::{unicode::chars, Error, Lexer};

impl<'a> Lexer<'a> {
	/// Lex a plain strand with either single or double quotes.
	pub fn relex_strand(&mut self, token: Token) -> Token {
		let is_double = match token.kind {
			TokenKind::Qoute(QouteKind::Plain) => false,
			TokenKind::Qoute(QouteKind::PlainDouble) => true,
			x => panic!("invalid token kind, '{:?}' is not allowed for re-lexing strands", x),
		};

		self.last_offset = token.span.offset;

		loop {
			let Some(x) = self.reader.next() else {
				self.scratch.clear();
				return self.eof_token();
			};

			if x.is_ascii() {
				match x {
					b'\'' if !is_double => {
						self.string = Some(mem::take(&mut self.scratch));
						return self.finish_token(TokenKind::Strand);
					}
					b'"' if is_double => {
						self.string = Some(mem::take(&mut self.scratch));
						return self.finish_token(TokenKind::Strand);
					}
					b'\0' => {
						return self.invalid_token(Error::UnexpectedCharacter('\0'));
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
							x => {
								let char = if x.is_ascii() {
									x as char
								} else {
									match self.reader.complete_char(x) {
										Ok(x) => x,
										Err(e) => return self.invalid_token(e.into()),
									}
								};
								return self.invalid_token(Error::InvalidEscapeCharacter(char));
							}
						}
					}
					x => self.scratch.push(x as char),
				}
			} else {
				match self.reader.complete_char(x) {
					Ok(x) => self.scratch.push(x),
					Err(e) => return self.invalid_token(e.into()),
				}
			}
		}
	}
}
