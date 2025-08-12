use std::mem;

use unicase::UniCase;

use super::unicode::{chars, is_identifier_continue};
use crate::syn::error::{SyntaxError, syntax_error};
use crate::syn::lexer::Lexer;
use crate::syn::lexer::keywords::KEYWORDS;
use crate::syn::token::{Token, TokenKind};

impl Lexer<'_> {
	/// Lex a parameter in the form of `$[a-zA-Z0-9_]*`
	///
	/// # Lexer State
	/// Expected the lexer to have already eaten the param starting `$`
	pub(super) fn lex_param(&mut self) -> Token {
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

	pub(super) fn lex_surrounded_param(&mut self, is_backtick: bool) -> Token {
		debug_assert_eq!(self.scratch, "");
		match self.lex_surrounded_ident_err(is_backtick) {
			Ok(_) => self.finish_token(TokenKind::Parameter),
			Err(e) => {
				self.scratch.clear();
				self.invalid_token(e)
			}
		}
	}

	/// Lex an not surrounded identifier in the form of `[a-zA-Z0-9_]*`
	///
	/// The start byte should already a valid byte of the identifier.
	///
	/// When calling the caller should already know that the token can't be any
	/// other token covered by `[a-zA-Z0-9_]*`.
	pub(super) fn lex_ident_from_next_byte(&mut self, start: u8) -> Token {
		debug_assert!(matches!(start, b'a'..=b'z' | b'A'..=b'Z' | b'_'));
		self.scratch.push(start as char);
		self.lex_ident()
	}

	/// Lex a not surrounded identfier.
	///
	/// The scratch should contain only identifier valid chars.
	pub(super) fn lex_ident(&mut self) -> Token {
		loop {
			if let Some(x) = self.reader.peek() {
				if is_identifier_continue(x) {
					self.scratch.push(x as char);
					self.reader.next();
					continue;
				}
			}
			// When finished parsing the identifier, try to match it to an keyword.
			// If there is one, return it as the keyword. Original identifier can be
			// reconstructed from the token.
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
	pub(super) fn lex_surrounded_ident(&mut self, is_backtick: bool) -> Token {
		match self.lex_surrounded_ident_err(is_backtick) {
			Ok(_) => self.finish_token(TokenKind::Identifier),
			Err(e) => {
				self.scratch.clear();
				self.invalid_token(e)
			}
		}
	}

	/// Lex an ident surrounded either by `⟨⟩` or `\`\``
	pub(super) fn lex_surrounded_ident_err(
		&mut self,
		is_backtick: bool,
	) -> Result<(), SyntaxError> {
		loop {
			let Some(x) = self.reader.next() else {
				let end_char = if is_backtick {
					'`'
				} else {
					'⟩'
				};
				let error = syntax_error!("Unexpected end of file, expected identifier to end with `{end_char}`", @self.current_span());
				return Err(error.with_data_pending());
			};
			if x.is_ascii() {
				match x {
					b'`' if is_backtick => {
						self.string = Some(mem::take(&mut self.scratch));
						return Ok(());
					}
					b'\0' => {
						// null bytes not allowed
						let err = syntax_error!("Invalid null byte in source, null bytes are not valid SurrealQL characters",@self.current_span());
						return Err(err);
					}
					b'\\' => {
						// handle escape sequences.
						let Some(next) = self.reader.next() else {
							let end_char = if is_backtick {
								'`'
							} else {
								'⟩'
							};
							let error = syntax_error!("Unexpected end of file, expected identifier to end with `{end_char}`", @self.current_span());
							return Err(error.with_data_pending());
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
							next => {
								let char = self.reader.convert_to_char(next)?;
								if !is_backtick && char == '⟩' {
									self.scratch.push(char);
								} else {
									let error = if !is_backtick {
										syntax_error!("Invalid escape character `{x}` for identifier, valid characters are `\\`, `⟩`, `/`, `b`, `f`, `n`, `r`, or `t`", @self.current_span())
									} else {
										syntax_error!("Invalid escape character `{x}` for identifier, valid characters are `\\`, ```, `/`, `b`, `f`, `n`, `r`, or `t`", @self.current_span())
									};
									return Err(error);
								}
							}
						}
					}
					x => self.scratch.push(x as char),
				}
			} else {
				let c = self.reader.complete_char(x)?;
				if !is_backtick && c == '⟩' {
					self.string = Some(mem::take(&mut self.scratch));
					return Ok(());
				}
				self.scratch.push(c);
			}
		}
	}
}
