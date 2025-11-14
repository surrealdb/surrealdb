use unicase::UniCase;

use super::unicode::is_identifier_continue;
use crate::syn::error::{SyntaxError, bail, syntax_error};
use crate::syn::lexer::keywords::KEYWORDS;
use crate::syn::lexer::{BytesReader, Lexer};
use crate::syn::token::{Span, Token, TokenKind};

impl Lexer<'_> {
	pub fn unescape_ident_span<'a>(
		str: &'a str,
		span: Span,
		buffer: &'a mut Vec<u8>,
	) -> Result<&'a str, SyntaxError> {
		buffer.clear();
		let mut reader = BytesReader::new(str.as_bytes());
		match reader.next() {
			Some(b'`') => Self::unescape_backtick_span(reader, span, buffer),
			// This is an unescaped ident so there is no need to do anything.
			_ => Ok(str),
		}
	}

	fn unescape_backtick_span<'a>(
		mut reader: BytesReader,
		span: Span,
		buffer: &'a mut Vec<u8>,
	) -> Result<&'a str, SyntaxError> {
		loop {
			// lexer ensures that backtick tokens end with `.
			let before = reader.offset();
			let x = reader.next().expect("lexer validated input");
			match x {
				b'\\' => {
					// Lexer already ensures there is a valid character after the \
					Self::lex_common_escape_sequence(&mut reader, span, before, buffer)?;
				}
				b'`' => break,
				x => {
					buffer.push(x);
				}
			}
		}

		Ok(unsafe { std::str::from_utf8_unchecked(buffer) })
	}

	/// Lex a parameter in the form of `$[a-zA-Z0-9_]*`
	///
	/// # Lexer State
	/// Expected the lexer to have already eaten the param starting `$`
	pub(super) fn lex_param(&mut self) -> Token {
		loop {
			if let Some(x) = self.reader.peek() {
				if x.is_ascii_alphanumeric() || x == b'_' {
					self.reader.next();
					continue;
				}
			}
			return self.finish_token(TokenKind::Parameter);
		}
	}

	pub(super) fn lex_surrounded_param(&mut self) -> Token {
		match self.lex_surrounded_ident_err() {
			Ok(_) => self.finish_token(TokenKind::Parameter),
			Err(e) => self.invalid_token(e),
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
		self.lex_ident()
	}

	/// Lex a not surrounded identfier.
	///
	/// The scratch should contain only identifier valid chars.
	pub(super) fn lex_ident(&mut self) -> Token {
		loop {
			if let Some(x) = self.reader.peek() {
				if is_identifier_continue(x) {
					self.reader.next();
					continue;
				}
			}

			let str = self.span_str(self.current_span());

			// When finished parsing the identifier, try to match it to an keyword.
			// If there is one, return it as the keyword. Original identifier can be
			// reconstructed from the token.
			if let Some(x) = KEYWORDS.get(&UniCase::ascii(str)).copied() {
				if x != TokenKind::Identifier {
					return self.finish_token(x);
				}
			} else if str == "NaN" {
				return self.finish_token(TokenKind::NaN);
			} else if str == "Infinity" {
				return self.finish_token(TokenKind::Infinity);
			}

			return self.finish_token(TokenKind::Identifier);
		}
	}

	/// Lex an ident which is surround by delimiters.
	pub(super) fn lex_surrounded_ident(&mut self) -> Token {
		match self.lex_surrounded_ident_err() {
			Ok(_) => self.finish_token(TokenKind::Identifier),
			Err(e) => self.invalid_token(e),
		}
	}

	/// Lex an ident surrounded by ````.
	pub(super) fn lex_surrounded_ident_err(&mut self) -> Result<(), SyntaxError> {
		let start_span = self.current_span();
		loop {
			let Some(x) = self.reader.next() else {
				let end_char = '`';
				let error = syntax_error!("Unexpected end of file, expected identifier to end with `{end_char}`", @self.current_span());
				return Err(error);
			};
			match x {
				b'`' => {
					return Ok(());
				}
				b'\\' => {
					// Don't bother parsing escape sequences, just skip the next byte
					let Some(next) = self.reader.next() else {
						bail!("Unexpected end of file, expected identifier to end.", @start_span => "Identifier starting here.");
					};

					if !next.is_ascii() {
						self.reader.complete_char(next)?;
					}
				}
				x => {
					if !x.is_ascii() {
						self.reader.complete_char(x)?;
					}
				}
			}
		}
	}
}
