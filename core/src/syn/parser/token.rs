//! Implements token gluing logic.

use bytes::buf;

use crate::syn::{
	parser::Parser,
	token::{t, DurationSuffix, NumberSuffix, Span, Token, TokenKind},
};

use super::{mac::unexpected, ParseResult};

impl Parser<'_> {
	pub fn tokenkind_can_start_ident(t: TokenKind) -> bool {
		matches!(
			t,
			TokenKind::Keyword(_)
				| TokenKind::Language(_)
				| TokenKind::Algorithm(_)
				| TokenKind::Distance(_)
				| TokenKind::VectorType(_)
				| TokenKind::Identifier
				| TokenKind::NumberSuffix(_)
				| TokenKind::DurationSuffix(
					DurationSuffix::Nano
						| DurationSuffix::Micro | DurationSuffix::Milli
						| DurationSuffix::Second | DurationSuffix::Minute
						| DurationSuffix::Hour | DurationSuffix::Day
						| DurationSuffix::Week
				)
		)
	}

	pub fn tokenkind_continues_ident(t: TokenKind) -> bool {
		matches!(
			t,
			TokenKind::Keyword(_)
				| TokenKind::Language(_)
				| TokenKind::Algorithm(_)
				| TokenKind::Distance(_)
				| TokenKind::VectorType(_)
				| TokenKind::Identifier
				| TokenKind::NumberSuffix(_)
				| TokenKind::NaN | TokenKind::DurationSuffix(
				DurationSuffix::Nano
					| DurationSuffix::Micro
					| DurationSuffix::Milli
					| DurationSuffix::Second
					| DurationSuffix::Minute
					| DurationSuffix::Hour
					| DurationSuffix::Day
					| DurationSuffix::Week
			)
		)
	}

	/// Returns if the peeked token can be a identifier.
	pub fn peek_can_start_ident(&mut self) -> bool {
		Self::tokenkind_can_start_ident(self.peek_kind())
	}

	/// Returns if the peeked token can be a identifier.
	pub fn peek_continues_ident(&mut self) -> bool {
		Self::tokenkind_can_start_ident(self.peek_kind())
	}

	/// Glues all next tokens follow eachother, which can make up an ident into a single string.
	pub fn glue_ident(
		&mut self,
		start_token: Token,
		mut start_buffer: &mut String,
	) -> Result<(), Span> {
		let mut cur = start_token;
		loop {
			let p = self.peek();
			if !p.follows_from(&cur) {
				return Ok(());
			}

			match p.kind {
				/// These token_kinds always complete an ident, no more identifier parts can happen
				/// after this.
				TokenKind::Identifier => {
					self.pop_peek();
					let buffer = self.lexer.string.take().unwrap();
					start_buffer.push_str(&buffer);

					return Ok(());
				}
				TokenKind::Keyword(_)
				| TokenKind::Language(_)
				| TokenKind::Algorithm(_)
				| TokenKind::Distance(_)
				| TokenKind::VectorType(_)
				| TokenKind::NumberSuffix(_) => {
					self.pop_peek();
					let str = self.lexer.reader.span(p.span);
					// Lexer should ensure that the token is valid utf-8
					let str = std::str::from_utf8(str).unwrap();
					start_buffer.push_str(str);

					return Ok(());
				}
				/// These tokens might have some more parts following them
				TokenKind::Exponent => {
					self.pop_peek();
					let str = self.lexer.reader.span(p.span);
					// Lexer should ensure that the token is valid utf-8
					let str = std::str::from_utf8(str).unwrap();

					start_buffer.push_str(str);
				}
				TokenKind::DurationSuffix(suffix) => {
					self.pop_peek();
					if !suffix.can_be_ident() {
						return Err(p.span);
					}
					start_buffer.push_str(suffix.as_str())
				}
				TokenKind::Digits => {
					self.pop_peek();
					let str = self.lexer.reader.span(p.span);
					// Lexer should ensure that the token is valid utf-8
					let str = std::str::from_utf8(str).unwrap();
					start_buffer.push_str(str)
				}
				_ => return Ok(()),
			}
		}
	}

	/// Glues the next tokens which would make up a float together into a single buffer.
	/// Return err if the tokens would return a invalid float.
	pub fn glue_float(&mut self, start: Token, mut buffer: &mut String) -> Result<(), Span> {
		let mut p = self.peek();
		if !p.follows_from(&start) {
			return Ok(buffer.parse()?);
		}

		/// Check for mantissa
		match p.kind {
			TokenKind::NumberSuffix(NumberSuffix::Float) => {
				self.pop_peek();

				return Ok(());
			}
			t!(".") => {
				self.pop_peek();

				let digits_token = self.peek();
				if !digits_token.follows_from(&p) || !matches!(digits_token.kind, TokenKind::Digits)
				{
					let span = start.span.covers(p.span);

					return Err(span);
				}

				let span = self.lexer.reader.span(digits_token.span);
				buffer.push('.');
				// filter out all the '_'
				buffer.extend(span.iter().filter(|x| x != b'_').map(|x| x as char));

				p = self.peek();
				if !p.follows_from(&digits_token) {
					return Ok(());
				}
			}
			TokenKind::Exponent => {}
			x => {
				if Parser::tokenkind_continues_ident(x) {
					return Err(p.span);
				} else {
					return Ok(());
				}
			}
		}

		/// Check for exponent
		match p.kind {
			TokenKind::Exponent => {
				self.pop_peek();
				let span = self.lexer.reader.span(p.span);
				// filter out all the '_'
				buffer.extend(span.iter().filter(|x| x != b'_').map(|x| x as char));
			}
			_ => {}
		}

		/// ensure that we don't have an invalid number due to identifiers following the number i.e.
		/// 123.456e789foo
		let new_p = self.peek();
		if !new_p.follows_from(&p) {
			return Ok(());
		}

		if let TokenKind::NumberSuffix(NumberSuffix::Float) = new_p.kind {
			self.pop_peek();
			return Ok(());
		}

		if !Parser::tokenkind_continues_ident(new_p.kind) {
			return Ok(());
		}

		return Err(new_p.span);
	}
}
