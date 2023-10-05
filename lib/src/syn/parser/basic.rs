use crate::{
	sql::{Duration, Ident, Number, Param, Strand},
	syn::{
		parser::mac::{to_do, unexpected},
		token::TokenKind,
	},
};

use super::{ParseResult, Parser};

impl Parser<'_> {
	pub fn parse_ident(&mut self) -> ParseResult<Ident> {
		self.parse_raw_ident().map(Ident)
	}

	pub fn parse_raw_ident(&mut self) -> ParseResult<String> {
		let token = self.next_token();
		match token.kind {
			TokenKind::Keyword(_)
			| TokenKind::Number
			| TokenKind::Duration {
				valid_identifier: true,
			} => {
				let str = self.lexer.reader.span(token.span);
				// Lexer should ensure that the token is valid utf-8
				let str = std::str::from_utf8(str).unwrap().to_owned();
				Ok(str)
			}
			TokenKind::Identifier => {
				let data_index = token.data_index.unwrap();
				let idx = u32::from(data_index) as usize;
				let str = self.lexer.strings[idx].clone();
				Ok(str)
			}
			x => {
				unexpected!(self, x, "a identifier");
			}
		}
	}

	pub(super) fn parse_u64(&mut self) -> ParseResult<u64> {
		to_do!(self)
	}

	pub(super) fn parse_number(&mut self) -> ParseResult<Number> {
		to_do!(self)
	}

	pub(super) fn parse_param(&mut self) -> ParseResult<Param> {
		to_do!(self)
	}

	pub(super) fn parse_duration(&mut self) -> ParseResult<Duration> {
		to_do!(self)
	}

	pub(super) fn parse_strand(&mut self) -> ParseResult<Strand> {
		to_do!(self)
	}
}
