use crate::{
	sql::{Dir, Duration, Ident, Number, Param, Strand},
	syn::{
		parser::mac::{to_do, unexpected},
		token::{t, TokenKind},
	},
};

use super::{ParseResult, Parser};

impl Parser<'_> {
	pub fn parse_ident(&mut self) -> ParseResult<Ident> {
		self.parse_raw_ident().map(Ident)
	}

	pub fn parse_dir(&mut self) -> ParseResult<Dir> {
		match self.next().kind {
			t!("<-") => Ok(Dir::In),
			t!("<->") => Ok(Dir::Both),
			t!("->") => Ok(Dir::Out),
			x => unexpected!(self, x, "a direction"),
		}
	}

	pub fn parse_raw_ident(&mut self) -> ParseResult<String> {
		let token = self.next();
		match token.kind {
			TokenKind::Keyword(_) | TokenKind::Language(_) | TokenKind::Algorithm(_) => {
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
		let next = self.next();
		match next.kind {
			TokenKind::Parameter => {
				let index = u32::from(next.data_index.unwrap());
				let param = self.lexer.strings[index as usize];
				Ok(Param(Ident(param)))
			}
			x => unexpected!(self, x, "a parameter"),
		}
	}

	pub(super) fn parse_duration(&mut self) -> ParseResult<Duration> {
		let next = self.next();
		match next.kind {
			TokenKind::Duration => {
				let index = u32::from(next.data_index.unwrap());
				let duration = self.lexer.durations[index as usize];
				Ok(Duration(duration))
			}
			x => unexpected!(self, x, "a duration"),
		}
	}

	pub(super) fn parse_strand(&mut self) -> ParseResult<Strand> {
		let next = self.next();
		match next.kind {
			TokenKind::Strand => {
				let index = u32::from(next.data_index.unwrap());
				let strand = self.lexer.strings[index as usize];
				Ok(Strand(strand))
			}
			x => unexpected!(self, x, "a strand"),
		}
	}
}
