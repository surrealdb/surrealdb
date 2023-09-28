use crate::sql::{
	parser2::{mac::expected, Expected},
	statements::{BeginStatement, SelectStatement},
	token::{t, TokenKind},
	Statement,
};

use super::{
	mac::{to_do, unexpected},
	ParseResult, Parser,
};

impl Parser<'_> {
	pub(super) fn parse_stmt(&mut self) -> ParseResult<Statement> {
		let token = self.peek_token();
		match token.kind {
			t!("ANALYZE") => self.parse_analyze(),
			t!("BEGIN") => self.parse_begin().map(Statement::Begin),
			t!("BREAK") => self.parse_begin().map(Statement::Begin),
			t!("CANCEL") => self.parse_begin().map(Statement::Begin),
			t!("COMMIT") => self.parse_begin().map(Statement::Begin),
			t!("CONTINUE") => self.parse_begin().map(Statement::Begin),
			t!("CREATE") => self.parse_begin().map(Statement::Begin),
			t!("DEFINE") => self.parse_begin().map(Statement::Begin),
			t!("DELETE") => self.parse_begin().map(Statement::Begin),
			t!("FOR") => self.parse_begin().map(Statement::Begin),
			t!("IF") => self.parse_begin().map(Statement::Begin),
			t!("INFO") => self.parse_begin().map(Statement::Begin),
			t!("INSERT") => self.parse_begin().map(Statement::Begin),
			t!("KILL") => self.parse_begin().map(Statement::Begin),
			t!("LIVE") => self.parse_begin().map(Statement::Begin),
			t!("OPTION") => self.parse_begin().map(Statement::Begin),
			t!("RETURN") => self.parse_begin().map(Statement::Begin),
			t!("RELATE") => self.parse_begin().map(Statement::Begin),
			t!("REMOVE") => self.parse_begin().map(Statement::Begin),
			t!("SELECT") => self.parse_select().map(Statement::Select),
			t!("LET") => self.parse_select().map(Statement::Select),
			t!("SHOW") => self.parse_select().map(Statement::Select),
			t!("SLEEP") => self.parse_select().map(Statement::Select),
			t!("THROW") => self.parse_select().map(Statement::Select),
			t!("UPDATE") => self.parse_select().map(Statement::Select),
			t!("USE") => self.parse_select().map(Statement::Select),
			_ => to_do!(self),
		}
	}

	fn parse_analyze(&mut self) -> ParseResult<Statement> {
		let keyword = self.next_token();
		debug_assert_eq!(keyword.kind, t!("ANALYZE"));
		let index = self.next_token();
		let t!("INDEX") = index.kind else {
			let value = self.parse_fallback_value(&[keyword, index])?;
			return Ok(Statement::Value(value));
		};
		let index = self.parse_ident();
		expected!(self, "ON");
		let table = self.parse_ident();

		to_do!(self)
	}

	fn parse_begin(&mut self) -> ParseResult<BeginStatement> {
		to_do!(self)
	}

	fn parse_select(&mut self) -> ParseResult<SelectStatement> {
		to_do!(self)
	}

	fn parse_ident(&mut self) -> ParseResult<String> {
		let token = self.next_token();
		match token.kind {
			TokenKind::Keyword(x) => {
				let str = self.lexer.reader.span(token.span);
				// Lexer should ensure that the token is valid utf-8
				let str = std::str::from_utf8(str).unwrap().to_owned();
				return Ok(str);
			}
			TokenKind::Number => {
				let str = self.lexer.reader.span(token.span);
				// Lexer should ensure that the token is valid utf-8
				let str = std::str::from_utf8(str).unwrap().to_owned();
				return Ok(str);
			}
			TokenKind::Identifier => {
				let data_index = token.data_index.unwrap();
				let idx = u32::from(data_index) as usize;
				return Ok(self.lexer.strings[idx].clone());
			}
			TokenKind::Invalid => todo!(),
			TokenKind::Eof => todo!(),
			x => {
				unexpected!(self, x, Expected::Identifier);
			}
		}
	}
}
