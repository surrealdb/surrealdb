//! SHOW, DESCRIBE, and EXPLAIN statement parser for MySQL.

use crate::ast::stmt::{DescribeStmt, ExplainStmt, ShowStmt};
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;
use crate::parser::error::{ErrorKind, ParseError, ParseResult};

impl<'a> Parser<'a> {
	/// Parse a SHOW statement.
	pub fn parse_show_stmt(&mut self) -> ParseResult<ShowStmt> {
		let start = self.expect_keyword(Keyword::Show, "SHOW")?.span;

		// SHOW [GLOBAL | SESSION] VARIABLES / STATUS
		let is_global = if self.peek_keyword() == Some(Keyword::Global) {
			self.bump();
			true
		} else if self.peek_keyword() == Some(Keyword::Session)
			|| self.peek_keyword() == Some(Keyword::Local)
		{
			self.bump();
			false
		} else {
			false
		};

		if self.peek_keyword() == Some(Keyword::Variables) {
			self.bump();
			let like = self.parse_optional_like()?;
			let end = self.peek().span;
			return Ok(ShowStmt::Variables {
				is_global,
				like,
				span: start.merge(end),
			});
		}

		if self.peek_keyword() == Some(Keyword::Status) {
			self.bump();
			let like = self.parse_optional_like()?;
			let end = self.peek().span;
			return Ok(ShowStmt::Status {
				is_global,
				like,
				span: start.merge(end),
			});
		}

		if self.peek_keyword() == Some(Keyword::Tables) {
			self.bump();
			let database = if self.peek_keyword() == Some(Keyword::From) {
				self.bump();
				Some(self.parse_ident()?.0)
			} else {
				None
			};
			let like = self.parse_optional_like()?;
			let end = self.peek().span;
			return Ok(ShowStmt::Tables {
				database,
				like,
				span: start.merge(end),
			});
		}

		if self.peek_keyword() == Some(Keyword::Databases)
			|| self.peek_keyword() == Some(Keyword::Schemas)
		{
			self.bump();
			let like = self.parse_optional_like()?;
			let end = self.peek().span;
			return Ok(ShowStmt::Databases {
				like,
				span: start.merge(end),
			});
		}

		if self.peek_keyword() == Some(Keyword::Columns) {
			self.bump();
			self.expect_keyword(Keyword::From, "FROM after SHOW COLUMNS")?;
			let table = self.parse_qualified_name()?;
			let like = self.parse_optional_like()?;
			let end = self.peek().span;
			return Ok(ShowStmt::Columns {
				table,
				like,
				span: start.merge(end),
			});
		}

		if self.peek_keyword() == Some(Keyword::Create)
			&& self.peek_nth_keyword(1) == Some(Keyword::Table)
		{
			self.bump(); // CREATE
			self.bump(); // TABLE
			let table = self.parse_qualified_name()?;
			let end = self.peek().span;
			return Ok(ShowStmt::CreateTable {
				table,
				span: start.merge(end),
			});
		}

		if self.peek_keyword() == Some(Keyword::Processlist) {
			self.bump();
			let end = self.peek().span;
			return Ok(ShowStmt::Processlist {
				full: false,
				span: start.merge(end),
			});
		}

		if self.peek_keyword() == Some(Keyword::Warnings) {
			self.bump();
			let end = self.peek().span;
			return Ok(ShowStmt::Warnings {
				span: start.merge(end),
			});
		}

		if self.peek_keyword() == Some(Keyword::Errors) {
			self.bump();
			let end = self.peek().span;
			return Ok(ShowStmt::Errors {
				span: start.merge(end),
			});
		}

		Err(ParseError::new(
			self.peek().span,
			ErrorKind::UnexpectedToken {
				expected: "TABLES, DATABASES, COLUMNS, VARIABLES, STATUS, etc. after SHOW",
				found: self.peek().kind.to_string(),
			},
		))
	}

	fn parse_optional_like(&mut self) -> ParseResult<Option<String>> {
		if self.peek_keyword() == Some(Keyword::Like) {
			self.bump();
			if let TokenKind::StringLiteral(s) = &self.peek().kind {
				let s = s.clone();
				self.bump();
				Ok(Some(s))
			} else {
				Err(ParseError::new(
					self.peek().span,
					ErrorKind::UnexpectedToken {
						expected: "string literal after LIKE",
						found: self.peek().kind.to_string(),
					},
				))
			}
		} else {
			Ok(None)
		}
	}

	/// Parse a DESCRIBE or DESC statement.
	pub fn parse_describe_stmt(&mut self) -> ParseResult<DescribeStmt> {
		let start = self.bump().span; // DESCRIBE or DESC
		let table = self.parse_qualified_name()?;
		let column =
			if matches!(self.peek().kind, TokenKind::Ident(_)) && self.peek_keyword().is_none() {
				Some(self.parse_ident()?.0)
			} else {
				None
			};
		let end = self.peek().span;
		Ok(DescribeStmt {
			table,
			column,
			span: start.merge(end),
		})
	}

	/// Parse an EXPLAIN statement.
	pub fn parse_explain_stmt(&mut self) -> ParseResult<ExplainStmt> {
		let start = self.expect_keyword(Keyword::Explain, "EXPLAIN")?.span;
		let statement = Box::new(self.parse_statement()?);
		let end = statement.span();
		Ok(ExplainStmt {
			statement,
			span: start.merge(end),
		})
	}
}
