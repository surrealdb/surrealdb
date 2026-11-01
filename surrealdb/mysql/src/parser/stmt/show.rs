//! SHOW, DESCRIBE, and EXPLAIN statement parser for MySQL.

use crate::ast::stmt::{DescribeStmt, ExplainStmt, QualifiedName, ShowStmt};
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;
use crate::parser::error::{ErrorKind, ParseError, ParseResult};

impl<'a> Parser<'a> {
	/// Parse a SHOW statement.
	pub fn parse_show_stmt(&mut self) -> ParseResult<ShowStmt> {
		let start = self.expect_keyword(Keyword::Show, "SHOW")?.span;

		let full = if self.peek_keyword() == Some(Keyword::Full) {
			self.bump();
			true
		} else {
			false
		};

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

		if self.peek_is_word("TRIGGERS") {
			self.bump();
			let database = if self.eat_from_or_in() {
				Some(self.parse_ident()?.0)
			} else {
				None
			};
			let like = self.parse_optional_like()?;
			let end = self.peek().span;
			return Ok(ShowStmt::Triggers {
				database,
				like,
				span: start.merge(end),
			});
		}

		if self.peek_keyword() == Some(Keyword::Table)
			&& self.peek_nth_keyword(1) == Some(Keyword::Status)
		{
			self.bump();
			self.bump();
			let database = if self.eat_from_or_in() {
				Some(self.parse_ident()?.0)
			} else {
				None
			};
			let like = self.parse_optional_like()?;
			let end = self.peek().span;
			return Ok(ShowStmt::TableStatus {
				database,
				like,
				span: start.merge(end),
			});
		}

		if self.peek_keyword() == Some(Keyword::Tables) {
			self.bump();
			let database = if self.eat_from_or_in() {
				Some(self.parse_ident()?.0)
			} else {
				None
			};
			let like = self.parse_optional_like()?;
			let end = self.peek().span;
			return Ok(ShowStmt::Tables {
				full,
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

		if self.peek_keyword() == Some(Keyword::Columns) || self.peek_is_word("FIELDS") {
			self.bump();
			if !self.eat_from_or_in() {
				return Err(self.unexpected("FROM or IN after SHOW COLUMNS"));
			}
			let mut table = self.parse_qualified_name()?;
			self.parse_database_after(&mut table)?;
			let like = self.parse_optional_like()?;
			let end = self.peek().span;
			return Ok(ShowStmt::Columns {
				full,
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
				full,
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

		if self.peek_keyword() == Some(Keyword::Index)
			|| self.peek_keyword() == Some(Keyword::Key)
			|| self.peek_is_word("KEYS")
			|| self.peek_is_word("INDEXES")
		{
			self.bump();
			self.eat_from_or_in();
			let mut table = self.parse_qualified_name()?;
			self.parse_database_after(&mut table)?;
			let end = self.peek().span;
			return Ok(ShowStmt::Index {
				table,
				span: start.merge(end),
			});
		}

		if self.peek_keyword() == Some(Keyword::Engines)
			|| self.peek_keyword() == Some(Keyword::Engine)
		{
			self.bump();
			let end = self.peek().span;
			return Ok(ShowStmt::Engines {
				span: start.merge(end),
			});
		}

		if self.peek_keyword() == Some(Keyword::Collation) {
			self.bump();
			let like = self.parse_optional_like()?;
			let end = self.peek().span;
			return Ok(ShowStmt::Collation {
				like,
				span: start.merge(end),
			});
		}

		if self.peek_keyword() == Some(Keyword::Charset)
			|| (self.peek_keyword() == Some(Keyword::Character)
				&& self.peek_nth_keyword(1) == Some(Keyword::Set))
		{
			if self.peek_keyword() == Some(Keyword::Character) {
				self.bump();
				self.bump();
			} else {
				self.bump();
			}
			let like = self.parse_optional_like()?;
			let end = self.peek().span;
			return Ok(ShowStmt::Charset {
				like,
				span: start.merge(end),
			});
		}

		Err(ParseError::new(
			self.peek().span,
			ErrorKind::UnexpectedToken {
				expected: "TABLES, DATABASES, COLUMNS, VARIABLES, STATUS, INDEX, ENGINES, COLLATION, CHARSET, etc. after SHOW",
				found: self.peek().kind.to_string(),
			},
		))
	}

	/// Whether the next token is the identifier `word`, which MySQL reads as a keyword here.
	pub(crate) fn peek_is_word(&self, word: &str) -> bool {
		matches!(&self.peek().kind, TokenKind::Ident(ident) if ident.eq_ignore_ascii_case(word))
	}

	/// Consume `FROM` or `IN`, which MySQL accepts alike before a table or database name.
	fn eat_from_or_in(&mut self) -> bool {
		if matches!(self.peek_keyword(), Some(Keyword::From | Keyword::In)) {
			self.bump();
			true
		} else {
			false
		}
	}

	/// Parse the optional `{FROM | IN} <database>` that follows a table name and put the database
	/// in front of the table's name, as if it had been written `<database>.<table>`. A database
	/// named both ways is refused.
	fn parse_database_after(&mut self, table: &mut QualifiedName) -> ParseResult<()> {
		if !self.eat_from_or_in() {
			return Ok(());
		}
		let (database, span) = self.parse_ident()?;
		if table.parts.len() != 1 {
			return Err(ParseError::new(
				span,
				ErrorKind::UnexpectedToken {
					expected: "a database named once, either before the table name or after it",
					found: database,
				},
			));
		}
		table.parts.insert(0, database);
		table.span = table.span.merge(span);
		Ok(())
	}

	fn unexpected(&self, expected: &'static str) -> ParseError {
		let tok = self.peek();
		ParseError::new(
			tok.span,
			ErrorKind::UnexpectedToken {
				expected,
				found: tok.kind.to_string(),
			},
		)
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
