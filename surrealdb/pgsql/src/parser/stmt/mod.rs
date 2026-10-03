//! Top-level PostgreSQL statement parser dispatching to specific statement kinds.

pub mod ddl;
pub mod delete;
pub mod insert;
pub mod misc;
pub mod select;
pub mod tx;
pub mod update;

use crate::ast::stmt::Statement;
use crate::error::{ErrorKind, ParseError, ParseResult};
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;

impl<'a> Parser<'a> {
	/// Parse a single top-level SQL statement.
	pub fn parse_statement(&mut self) -> ParseResult<Statement> {
		if self.peek().is_eof() || self.peek().kind == TokenKind::Semi {
			return Ok(Statement::Empty);
		}

		match &self.peek().kind {
			TokenKind::Keyword(Keyword::Select) => {
				let s = self.parse_select_stmt()?;
				Ok(Statement::Select(Box::new(s)))
			}
			TokenKind::Keyword(Keyword::Insert) => {
				let s = self.parse_insert_stmt()?;
				Ok(Statement::Insert(Box::new(s)))
			}
			TokenKind::Keyword(Keyword::Update) => {
				let s = self.parse_update_stmt()?;
				Ok(Statement::Update(Box::new(s)))
			}
			TokenKind::Keyword(Keyword::Delete) => {
				let s = self.parse_delete_stmt()?;
				Ok(Statement::Delete(Box::new(s)))
			}
			TokenKind::Keyword(Keyword::Create) => {
				let s = self.parse_create_table_stmt()?;
				Ok(Statement::CreateTable(Box::new(s)))
			}
			TokenKind::Keyword(Keyword::Drop) => {
				let s = self.parse_drop_stmt()?;
				Ok(Statement::Drop(Box::new(s)))
			}
			TokenKind::Keyword(
				Keyword::Begin
				| Keyword::Start
				| Keyword::Commit
				| Keyword::End
				| Keyword::Rollback
				| Keyword::Abort
				| Keyword::Savepoint
				| Keyword::Release,
			) => {
				let s = self.parse_transaction_stmt()?;
				Ok(Statement::Transaction(s))
			}
			TokenKind::Keyword(Keyword::Set) => {
				let s = self.parse_set_stmt()?;
				Ok(Statement::Set(s))
			}
			TokenKind::Keyword(Keyword::Show) => {
				let s = self.parse_show_stmt()?;
				Ok(Statement::Show(s))
			}
			TokenKind::Keyword(Keyword::Reset) => {
				let s = self.parse_reset_stmt()?;
				Ok(Statement::Reset(s))
			}
			TokenKind::Keyword(Keyword::Explain) => {
				let s = self.parse_explain_stmt()?;
				Ok(Statement::Explain(Box::new(s)))
			}
			_ => Err(ParseError::new(
				self.peek().span,
				ErrorKind::UnexpectedToken {
					expected: "SQL statement starting keyword",
					found: self.peek().kind.to_string(),
				},
			)),
		}
	}

	/// Parse a sequence of statements separated by semicolons.
	pub fn parse_statements(&mut self) -> ParseResult<Vec<Statement>> {
		let mut stmts = Vec::new();

		while !self.peek().is_eof() {
			if self.eat(&TokenKind::Semi)? {
				continue;
			}

			let stmt = self.parse_statement()?;
			if stmt != Statement::Empty {
				stmts.push(stmt);
			}

			// Eat optional trailing semicolon
			self.eat(&TokenKind::Semi)?;
		}

		Ok(stmts)
	}
}
