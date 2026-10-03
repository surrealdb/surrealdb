//! Transaction statement parser for MySQL.

use crate::ast::stmt::TransactionStmt;
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;
use crate::parser::error::ParseResult;

impl<'a> Parser<'a> {
	/// Parse a transaction control statement.
	pub fn parse_transaction_stmt(&mut self) -> ParseResult<TransactionStmt> {
		if self.peek_keyword() == Some(Keyword::Start) {
			let start = self.bump().span;
			self.expect_keyword(Keyword::Transaction, "TRANSACTION after START")?;
			let read_only = if self.peek_keyword() == Some(Keyword::Read) {
				self.bump();
				if matches!(self.peek().kind, TokenKind::Ident(ref s) if s.eq_ignore_ascii_case("only"))
				{
					self.bump();
					true
				} else if self.peek_keyword() == Some(Keyword::Write) {
					self.bump();
					false
				} else {
					false
				}
			} else {
				false
			};
			let end = self.peek().span;
			Ok(TransactionStmt::StartTransaction {
				read_only,
				span: start.merge(end),
			})
		} else if self.peek_keyword() == Some(Keyword::Begin) {
			let start = self.bump().span;
			if self.peek_keyword() == Some(Keyword::Work) {
				self.bump();
			}
			let end = self.peek().span;
			Ok(TransactionStmt::Begin(start.merge(end)))
		} else if self.peek_keyword() == Some(Keyword::Commit) {
			let start = self.bump().span;
			if self.peek_keyword() == Some(Keyword::Work) {
				self.bump();
			}
			let end = self.peek().span;
			Ok(TransactionStmt::Commit(start.merge(end)))
		} else if self.peek_keyword() == Some(Keyword::Rollback) {
			let start = self.bump().span;
			if self.peek_keyword() == Some(Keyword::Work) {
				self.bump();
			}
			let end = self.peek().span;
			Ok(TransactionStmt::Rollback(start.merge(end)))
		} else {
			Err(crate::parser::error::ParseError::new(
				self.peek().span,
				crate::parser::error::ErrorKind::UnexpectedToken {
					expected: "START TRANSACTION, BEGIN, COMMIT, or ROLLBACK",
					found: self.peek().kind.to_string(),
				},
			))
		}
	}
}
