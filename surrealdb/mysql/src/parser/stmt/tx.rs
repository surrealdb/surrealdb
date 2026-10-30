//! Transaction statement parser for MySQL.

use crate::ast::stmt::TransactionStmt;
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;
use crate::parser::error::{ErrorKind, ParseError, ParseResult};

impl<'a> Parser<'a> {
	/// Parse a transaction control statement.
	pub fn parse_transaction_stmt(&mut self) -> ParseResult<TransactionStmt> {
		if self.peek_keyword() == Some(Keyword::Start) {
			let start = self.bump().span;
			self.expect_keyword(Keyword::Transaction, "TRANSACTION after START")?;
			let unexpected = |parser: &Self, expected: &'static str| {
				let tok = parser.peek();
				ParseError::new(
					tok.span,
					ErrorKind::UnexpectedToken {
						expected,
						found: tok.kind.to_string(),
					},
				)
			};
			let word_is = |parser: &Self, word: &str| matches!(&parser.peek().kind, TokenKind::Ident(s) if s.eq_ignore_ascii_case(word));
			let mut read_only = false;
			// `WITH CONSISTENT SNAPSHOT` and `READ WRITE` ask for what every transaction here
			// already has, so they are accepted and need no record.
			loop {
				if self.peek_keyword() == Some(Keyword::With) {
					self.bump();
					if !word_is(self, "consistent") {
						return Err(unexpected(self, "CONSISTENT SNAPSHOT after WITH"));
					}
					self.bump();
					if !word_is(self, "snapshot") {
						return Err(unexpected(self, "SNAPSHOT after CONSISTENT"));
					}
					self.bump();
				} else if self.peek_keyword() == Some(Keyword::Read) {
					self.bump();
					if word_is(self, "only") {
						self.bump();
						read_only = true;
					} else if self.peek_keyword() == Some(Keyword::Write) {
						self.bump();
						read_only = false;
					} else {
						return Err(unexpected(self, "ONLY or WRITE after READ"));
					}
				} else {
					break;
				}
				if self.peek().kind == TokenKind::Comma {
					self.bump();
				}
			}
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
			if self.peek_keyword() == Some(Keyword::To) {
				self.bump();
				if self.peek_keyword() == Some(Keyword::Savepoint) {
					self.bump();
				}
				let (name, name_span) = self.parse_ident()?;
				return Ok(TransactionStmt::RollbackToSavepoint {
					name,
					span: start.merge(name_span),
				});
			}
			let end = self.peek().span;
			Ok(TransactionStmt::Rollback(start.merge(end)))
		} else if self.peek_keyword() == Some(Keyword::Savepoint) {
			let start = self.bump().span;
			let (name, name_span) = self.parse_ident()?;
			Ok(TransactionStmt::Savepoint {
				name,
				span: start.merge(name_span),
			})
		} else if self.peek_keyword() == Some(Keyword::Release) {
			let start = self.bump().span;
			if self.peek_keyword() == Some(Keyword::Savepoint) {
				self.bump();
			}
			let (name, name_span) = self.parse_ident()?;
			Ok(TransactionStmt::ReleaseSavepoint {
				name,
				span: start.merge(name_span),
			})
		} else {
			Err(crate::parser::error::ParseError::new(
				self.peek().span,
				crate::parser::error::ErrorKind::UnexpectedToken {
					expected: "START TRANSACTION, BEGIN, COMMIT, ROLLBACK, SAVEPOINT, or RELEASE",
					found: self.peek().kind.to_string(),
				},
			))
		}
	}
}
