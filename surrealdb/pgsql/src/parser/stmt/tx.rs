//! PostgreSQL Transaction statement parser (`BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`).
//!
//! gram.y: TransactionStmt

use crate::ast::stmt::TransactionStmt;
use crate::error::ParseResult;
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;

impl<'a> Parser<'a> {
	/// Parse a PostgreSQL transaction management statement.
	///
	/// gram.y: TransactionStmt
	pub fn parse_transaction_stmt(&mut self) -> ParseResult<TransactionStmt> {
		let tok = self.peek().clone();

		match &tok.kind {
			TokenKind::Keyword(Keyword::Begin | Keyword::Start) => {
				let start_tok = self.bump()?;
				self.eat(&TokenKind::Keyword(Keyword::Work))?;
				self.eat(&TokenKind::Keyword(Keyword::Transaction))?;
				self.expect_tx_stmt_end()?;
				let span = start_tok.span.merge(self.peek().span);
				Ok(TransactionStmt::Begin {
					span,
				})
			}

			TokenKind::Keyword(Keyword::Commit | Keyword::End) => {
				let start_tok = self.bump()?;
				self.eat(&TokenKind::Keyword(Keyword::Work))?;
				self.eat(&TokenKind::Keyword(Keyword::Transaction))?;
				self.expect_tx_stmt_end()?;
				let span = start_tok.span.merge(self.peek().span);
				Ok(TransactionStmt::Commit {
					span,
				})
			}

			TokenKind::Keyword(Keyword::Rollback | Keyword::Abort) => {
				let start_tok = self.bump()?;
				self.eat(&TokenKind::Keyword(Keyword::Work))?;
				self.eat(&TokenKind::Keyword(Keyword::Transaction))?;

				// Check for ROLLBACK TO [SAVEPOINT] name
				if self.eat(&TokenKind::Keyword(Keyword::To))? {
					self.eat(&TokenKind::Keyword(Keyword::Savepoint))?;
					let (name, name_span) = self.parse_ident()?;
					self.expect_tx_stmt_end()?;
					let span = start_tok.span.merge(name_span);
					return Ok(TransactionStmt::RollbackToSavepoint {
						name,
						span,
					});
				}

				self.expect_tx_stmt_end()?;
				let span = start_tok.span.merge(self.peek().span);
				Ok(TransactionStmt::Rollback {
					span,
				})
			}

			TokenKind::Keyword(Keyword::Savepoint) => {
				let start_tok = self.bump()?;
				let (name, name_span) = self.parse_ident()?;
				self.expect_tx_stmt_end()?;
				let span = start_tok.span.merge(name_span);
				Ok(TransactionStmt::Savepoint {
					name,
					span,
				})
			}

			TokenKind::Keyword(Keyword::Release) => {
				let start_tok = self.bump()?;
				self.eat(&TokenKind::Keyword(Keyword::Savepoint))?;
				let (name, name_span) = self.parse_ident()?;
				self.expect_tx_stmt_end()?;
				let span = start_tok.span.merge(name_span);
				Ok(TransactionStmt::ReleaseSavepoint {
					name,
					span,
				})
			}

			_ => Err(crate::error::ParseError::new(
				tok.span,
				crate::error::ErrorKind::UnexpectedToken {
					expected: "transaction control keyword",
					found: tok.kind.to_string(),
				},
			)),
		}
	}

	/// Verify that a transaction statement has no unexpected trailing clauses or tokens before `;`
	/// or EOF.
	fn expect_tx_stmt_end(&self) -> ParseResult<()> {
		if self.peek().kind != TokenKind::Semi && !self.peek().is_eof() {
			Err(crate::error::ParseError::new(
				self.peek().span,
				crate::error::ErrorKind::InvalidStatement(
					"unsupported transaction mode or unexpected token after transaction statement",
				),
			))
		} else {
			Ok(())
		}
	}
}
