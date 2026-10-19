//! PostgreSQL Transaction statement parser (`BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`).
//!
//! gram.y: TransactionStmt

use crate::ast::stmt::{IsolationLevel, TransactionStmt};
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
				let mut read_only = false;
				let mut isolation = None;
				let mut deferrable = false;
				let unsupported = |span, message: &'static str| {
					crate::error::ParseError::new(
						span,
						crate::error::ErrorKind::InvalidStatement(message),
					)
				};
				while self.peek().kind != TokenKind::Semi && !self.peek().is_eof() {
					if self.eat(&TokenKind::Keyword(Keyword::Isolation))? {
						if !self.eat(&TokenKind::Keyword(Keyword::Level))? {
							return Err(unsupported(
								self.peek().span,
								"expected LEVEL after ISOLATION",
							));
						}
						isolation = Some(
							if self.eat(&TokenKind::Keyword(Keyword::Serializable))? {
								IsolationLevel::Serializable
							} else if matches!(&self.peek().kind, TokenKind::Ident(id) if id.eq_ignore_ascii_case("repeatable"))
							{
								self.bump()?;
								if !self.eat(&TokenKind::Keyword(Keyword::Read))? {
									return Err(unsupported(
										self.peek().span,
										"expected READ after REPEATABLE",
									));
								}
								IsolationLevel::RepeatableRead
							} else if self.eat(&TokenKind::Keyword(Keyword::Read))? {
								if self.eat(&TokenKind::Keyword(Keyword::Committed))? {
									IsolationLevel::ReadCommitted
								} else if self.eat(&TokenKind::Keyword(Keyword::Uncommitted))? {
									IsolationLevel::ReadUncommitted
								} else {
									return Err(unsupported(
										self.peek().span,
										"expected COMMITTED or UNCOMMITTED after READ",
									));
								}
							} else {
								return Err(unsupported(
									self.peek().span,
									"unsupported transaction isolation level",
								));
							},
						);
					} else if self.eat(&TokenKind::Keyword(Keyword::Read))? {
						if self.eat(&TokenKind::Keyword(Keyword::Only))? {
							read_only = true;
						} else if self.eat(&TokenKind::Keyword(Keyword::Write))? {
							read_only = false;
						} else {
							return Err(unsupported(
								self.peek().span,
								"expected ONLY or WRITE after READ",
							));
						}
					} else if self.eat(&TokenKind::Keyword(Keyword::Not))? {
						if !self.eat(&TokenKind::Keyword(Keyword::Deferrable))? {
							return Err(unsupported(
								self.peek().span,
								"expected DEFERRABLE after NOT",
							));
						}
						deferrable = false;
					} else if self.eat(&TokenKind::Keyword(Keyword::Deferrable))? {
						deferrable = true;
					} else if self.eat(&TokenKind::Comma)? {
					} else {
						break;
					}
				}
				// `DEFERRABLE` only means something on a serializable read-only
				// transaction, where it has no effect here; anywhere else it is
				// refused rather than accepted and ignored.
				if deferrable && !(read_only && isolation == Some(IsolationLevel::Serializable)) {
					return Err(unsupported(
						self.peek().span,
						"DEFERRABLE needs SERIALIZABLE READ ONLY",
					));
				}
				self.expect_tx_stmt_end()?;
				let span = start_tok.span.merge(self.peek().span);
				Ok(TransactionStmt::Begin {
					read_only,
					isolation,
					span,
				})
			}

			TokenKind::Keyword(Keyword::Commit | Keyword::End) => {
				let start_tok = self.bump()?;
				self.eat(&TokenKind::Keyword(Keyword::Work))?;
				self.eat(&TokenKind::Keyword(Keyword::Transaction))?;
				if self.eat(&TokenKind::Keyword(Keyword::And))? {
					if self.eat(&TokenKind::Keyword(Keyword::No))? {
						self.eat(&TokenKind::Keyword(Keyword::Chain))?;
					} else if self.eat(&TokenKind::Keyword(Keyword::Chain))? {
						return Err(crate::error::ParseError::new(
							self.peek().span,
							crate::error::ErrorKind::InvalidStatement(
								"chained transactions are not supported",
							),
						));
					}
				}
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

				if self.eat(&TokenKind::Keyword(Keyword::And))? {
					if self.eat(&TokenKind::Keyword(Keyword::No))? {
						self.eat(&TokenKind::Keyword(Keyword::Chain))?;
					} else if self.eat(&TokenKind::Keyword(Keyword::Chain))? {
						return Err(crate::error::ParseError::new(
							self.peek().span,
							crate::error::ErrorKind::InvalidStatement(
								"chained transactions are not supported",
							),
						));
					}
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
