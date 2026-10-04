//! Top-level PostgreSQL statement parser dispatching to specific statement kinds.

pub mod ddl;
pub mod delete;
pub mod graph;
pub mod insert;
pub mod misc;
pub mod select;
pub mod tx;
pub mod update;

use crate::ast::stmt::{CommonTableExpr, Statement, WithClause};
use crate::error::{ErrorKind, ParseError, ParseResult};
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;

impl<'a> Parser<'a> {
	/// Parse a single top-level SQL statement.
	pub fn parse_statement(&mut self) -> ParseResult<Statement> {
		self.enter_recursion(self.peek().span)?;
		let res = self.parse_statement_inner();
		self.leave_recursion();
		res
	}

	fn parse_statement_inner(&mut self) -> ParseResult<Statement> {
		if self.peek().is_eof() || self.peek().kind == TokenKind::Semi {
			return Ok(Statement::Empty);
		}

		match &self.peek().kind {
			TokenKind::Keyword(Keyword::With) => {
				let with = self.parse_with_clause()?;
				match &self.peek().kind {
					TokenKind::Keyword(Keyword::Select) => {
						let mut s = self.parse_select_stmt()?;
						s.span = with.span.merge(s.span);
						s.with = Some(with);
						Ok(Statement::Select(Box::new(s)))
					}
					TokenKind::Keyword(Keyword::Insert) => {
						let mut s = self.parse_insert_stmt()?;
						s.span = with.span.merge(s.span);
						s.with = Some(with);
						Ok(Statement::Insert(Box::new(s)))
					}
					TokenKind::Keyword(Keyword::Update) => {
						let mut s = self.parse_update_stmt()?;
						s.span = with.span.merge(s.span);
						s.with = Some(with);
						Ok(Statement::Update(Box::new(s)))
					}
					TokenKind::Keyword(Keyword::Delete) => {
						let mut s = self.parse_delete_stmt()?;
						s.span = with.span.merge(s.span);
						s.with = Some(with);
						Ok(Statement::Delete(Box::new(s)))
					}
					_ => Err(ParseError::new(
						self.peek().span,
						ErrorKind::UnexpectedToken {
							expected: "SELECT, INSERT, UPDATE, or DELETE after WITH clause",
							found: self.peek().kind.to_string(),
						},
					)),
				}
			}
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
			TokenKind::Keyword(Keyword::Truncate) => {
				let s = self.parse_truncate_stmt()?;
				Ok(Statement::Truncate(Box::new(s)))
			}
			TokenKind::Keyword(Keyword::Create) => self.parse_create_stmt(),
			TokenKind::Keyword(Keyword::Alter) => {
				let s = self.parse_alter_table_stmt()?;
				Ok(Statement::AlterTable(Box::new(s)))
			}
			TokenKind::Keyword(Keyword::Drop) => self.parse_drop_stmt(),
			TokenKind::Keyword(Keyword::Prepare) => {
				let s = self.parse_prepare_stmt()?;
				Ok(Statement::Prepare(Box::new(s)))
			}
			TokenKind::Keyword(Keyword::Execute) => {
				let s = self.parse_execute_stmt()?;
				Ok(Statement::Execute(s))
			}
			TokenKind::Keyword(Keyword::Deallocate) => {
				let s = self.parse_deallocate_stmt()?;
				Ok(Statement::Deallocate(s))
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

	/// Parse a `WITH [RECURSIVE] cte_name AS (...)` clause.
	pub fn parse_with_clause(&mut self) -> ParseResult<WithClause> {
		let with_tok = self.expect(&TokenKind::Keyword(Keyword::With), "'WITH'")?;
		let recursive = self.eat(&TokenKind::Keyword(Keyword::Recursive))?;

		let mut ctes = Vec::new();
		loop {
			let (name, name_span) = self.parse_ident()?;
			let mut columns = Vec::new();

			if self.eat(&TokenKind::LParen)? {
				loop {
					let (col, _) = self.parse_ident()?;
					columns.push(col);
					if !self.eat(&TokenKind::Comma)? {
						break;
					}
				}
				self.expect(&TokenKind::RParen, "')'")?;
			}

			self.expect(&TokenKind::Keyword(Keyword::As), "'AS'")?;

			let materialized = if self.eat(&TokenKind::Keyword(Keyword::Materialized))? {
				Some(true)
			} else if self.eat(&TokenKind::Keyword(Keyword::Not))? {
				self.expect(&TokenKind::Keyword(Keyword::Materialized), "'MATERIALIZED'")?;
				Some(false)
			} else {
				None
			};

			self.expect(&TokenKind::LParen, "'('")?;
			let query = self.parse_statement()?;
			let rparen = self.expect(&TokenKind::RParen, "')'")?;

			let span = name_span.merge(rparen.span);
			ctes.push(CommonTableExpr {
				name,
				columns,
				materialized,
				query: Box::new(query),
				span,
			});

			if !self.eat(&TokenKind::Comma)? {
				break;
			}
		}

		let end_span = ctes.last().map(|c| c.span).unwrap_or(with_tok.span);
		let span = with_tok.span.merge(end_span);

		Ok(WithClause {
			recursive,
			ctes,
			span,
		})
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

			// Require semicolon between statements if not at EOF
			if !self.peek().is_eof() && !self.eat(&TokenKind::Semi)? {
				return Err(ParseError::new(
					self.peek().span,
					ErrorKind::UnexpectedToken {
						expected: ";",
						found: self.peek().kind.to_string(),
					},
				));
			}
		}

		Ok(stmts)
	}
}
