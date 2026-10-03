//! PostgreSQL `INSERT` statement parser.
//!
//! gram.y: InsertStmt

use crate::ast::stmt::{InsertStmt, OnConflictAction, OnConflictClause, SetClause};
use crate::error::ParseResult;
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;

impl<'a> Parser<'a> {
	/// Parse a PostgreSQL `INSERT` statement.
	///
	/// gram.y: InsertStmt
	pub fn parse_insert_stmt(&mut self) -> ParseResult<InsertStmt> {
		let with = if self.peek().kind == TokenKind::Keyword(Keyword::With) {
			Some(self.parse_with_clause()?)
		} else {
			None
		};

		let insert_tok = self.expect(&TokenKind::Keyword(Keyword::Insert), "'INSERT'")?;
		let start_span = with.as_ref().map(|w| w.span).unwrap_or(insert_tok.span);
		self.expect(&TokenKind::Keyword(Keyword::Into), "'INTO'")?;

		// Target table name
		let relation = self.parse_qualified_name()?;

		// Optional column list: `(col1, col2, ...)`
		let columns = if self.eat(&TokenKind::LParen)? {
			let mut cols = Vec::new();
			loop {
				let (col, _) = self.parse_ident()?;
				cols.push(col);
				if !self.eat(&TokenKind::Comma)? {
					break;
				}
			}
			self.expect(&TokenKind::RParen, "')'")?;
			cols
		} else {
			Vec::new()
		};

		// Values or Subquery
		let (values, query) = if self.eat(&TokenKind::Keyword(Keyword::Values))? {
			let mut rows = Vec::new();
			loop {
				self.expect(&TokenKind::LParen, "'('")?;
				let mut row = Vec::new();
				loop {
					row.push(self.parse_expr()?);
					if !self.eat(&TokenKind::Comma)? {
						break;
					}
				}
				self.expect(&TokenKind::RParen, "')'")?;
				rows.push(row);

				if !self.eat(&TokenKind::Comma)? {
					break;
				}
			}
			(Some(rows), None)
		} else if self.peek().kind == TokenKind::Keyword(Keyword::Select) {
			let select_stmt = self.parse_select_stmt()?;
			(None, Some(Box::new(select_stmt)))
		} else {
			(None, None)
		};

		// Optional ON CONFLICT clause
		let on_conflict = if self.eat(&TokenKind::Keyword(Keyword::On))? {
			self.expect(&TokenKind::Keyword(Keyword::Conflict), "'CONFLICT'")?;

			// Optional conflict target: `(col1, col2)`
			let target = if self.eat(&TokenKind::LParen)? {
				let mut cols = Vec::new();
				loop {
					let (col, _) = self.parse_ident()?;
					cols.push(col);
					if !self.eat(&TokenKind::Comma)? {
						break;
					}
				}
				self.expect(&TokenKind::RParen, "')'")?;
				Some(cols)
			} else {
				None
			};

			self.expect(&TokenKind::Keyword(Keyword::Do), "'DO'")?;

			let action = if self.eat(&TokenKind::Keyword(Keyword::Nothing))? {
				OnConflictAction::DoNothing
			} else {
				self.expect(&TokenKind::Keyword(Keyword::Update), "'UPDATE'")?;
				self.expect(&TokenKind::Keyword(Keyword::Set), "'SET'")?;
				let mut assignments = Vec::new();
				loop {
					let (column, col_span) = self.parse_ident()?;
					self.expect(&TokenKind::Operator("="), "'='")?;
					let value = self.parse_expr()?;
					let span = col_span.merge(value.span());
					assignments.push(SetClause {
						column,
						value,
						span,
					});
					if !self.eat(&TokenKind::Comma)? {
						break;
					}
				}
				OnConflictAction::DoUpdate(assignments)
			};

			let span = self.peek().span;
			Some(OnConflictClause {
				target,
				action,
				span,
			})
		} else {
			None
		};

		// Optional RETURNING clause
		let returning = if self.eat(&TokenKind::Keyword(Keyword::Returning))? {
			Some(self.parse_target_list()?)
		} else {
			None
		};

		let end_span = self.peek().span;
		let span = start_span.merge(end_span);

		Ok(InsertStmt {
			with,
			relation,
			columns,
			values,
			query,
			on_conflict,
			returning,
			span,
		})
	}
}
