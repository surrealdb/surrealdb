//! PostgreSQL `UPDATE` statement parser.
//!
//! gram.y: UpdateStmt

use crate::ast::stmt::{SetClause, UpdateStmt};
use crate::error::ParseResult;
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;

impl<'a> Parser<'a> {
	/// Parse a PostgreSQL `UPDATE` statement.
	///
	/// gram.y: UpdateStmt
	pub fn parse_update_stmt(&mut self) -> ParseResult<UpdateStmt> {
		let with = if self.peek().kind == TokenKind::Keyword(Keyword::With) {
			Some(self.parse_with_clause()?)
		} else {
			None
		};

		let update_tok = self.expect(&TokenKind::Keyword(Keyword::Update), "'UPDATE'")?;
		let start_span = with.as_ref().map(|w| w.span).unwrap_or(update_tok.span);

		// Target table name
		let relation = self.parse_qualified_name()?;

		// SET col = val, ...
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

		// Optional FROM clause
		let from = if self.eat(&TokenKind::Keyword(Keyword::From))? {
			Some(self.parse_from_clause()?)
		} else {
			None
		};

		// Optional WHERE clause
		let where_clause = if self.eat(&TokenKind::Keyword(Keyword::Where))? {
			Some(self.parse_expr()?)
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

		Ok(UpdateStmt {
			with,
			relation,
			assignments,
			from,
			where_clause,
			returning,
			span,
		})
	}
}
