//! UPDATE statement parser for MySQL.

use crate::ast::stmt::UpdateStmt;
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;
use crate::parser::error::ParseResult;

impl<'a> Parser<'a> {
	/// Parse an UPDATE statement.
	pub fn parse_update_stmt(&mut self) -> ParseResult<UpdateStmt> {
		let start = self.expect_keyword(Keyword::Update, "UPDATE")?.span;
		let table = self.parse_table_references()?;

		self.expect_keyword(Keyword::Set, "SET in UPDATE")?;

		let mut assignments = Vec::new();
		loop {
			let (col, _) = self.parse_ident()?;
			self.expect(&TokenKind::Equal, "'=' in UPDATE assignment")?;
			let val = self.parse_expr()?;
			assignments.push((col, val));

			if self.peek().kind == TokenKind::Comma {
				self.bump();
			} else {
				break;
			}
		}

		let where_clause = if self.peek_keyword() == Some(Keyword::Where) {
			self.bump();
			Some(self.parse_expr()?)
		} else {
			None
		};

		let order_by = if self.peek_keyword() == Some(Keyword::Order) {
			self.bump();
			self.expect_keyword(Keyword::By, "BY after ORDER")?;
			self.parse_order_by_list()?
		} else {
			Vec::new()
		};

		let limit = if self.peek_keyword() == Some(Keyword::Limit) {
			self.bump();
			Some(self.parse_expr()?)
		} else {
			None
		};

		let end = self.peek().span;
		let span = start.merge(end);

		Ok(UpdateStmt {
			table,
			assignments,
			where_clause,
			order_by,
			limit,
			span,
		})
	}
}
