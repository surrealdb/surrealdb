//! DELETE statement parser for MySQL.

use crate::ast::stmt::DeleteStmt;
use crate::lexer::keyword::Keyword;
use crate::parser::Parser;
use crate::parser::error::ParseResult;

impl<'a> Parser<'a> {
	/// Parse a DELETE statement.
	pub fn parse_delete_stmt(&mut self) -> ParseResult<DeleteStmt> {
		let start = self.expect_keyword(Keyword::Delete, "DELETE")?.span;
		self.expect_keyword(Keyword::From, "FROM after DELETE")?;
		let table = self.parse_qualified_name()?;

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

		Ok(DeleteStmt {
			table,
			where_clause,
			order_by,
			limit,
			span,
		})
	}
}
