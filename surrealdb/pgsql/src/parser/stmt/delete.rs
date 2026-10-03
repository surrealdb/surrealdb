//! PostgreSQL `DELETE` statement parser.
//!
//! gram.y: DeleteStmt

use crate::ast::stmt::DeleteStmt;
use crate::error::ParseResult;
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;

impl<'a> Parser<'a> {
	/// Parse a PostgreSQL `DELETE` statement.
	///
	/// gram.y: DeleteStmt
	pub fn parse_delete_stmt(&mut self) -> ParseResult<DeleteStmt> {
		let with = if self.peek().kind == TokenKind::Keyword(Keyword::With) {
			Some(self.parse_with_clause()?)
		} else {
			None
		};

		let delete_tok = self.expect(&TokenKind::Keyword(Keyword::Delete), "'DELETE'")?;
		let start_span = with.as_ref().map(|w| w.span).unwrap_or(delete_tok.span);
		self.expect(&TokenKind::Keyword(Keyword::From), "'FROM'")?;

		// Target table name
		let relation = self.parse_qualified_name()?;

		// Optional USING clause
		let using = if self.eat(&TokenKind::Keyword(Keyword::Using))? {
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

		Ok(DeleteStmt {
			with,
			relation,
			using,
			where_clause,
			returning,
			span,
		})
	}
}
