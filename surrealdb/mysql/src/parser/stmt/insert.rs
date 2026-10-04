//! INSERT statement parser for MySQL.

use crate::ast::stmt::InsertStmt;
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;
use crate::parser::error::ParseResult;

impl<'a> Parser<'a> {
	/// Parse an INSERT statement.
	pub fn parse_insert_stmt(&mut self) -> ParseResult<InsertStmt> {
		let start = self.expect_keyword(Keyword::Insert, "INSERT")?.span;

		let ignore = if self.peek_keyword() == Some(Keyword::Ignore) {
			self.bump();
			true
		} else {
			false
		};

		self.expect_keyword(Keyword::Into, "INTO after INSERT")?;
		let table = self.parse_qualified_name()?;

		// Optional column list: (col1, col2, ...)
		let columns = if self.peek().kind == TokenKind::LParen {
			self.bump();
			let mut cols = Vec::new();
			loop {
				cols.push(self.parse_ident()?.0);
				if self.peek().kind == TokenKind::Comma {
					self.bump();
				} else {
					break;
				}
			}
			self.expect(&TokenKind::RParen, "')' closing column list")?;
			cols
		} else {
			Vec::new()
		};

		// VALUES / VALUE / SELECT / DEFAULT VALUES
		let (values, query) = if self.peek_keyword() == Some(Keyword::Values)
			|| self.peek_keyword() == Some(Keyword::Value)
		{
			self.bump();
			let mut values = Vec::new();
			loop {
				self.expect(&TokenKind::LParen, "'(' starting row values")?;
				let mut row = Vec::new();
				loop {
					row.push(self.parse_expr()?);
					if self.peek().kind == TokenKind::Comma {
						self.bump();
					} else {
						break;
					}
				}
				self.expect(&TokenKind::RParen, "')' closing row values")?;
				values.push(row);

				if self.peek().kind == TokenKind::Comma {
					self.bump();
				} else {
					break;
				}
			}
			(values, None)
		} else if self.peek_keyword() == Some(Keyword::Select) {
			let select = self.parse_select_stmt()?;
			(Vec::new(), Some(Box::new(select)))
		} else if self.peek_keyword() == Some(Keyword::Default) {
			self.bump();
			self.expect_keyword(Keyword::Values, "VALUES after DEFAULT in INSERT")?;
			(Vec::new(), None)
		} else {
			self.expect_keyword(Keyword::Values, "VALUES or SELECT in INSERT")?;
			(Vec::new(), None)
		};

		// ON DUPLICATE KEY UPDATE col = expr, ...
		let mut on_duplicate_key_update = Vec::new();
		if self.peek_keyword() == Some(Keyword::On)
			&& self.peek_nth_keyword(1) == Some(Keyword::Duplicate)
			&& self.peek_nth_keyword(2) == Some(Keyword::Key)
			&& self.peek_nth_keyword(3) == Some(Keyword::Update)
		{
			self.bump(); // ON
			self.bump(); // DUPLICATE
			self.bump(); // KEY
			self.bump(); // UPDATE

			loop {
				let (col, _) = self.parse_ident()?;
				self.expect(&TokenKind::Equal, "'=' in ON DUPLICATE KEY UPDATE")?;
				let val = self.parse_expr()?;
				on_duplicate_key_update.push((col, val));

				if self.peek().kind == TokenKind::Comma {
					self.bump();
				} else {
					break;
				}
			}
		}

		let end = self.peek().span;
		let span = start.merge(end);

		Ok(InsertStmt {
			ignore,
			table,
			columns,
			values,
			query,
			on_duplicate_key_update,
			span,
		})
	}
}
