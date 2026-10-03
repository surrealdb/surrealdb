//! REPLACE statement parser for MySQL.

use crate::ast::stmt::ReplaceStmt;
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;
use crate::parser::error::ParseResult;

impl<'a> Parser<'a> {
	/// Parse a REPLACE statement.
	pub fn parse_replace_stmt(&mut self) -> ParseResult<ReplaceStmt> {
		let start = self.expect_keyword(Keyword::Replace, "REPLACE")?.span;

		self.expect_keyword(Keyword::Into, "INTO after REPLACE")?;
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

		// VALUES / VALUE (row1), (row2)...
		if self.peek_keyword() == Some(Keyword::Values)
			|| self.peek_keyword() == Some(Keyword::Value)
		{
			self.bump();
		} else {
			self.expect_keyword(Keyword::Values, "VALUES in REPLACE")?;
		}

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

		let end = self.peek().span;
		let span = start.merge(end);

		Ok(ReplaceStmt {
			table,
			columns,
			values,
			span,
		})
	}
}
