//! SELECT statement parsing for MySQL.

use crate::ast::stmt::{
	JoinCondition, JoinType, LimitClause, LockClause, LockType, OrderByExpr, SelectItem,
	SelectStmt, TableRef,
};
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;
use crate::parser::error::{ErrorKind, ParseError, ParseResult};

impl<'a> Parser<'a> {
	/// Parse a SELECT statement.
	pub fn parse_select_stmt(&mut self) -> ParseResult<SelectStmt> {
		let start = self.expect_keyword(Keyword::Select, "SELECT")?.span;

		// Distinct / All
		let distinct = if self.peek_keyword() == Some(Keyword::Distinct)
			|| self.peek_keyword() == Some(Keyword::Distinctrow)
		{
			self.bump();
			true
		} else if self.peek_keyword() == Some(Keyword::All) {
			self.bump();
			false
		} else {
			false
		};

		// Projection list
		let mut projection = Vec::new();
		loop {
			if self.peek().kind == TokenKind::Star {
				let s = self.bump().span;
				projection.push(SelectItem::Wildcard(s));
			} else {
				let expr = self.parse_expr()?;
				let alias = if self.peek_keyword() == Some(Keyword::As) {
					self.bump();
					Some(self.parse_ident()?.0)
				} else if matches!(self.peek().kind, TokenKind::Ident(_))
					&& self.peek_keyword().is_none()
				{
					Some(self.parse_ident()?.0)
				} else {
					None
				};
				let span = expr.span();
				projection.push(SelectItem::Expr {
					expr,
					alias,
					span,
				});
			}

			if self.peek().kind == TokenKind::Comma {
				self.bump();
			} else {
				break;
			}
		}

		// FROM clause
		let from = if self.peek_keyword() == Some(Keyword::From) {
			self.bump(); // FROM
			Some(self.parse_table_references()?)
		} else {
			None
		};

		// WHERE clause
		let where_clause = if self.peek_keyword() == Some(Keyword::Where) {
			self.bump(); // WHERE
			Some(self.parse_expr()?)
		} else {
			None
		};

		// GROUP BY clause
		let group_by = if self.peek_keyword() == Some(Keyword::Group) {
			self.bump(); // GROUP
			self.expect_keyword(Keyword::By, "BY after GROUP")?;
			let mut items = Vec::new();
			loop {
				items.push(self.parse_expr()?);
				if self.peek().kind == TokenKind::Comma {
					self.bump();
				} else {
					break;
				}
			}
			items
		} else {
			Vec::new()
		};

		// HAVING clause
		let having = if self.peek_keyword() == Some(Keyword::Having) {
			self.bump(); // HAVING
			Some(self.parse_expr()?)
		} else {
			None
		};

		// ORDER BY clause
		let order_by = if self.peek_keyword() == Some(Keyword::Order) {
			self.bump(); // ORDER
			self.expect_keyword(Keyword::By, "BY after ORDER")?;
			self.parse_order_by_list()?
		} else {
			Vec::new()
		};

		// LIMIT clause: LIMIT count, LIMIT offset, count, or LIMIT count OFFSET offset
		let limit = if self.peek_keyword() == Some(Keyword::Limit) {
			let limit_start = self.bump().span;
			let first = self.parse_expr()?;
			if self.peek().kind == TokenKind::Comma {
				self.bump(); // ','
				let second = self.parse_expr()?;
				let span = limit_start.merge(second.span());
				Some(LimitClause {
					offset: Some(first),
					count: second,
					span,
				})
			} else if self.peek_keyword() == Some(Keyword::Offset) {
				self.bump(); // OFFSET
				let off = self.parse_expr()?;
				let span = limit_start.merge(off.span());
				Some(LimitClause {
					offset: Some(off),
					count: first,
					span,
				})
			} else {
				let span = limit_start.merge(first.span());
				Some(LimitClause {
					offset: None,
					count: first,
					span,
				})
			}
		} else {
			None
		};

		// Locking clause: FOR UPDATE, FOR SHARE, LOCK IN SHARE MODE
		let lock = self.parse_lock_clause()?;

		let end = self.peek().span;
		let span = start.merge(end);

		Ok(SelectStmt {
			distinct,
			projection,
			from,
			where_clause,
			group_by,
			having,
			order_by,
			limit,
			lock,
			span,
		})
	}

	pub(crate) fn parse_order_by_list(&mut self) -> ParseResult<Vec<OrderByExpr>> {
		let mut items = Vec::new();
		loop {
			let expr = self.parse_expr()?;
			let asc = if self.peek_keyword() == Some(Keyword::Desc) {
				self.bump();
				false
			} else if self.peek_keyword() == Some(Keyword::Asc) {
				self.bump();
				true
			} else {
				true
			};
			let span = expr.span();
			items.push(OrderByExpr {
				expr,
				asc,
				span,
			});
			if self.peek().kind == TokenKind::Comma {
				self.bump();
			} else {
				break;
			}
		}
		Ok(items)
	}

	pub(crate) fn parse_table_references(&mut self) -> ParseResult<TableRef> {
		let mut left = self.parse_table_factor()?;

		loop {
			let join_type = if self.peek_keyword() == Some(Keyword::Inner) {
				self.bump();
				self.expect_keyword(Keyword::Join, "JOIN after INNER")?;
				JoinType::Inner
			} else if self.peek_keyword() == Some(Keyword::Left) {
				self.bump();
				if self.peek_keyword() == Some(Keyword::Outer) {
					self.bump();
				}
				self.expect_keyword(Keyword::Join, "JOIN after LEFT")?;
				JoinType::Left
			} else if self.peek_keyword() == Some(Keyword::Right) {
				self.bump();
				if self.peek_keyword() == Some(Keyword::Outer) {
					self.bump();
				}
				self.expect_keyword(Keyword::Join, "JOIN after RIGHT")?;
				JoinType::Right
			} else if self.peek_keyword() == Some(Keyword::Cross) {
				self.bump();
				self.expect_keyword(Keyword::Join, "JOIN after CROSS")?;
				JoinType::Cross
			} else if self.peek_keyword() == Some(Keyword::StraightJoin) {
				self.bump();
				JoinType::Straight
			} else if self.peek_keyword() == Some(Keyword::Join) {
				self.bump();
				JoinType::Inner
			} else if self.peek().kind == TokenKind::Comma {
				self.bump();
				JoinType::Cross
			} else {
				break;
			};

			let right = self.parse_table_factor()?;

			let condition = if self.peek_keyword() == Some(Keyword::On) {
				self.bump();
				Some(JoinCondition::On(self.parse_expr()?))
			} else if self.peek_keyword() == Some(Keyword::Using) {
				self.bump();
				self.expect(&TokenKind::LParen, "'(' after USING")?;
				let mut cols = Vec::new();
				loop {
					cols.push(self.parse_ident()?.0);
					if self.peek().kind == TokenKind::Comma {
						self.bump();
					} else {
						break;
					}
				}
				self.expect(&TokenKind::RParen, "')' after USING column list")?;
				Some(JoinCondition::Using(cols))
			} else {
				None
			};

			let span = match (&left, &right) {
				(
					TableRef::Table {
						span: s1,
						..
					},
					TableRef::Table {
						span: s2,
						..
					},
				) => s1.merge(*s2),
				_ => left.span(),
			};

			left = TableRef::Join {
				left: Box::new(left),
				join_type,
				right: Box::new(right),
				condition,
				span,
			};
		}

		Ok(left)
	}

	fn parse_table_factor(&mut self) -> ParseResult<TableRef> {
		if self.peek().kind == TokenKind::LParen {
			let start = self.bump().span;
			let sub = self.parse_select_stmt()?;
			self.expect(&TokenKind::RParen, "')' closing subquery in FROM")?;
			let alias = if self.peek_keyword() == Some(Keyword::As) {
				self.bump();
				self.parse_ident()?.0
			} else if matches!(self.peek().kind, TokenKind::Ident(_))
				&& self.peek_keyword().is_none()
			{
				self.parse_ident()?.0
			} else {
				return Err(ParseError::new(
					self.peek().span,
					ErrorKind::UnexpectedToken {
						expected: "subquery alias in FROM clause",
						found: self.peek().kind.to_string(),
					},
				));
			};
			let end = self.peek().span;
			Ok(TableRef::Subquery {
				subquery: Box::new(sub),
				alias,
				span: start.merge(end),
			})
		} else {
			let name = self.parse_qualified_name()?;
			let alias = if self.peek_keyword() == Some(Keyword::As) {
				self.bump();
				Some(self.parse_ident()?.0)
			} else if matches!(self.peek().kind, TokenKind::Ident(_))
				&& self.peek_keyword().is_none()
			{
				Some(self.parse_ident()?.0)
			} else {
				None
			};
			let span = name.span;
			Ok(TableRef::Table {
				name,
				alias,
				span,
			})
		}
	}

	fn parse_lock_clause(&mut self) -> ParseResult<Option<LockClause>> {
		if self.peek_keyword() == Some(Keyword::For) {
			let start = self.bump().span;
			let lock_type = if self.peek_keyword() == Some(Keyword::Update) {
				self.bump();
				LockType::ForUpdate
			} else if self.peek_keyword() == Some(Keyword::Share) {
				self.bump();
				LockType::ForShare
			} else {
				return Err(ParseError::new(
					self.peek().span,
					ErrorKind::UnexpectedToken {
						expected: "UPDATE or SHARE after FOR",
						found: self.peek().kind.to_string(),
					},
				));
			};

			let mut nowait = false;
			let mut skip_locked = false;
			let mut end = start;

			if self.peek_keyword() == Some(Keyword::Nowait) {
				end = self.bump().span;
				nowait = true;
			} else if self.peek_keyword() == Some(Keyword::Skip) {
				self.bump();
				end = self.expect_keyword(Keyword::Locked, "LOCKED after SKIP")?.span;
				skip_locked = true;
			}

			Ok(Some(LockClause {
				lock_type,
				nowait,
				skip_locked,
				span: start.merge(end),
			}))
		} else if self.peek_keyword() == Some(Keyword::Lock)
			&& self.peek_nth_keyword(1) == Some(Keyword::In)
			&& self.peek_nth_keyword(2) == Some(Keyword::Share)
			&& self.peek_nth_keyword(3) == Some(Keyword::Mode)
		{
			let start = self.bump().span; // LOCK
			self.bump(); // IN
			self.bump(); // SHARE
			let end = self.bump().span; // MODE
			Ok(Some(LockClause {
				lock_type: LockType::LockInShareMode,
				nowait: false,
				skip_locked: false,
				span: start.merge(end),
			}))
		} else {
			Ok(None)
		}
	}
}

impl TableRef {
	pub fn span(&self) -> crate::lexer::span::Span {
		match self {
			Self::Table {
				span,
				..
			} => *span,
			Self::Subquery {
				span,
				..
			} => *span,
			Self::Join {
				span,
				..
			} => *span,
		}
	}
}
