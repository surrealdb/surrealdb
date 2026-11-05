//! SELECT statement parsing for MySQL.

use crate::ast::stmt::{
	CommonTableExpr, IndexHint, IndexHintType, JoinCondition, JoinType, LimitClause, LockClause,
	LockType, OrderByExpr, SelectItem, SelectStmt, SetOpKind, SetOperation, TableRef, WindowDef,
	WithClause,
};
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;
use crate::parser::error::{ErrorKind, ParseError, ParseResult};

impl<'a> Parser<'a> {
	/// Parse a SELECT statement.
	pub fn parse_select_stmt(&mut self) -> ParseResult<SelectStmt> {
		self.check_recursion_limit()?;
		self.recursion_depth += 1;
		let with = if self.peek_keyword() == Some(Keyword::With) {
			Some(self.parse_with_clause()?)
		} else {
			None
		};
		let mut root = self.parse_select_stmt_inner()?;
		if with.is_some() {
			root.with = with;
		}

		let mut set_depth: usize = 0;
		let mut current = &mut root;
		loop {
			let set_op_kind = match self.peek_keyword() {
				Some(Keyword::Union) => Some(SetOpKind::Union),
				Some(Keyword::Intersect) => Some(SetOpKind::Intersect),
				Some(Keyword::Except) => Some(SetOpKind::Except),
				_ => None,
			};
			let Some(op) = set_op_kind else {
				break;
			};
			if current.set_op.is_some() {
				return Err(ParseError::new(
					self.peek().span,
					ErrorKind::Custom(
						"a set operation cannot be the left operand of another set operation"
							.to_string(),
					),
				));
			}
			// Every link of a chain nests one level deeper in the tree later stages walk.
			self.check_recursion_limit()?;
			self.recursion_depth += 1;
			set_depth += 1;
			self.bump();
			let all = if self.peek_keyword() == Some(Keyword::All) {
				self.bump();
				true
			} else if self.peek_keyword() == Some(Keyword::Distinct) {
				self.bump();
				false
			} else {
				false
			};
			let right = self.parse_select_stmt_inner()?;
			current.span = current.span.merge(right.span);
			current.set_op = Some(SetOperation {
				op,
				all,
				right: Box::new(right),
			});
			if let Some(ref mut so) = current.set_op {
				current = so.right.as_mut();
			} else {
				break;
			}
		}

		// Optional outer ORDER BY and LIMIT after set operations
		if root.set_op.is_some() {
			if self.peek_keyword() == Some(Keyword::Order) {
				self.bump();
				self.expect_keyword(Keyword::By, "BY after ORDER")?;
				root.order_by = self.parse_order_by_list()?;
			}
			if self.peek_keyword() == Some(Keyword::Limit) {
				let limit_start = self.bump().span;
				let first = self.parse_expr()?;
				if self.peek().kind == TokenKind::Comma {
					self.bump();
					let second = self.parse_expr()?;
					let span = limit_start.merge(second.span());
					root.limit = Some(LimitClause {
						offset: Some(first),
						count: second,
						span,
					});
				} else if self.peek_keyword() == Some(Keyword::Offset) {
					self.bump();
					let off = self.parse_expr()?;
					let span = limit_start.merge(off.span());
					root.limit = Some(LimitClause {
						offset: Some(off),
						count: first,
						span,
					});
				} else {
					let span = limit_start.merge(first.span());
					root.limit = Some(LimitClause {
						offset: None,
						count: first,
						span,
					});
				}
			}

			// If outer order_by or limit was not parsed separately, hoist from rightmost child
			if root.order_by.is_empty() && root.limit.is_none() {
				let mut curr = &mut root.set_op;
				let mut hoisted_order = None;
				let mut hoisted_limit = None;
				while let Some(so) = curr {
					if so.right.set_op.is_none() {
						hoisted_order = Some(std::mem::take(&mut so.right.order_by));
						hoisted_limit = so.right.limit.take();
						break;
					}
					curr = &mut so.right.set_op;
				}
				if let Some(o) = hoisted_order {
					root.order_by = o;
				}
				if hoisted_limit.is_some() {
					root.limit = hoisted_limit;
				}
			}
		}

		self.recursion_depth -= 1 + set_depth;
		Ok(root)
	}

	fn parse_select_stmt_inner(&mut self) -> ParseResult<SelectStmt> {
		if self.peek().kind == TokenKind::LParen {
			self.bump();
			let stmt = self.parse_select_stmt()?;
			self.expect(&TokenKind::RParen, ")")?;
			return Ok(stmt);
		}
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

		// Optimizer and cache hints, which change no result. A hint is a reserved word, so one
		// followed by a separator is a quoted identifier of that name instead.
		while [
			"SQL_NO_CACHE",
			"SQL_CACHE",
			"SQL_SMALL_RESULT",
			"SQL_BIG_RESULT",
			"SQL_BUFFER_RESULT",
			"HIGH_PRIORITY",
		]
		.iter()
		.any(|word| self.peek_is_word(word))
			&& !matches!(self.peek_nth(1).kind, TokenKind::Comma | TokenKind::Dot)
			&& self.peek_nth_keyword(1) != Some(Keyword::From)
		{
			self.bump();
		}

		// Projection list
		let mut projection = Vec::new();
		loop {
			if self.peek().kind == TokenKind::Star {
				let s = self.bump().span;
				projection.push(SelectItem::Wildcard(s));
			} else if matches!(self.peek().kind, TokenKind::Ident(_) | TokenKind::Keyword(_))
				&& self.peek_nth(1).kind == TokenKind::Dot
				&& self.peek_nth(2).kind == TokenKind::Star
			{
				// `t.*`; a quoted `*` is an identifier, and `t.`*`` is a column like any other.
				let (table, start) = self.parse_ident()?;
				self.bump(); // '.'
				let end = self.bump().span;
				projection.push(SelectItem::QualifiedWildcard(vec![table], start.merge(end)));
			} else if matches!(self.peek().kind, TokenKind::Ident(_) | TokenKind::Keyword(_))
				&& self.peek_nth(1).kind == TokenKind::Dot
				&& matches!(self.peek_nth(2).kind, TokenKind::Ident(_) | TokenKind::Keyword(_))
				&& self.peek_nth(3).kind == TokenKind::Dot
				&& self.peek_nth(4).kind == TokenKind::Star
			{
				// `db.t.*`
				let (database, start) = self.parse_ident()?;
				self.bump(); // '.'
				let (table, _) = self.parse_ident()?;
				self.bump(); // '.'
				let end = self.bump().span;
				projection
					.push(SelectItem::QualifiedWildcard(vec![database, table], start.merge(end)));
			} else {
				let first = self.peek().span;
				let expr = self.parse_expr()?;
				// The tokens of the item, parentheses included, which the spans inside `expr`
				// leave out.
				let span = first.merge(self.last_span());
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
			if self.peek_keyword() == Some(Keyword::Dual) && self.peek_nth(1).kind != TokenKind::Dot
			{
				// The unquoted keyword DUAL names no table: `FROM DUAL` is a SELECT with no
				// FROM clause. A quoted or schema-qualified `dual` stays an ordinary table.
				self.bump();
				None
			} else {
				Some(self.parse_table_references()?)
			}
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
		let (group_by, with_rollup) = if self.peek_keyword() == Some(Keyword::Group) {
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
			let rollup = if self.peek_keyword() == Some(Keyword::With)
				&& self.peek_nth_keyword(1) == Some(Keyword::Rollup)
			{
				self.bump();
				self.bump();
				true
			} else {
				false
			};
			(items, rollup)
		} else {
			(Vec::new(), false)
		};

		// HAVING clause
		let having = if self.peek_keyword() == Some(Keyword::Having) {
			self.bump(); // HAVING
			Some(self.parse_expr()?)
		} else {
			None
		};

		// WINDOW clause
		let window = if self.eat_keyword(Keyword::Window) {
			let mut windows = Vec::new();
			loop {
				let start_span = self.peek().span;
				let (name, _) = self.parse_ident()?;
				self.expect_keyword(Keyword::As, "'AS' after window name in WINDOW clause")?;
				let spec = self.parse_window_spec()?;
				let span = start_span.merge(spec.span);
				windows.push(WindowDef {
					name,
					spec,
					span,
				});
				if !self.eat(&TokenKind::Comma) {
					break;
				}
			}
			windows
		} else {
			Vec::new()
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
			with: None,
			distinct,
			projection,
			from,
			where_clause,
			group_by,
			with_rollup,
			having,
			window,
			order_by,
			limit,
			lock,
			set_op: None,
			span,
		})
	}

	/// Parse a `WITH [RECURSIVE] cte_name [(cols...)] AS (query) [, ...]` clause.
	pub fn parse_with_clause(&mut self) -> ParseResult<WithClause> {
		let with_tok = self.expect_keyword(Keyword::With, "WITH")?;
		let recursive = if self.peek_keyword() == Some(Keyword::Recursive) {
			self.bump();
			true
		} else {
			false
		};

		let mut ctes = Vec::new();
		loop {
			let (name, name_span) = self.parse_ident()?;
			let mut columns = Vec::new();

			if self.peek().kind == TokenKind::LParen {
				self.bump();
				loop {
					let (col, _) = self.parse_ident()?;
					columns.push(col);
					if self.peek().kind == TokenKind::Comma {
						self.bump();
					} else {
						break;
					}
				}
				self.expect(&TokenKind::RParen, ")")?;
			}

			self.expect_keyword(Keyword::As, "AS")?;
			self.expect(&TokenKind::LParen, "(")?;
			let query = self.parse_select_stmt()?;
			let rparen = self.expect(&TokenKind::RParen, ")")?;

			let span = name_span.merge(rparen.span);
			ctes.push(CommonTableExpr {
				name,
				columns,
				query: Box::new(query),
				span,
			});

			if self.peek().kind == TokenKind::Comma {
				self.bump();
			} else {
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
		let mut join_depth = 0;

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

			self.check_recursion_limit()?;
			self.recursion_depth += 1;
			join_depth += 1;

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

		self.recursion_depth = self.recursion_depth.saturating_sub(join_depth);
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

			let mut index_hints = Vec::new();
			while let Some(hint_type) = match self.peek_keyword() {
				Some(Keyword::Use) => Some(IndexHintType::Use),
				Some(Keyword::Force) => Some(IndexHintType::Force),
				Some(Keyword::Ignore) => Some(IndexHintType::Ignore),
				_ => None,
			} {
				let hint_start = self.bump().span;
				if self.peek_keyword() == Some(Keyword::Index)
					|| self.peek_keyword() == Some(Keyword::Key)
				{
					self.bump();
				}
				if self.peek_keyword() == Some(Keyword::For) {
					self.bump();
					if self.peek_keyword() == Some(Keyword::Join) {
						self.bump();
					} else if matches!(self.peek_keyword(), Some(Keyword::Order | Keyword::Group)) {
						self.bump();
						if self.peek_keyword() == Some(Keyword::By) {
							self.bump();
						}
					}
				}
				self.expect(&TokenKind::LParen, "'(' after index hint")?;
				let mut indexes = Vec::new();
				loop {
					if self.peek().kind == TokenKind::RParen {
						break;
					}
					indexes.push(self.parse_ident()?.0);
					if self.peek().kind == TokenKind::Comma {
						self.bump();
					} else {
						break;
					}
				}
				let rparen = self.expect(&TokenKind::RParen, "')' after index hint list")?;
				index_hints.push(IndexHint {
					hint_type,
					indexes,
					span: hint_start.merge(rparen.span),
				});
			}

			let mut span = name.span;
			if let Some(last_hint) = index_hints.last() {
				span = span.merge(last_hint.span);
			}
			Ok(TableRef::Table {
				name,
				alias,
				index_hints,
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
