//! PostgreSQL `SELECT` statement parser.
//!
//! gram.y: SelectStmt

use crate::ast::expr::{ColumnRef, Expr};
use crate::ast::stmt::{
	DistinctClause, JoinCondition, JoinType, LockingClause, LockingStrength, LockingWaitPolicy,
	ResTarget, SelectStmt, SetOpKind, SetOperation, TableAlias, TableRef, WindowDef,
};
use crate::error::{ErrorKind, ParseError, ParseResult};
use crate::lexer::keyword::{Keyword, KeywordCategory};
use crate::lexer::token::TokenKind;
use crate::parser::Parser;
use crate::span::Span;

impl<'a> Parser<'a> {
	/// Parse a PostgreSQL `SELECT` statement.
	///
	/// gram.y: SelectStmt
	pub fn parse_select_stmt(&mut self) -> ParseResult<SelectStmt> {
		self.enter_recursion(self.peek().span)?;
		let res = self.parse_select_stmt_inner();
		self.leave_recursion();
		res
	}

	fn parse_select_stmt_inner(&mut self) -> ParseResult<SelectStmt> {
		let with = if self.peek().kind == TokenKind::Keyword(Keyword::With) {
			Some(self.parse_with_clause()?)
		} else {
			None
		};

		let start_tok = self.expect(&TokenKind::Keyword(Keyword::Select), "'SELECT'")?;
		let start_span = with.as_ref().map(|w| w.span).unwrap_or(start_tok.span);

		// 1. DISTINCT or DISTINCT ON (expr, ...)
		let distinct = if self.eat(&TokenKind::Keyword(Keyword::Distinct))? {
			if self.eat(&TokenKind::Keyword(Keyword::On))? {
				self.expect(&TokenKind::LParen, "'('")?;
				let mut on_exprs = Vec::new();
				while self.peek().kind != TokenKind::RParen {
					on_exprs.push(self.parse_expr()?);
					if !self.eat(&TokenKind::Comma)? {
						break;
					}
				}
				self.expect(&TokenKind::RParen, "')'")?;
				Some(DistinctClause::On(on_exprs))
			} else {
				Some(DistinctClause::All)
			}
		} else {
			self.eat(&TokenKind::Keyword(Keyword::All))?;
			None
		};

		// 2. Target list
		let targets = self.parse_target_list()?;

		// 3. Optional FROM clause
		let from = if self.eat(&TokenKind::Keyword(Keyword::From))? {
			self.parse_from_clause()?
		} else {
			Vec::new()
		};

		// 4. Optional WHERE clause
		let where_clause = if self.eat(&TokenKind::Keyword(Keyword::Where))? {
			Some(self.parse_expr()?)
		} else {
			None
		};

		// 5. Optional GROUP BY clause
		let group_by = if self.eat(&TokenKind::Keyword(Keyword::Group))? {
			self.expect(&TokenKind::Keyword(Keyword::By), "'BY'")?;
			let mut groups = Vec::new();
			loop {
				groups.push(self.parse_expr()?);
				if !self.eat(&TokenKind::Comma)? {
					break;
				}
			}
			groups
		} else {
			Vec::new()
		};

		// 6. Optional HAVING clause
		let having = if self.eat(&TokenKind::Keyword(Keyword::Having))? {
			Some(self.parse_expr()?)
		} else {
			None
		};

		// 7. Optional WINDOW clause
		let window = if self.eat(&TokenKind::Keyword(Keyword::Window))? {
			let mut windows = Vec::new();
			loop {
				let win_tok = self.peek().clone();
				let (name, _) = self.parse_ident()?;
				self.expect(&TokenKind::Keyword(Keyword::As), "'AS'")?;
				let spec = self.parse_window_spec()?;
				let span = win_tok.span.merge(spec.span);
				windows.push(WindowDef {
					name,
					spec,
					span,
				});
				if !self.eat(&TokenKind::Comma)? {
					break;
				}
			}
			windows
		} else {
			Vec::new()
		};

		// 8. Optional ORDER BY clause
		let order_by = if self.eat(&TokenKind::Keyword(Keyword::Order))? {
			self.expect(&TokenKind::Keyword(Keyword::By), "'BY'")?;
			self.parse_order_by_clause()?
		} else {
			Vec::new()
		};

		// 9. Optional LIMIT and OFFSET clauses (can appear in either order in PG)
		let mut limit = None;
		let mut offset = None;

		loop {
			if limit.is_none() && self.eat(&TokenKind::Keyword(Keyword::Limit))? {
				limit = Some(self.parse_expr()?);
				continue;
			}
			if offset.is_none() && self.eat(&TokenKind::Keyword(Keyword::Offset))? {
				offset = Some(self.parse_expr()?);
				// Optional trailing 'ROW' or 'ROWS' in PG standard
				if self.peek().kind == TokenKind::Keyword(Keyword::Row)
					|| self.peek().kind == TokenKind::Keyword(Keyword::Rows)
				{
					self.bump()?;
				}
				continue;
			}
			break;
		}

		// 10. Optional FOR UPDATE / SHARE locking clauses
		let mut locking = Vec::new();
		while self.eat(&TokenKind::Keyword(Keyword::For))? {
			let for_tok = self.peek().clone();
			let strength = if self.eat(&TokenKind::Keyword(Keyword::Update))? {
				LockingStrength::Update
			} else if self.eat(&TokenKind::Keyword(Keyword::Share))? {
				LockingStrength::Share
			} else if self.eat(&TokenKind::Keyword(Keyword::No))? {
				self.expect(&TokenKind::Keyword(Keyword::Key), "'KEY'")?;
				self.expect(&TokenKind::Keyword(Keyword::Update), "'UPDATE'")?;
				LockingStrength::NoKeyUpdate
			} else if self.eat(&TokenKind::Keyword(Keyword::Key))? {
				self.expect(&TokenKind::Keyword(Keyword::Share), "'SHARE'")?;
				LockingStrength::KeyShare
			} else {
				return Err(ParseError::new(
					self.peek().span,
					ErrorKind::UnexpectedToken {
						expected: "UPDATE, SHARE, NO KEY UPDATE, or KEY SHARE",
						found: self.peek().kind.to_string(),
					},
				));
			};

			let mut tables = Vec::new();
			if self.eat(&TokenKind::Keyword(Keyword::Of))? {
				loop {
					tables.push(self.parse_qualified_name()?);
					if !self.eat(&TokenKind::Comma)? {
						break;
					}
				}
			}

			let wait_policy = if self.eat(&TokenKind::Keyword(Keyword::Nowait))? {
				LockingWaitPolicy::NoWait
			} else if self.eat(&TokenKind::Keyword(Keyword::Skip))? {
				self.expect(&TokenKind::Keyword(Keyword::Locked), "'LOCKED'")?;
				LockingWaitPolicy::SkipLocked
			} else {
				LockingWaitPolicy::Block
			};

			let span = for_tok.span.merge(self.peek().span);
			locking.push(LockingClause {
				strength,
				tables,
				wait_policy,
				span,
			});
		}

		let end_span = self.peek().span;
		let span = start_span.merge(end_span);

		let mut stmt = SelectStmt {
			with,
			distinct,
			targets,
			from,
			where_clause,
			group_by,
			having,
			window,
			order_by,
			limit,
			offset,
			locking,
			set_op: None,
			span,
		};

		// 9. Optional Set Operations: UNION, INTERSECT, EXCEPT
		let set_op_kind = match &self.peek().kind {
			TokenKind::Keyword(Keyword::Union) => Some(SetOpKind::Union),
			TokenKind::Keyword(Keyword::Intersect) => Some(SetOpKind::Intersect),
			TokenKind::Keyword(Keyword::Except) => Some(SetOpKind::Except),
			_ => None,
		};

		if let Some(op) = set_op_kind {
			self.bump()?; // consume UNION / INTERSECT / EXCEPT
			let all = self.eat(&TokenKind::Keyword(Keyword::All))?;
			let right = self.parse_select_stmt()?;
			stmt.span = stmt.span.merge(right.span);
			stmt.set_op = Some(SetOperation {
				op,
				all,
				right: Box::new(right),
			});
		}

		Ok(stmt)
	}

	/// Parse projection target list (`col AS alias, *`).
	pub fn parse_target_list(&mut self) -> ParseResult<Vec<ResTarget>> {
		let mut targets = Vec::new();

		loop {
			// Check for bare `*`
			if self.peek().kind == TokenKind::Operator("*") {
				let star_tok = self.bump()?;
				targets.push(ResTarget {
					expr: Expr::ColumnRef(ColumnRef {
						fields: vec!["*".to_string()],
						span: star_tok.span,
					}),
					alias: None,
					span: star_tok.span,
				});
			} else {
				let expr = self.parse_expr()?;
				let mut span = expr.span();

				// Optional AS alias or bare identifier alias
				let has_as = self.eat(&TokenKind::Keyword(Keyword::As))?;
				let alias = if has_as || self.is_table_or_col_alias_start() {
					let (name, alias_span) = self.parse_ident()?;
					span = span.merge(alias_span);
					Some(name)
				} else {
					None
				};

				targets.push(ResTarget {
					expr,
					alias,
					span,
				});
			}

			if !self.eat(&TokenKind::Comma)? {
				break;
			}
		}

		Ok(targets)
	}

	/// Parse `FROM` table references.
	pub fn parse_from_clause(&mut self) -> ParseResult<Vec<TableRef>> {
		let mut refs = Vec::new();

		loop {
			let table_ref = self.parse_table_ref()?;
			refs.push(table_ref);
			if !self.eat(&TokenKind::Comma)? {
				break;
			}
		}

		Ok(refs)
	}

	/// Parse a single table reference factor (table, subquery, GRAPH_TABLE, JSON_TABLE,
	/// parenthesized join).
	pub fn parse_table_factor(&mut self) -> ParseResult<TableRef> {
		if self.eat(&TokenKind::LParen)? {
			if self.peek().kind == TokenKind::Keyword(Keyword::Select) {
				// Subquery: `(SELECT ...) AS alias`
				let query = self.parse_select_stmt()?;
				self.expect(&TokenKind::RParen, "')'")?;
				let alias = self.parse_table_alias()?.ok_or_else(|| {
					ParseError::new(
						query.span,
						ErrorKind::InvalidStatement("subquery in FROM must have an alias"),
					)
				})?;
				let span = query.span.merge(alias.span);
				Ok(TableRef::Subquery {
					query: Box::new(query),
					alias,
					span,
				})
			} else {
				// Parenthesized join: `(t1 JOIN t2 ON ...)`
				let inner = self.parse_table_ref()?;
				self.expect(&TokenKind::RParen, "')'")?;
				Ok(inner)
			}
		} else if self.peek().kind == TokenKind::Keyword(Keyword::GraphTable) {
			let tok = self.bump()?;
			self.parse_graph_table_tail(tok.span)
		} else if self.peek().kind == TokenKind::Keyword(Keyword::JsonTable) {
			let tok = self.bump()?;
			self.parse_json_table_tail(tok.span)
		} else {
			// Plain table name
			let name = self.parse_qualified_name()?;
			let alias = self.parse_table_alias()?;
			let span = if let Some(ref a) = alias {
				name.span.merge(a.span)
			} else {
				name.span
			};
			Ok(TableRef::Table {
				name,
				alias,
				span,
			})
		}
	}

	/// Parse `JSON_TABLE(...)` table factor tail.
	pub fn parse_json_table_tail(&mut self, start_span: Span) -> ParseResult<TableRef> {
		self.expect(&TokenKind::LParen, "'('")?;
		let context_item = Box::new(self.parse_expr()?);
		self.expect(&TokenKind::Comma, "','")?;
		let (path, _) = self.parse_string_constant()?;
		self.expect(&TokenKind::Keyword(Keyword::Columns), "'COLUMNS'")?;
		self.expect(&TokenKind::LParen, "'('")?;
		let mut columns = Vec::new();
		loop {
			let col_start = self.peek().span;
			let (name, _) = self.parse_ident()?;
			let data_type = self.parse_type_name()?;
			let path = if self.eat(&TokenKind::Keyword(Keyword::Path))? {
				Some(self.parse_string_constant()?.0)
			} else {
				None
			};
			let col_end = self.peek().span;
			columns.push(crate::ast::json::JsonTableColumn {
				name,
				data_type,
				path,
				span: col_start.merge(col_end),
			});
			if !self.eat(&TokenKind::Comma)? {
				break;
			}
		}
		self.expect(&TokenKind::RParen, "')'")?;
		let rparen = self.expect(&TokenKind::RParen, "')'")?;
		let alias = self.parse_table_alias()?;
		let end_span = alias.as_ref().map(|a| a.span).unwrap_or(rparen.span);

		Ok(TableRef::JsonTable(crate::ast::json::JsonTable {
			context_item,
			path,
			columns,
			alias,
			span: start_span.merge(end_span),
		}))
	}

	/// Parse a single table reference or join chain.
	pub fn parse_table_ref(&mut self) -> ParseResult<TableRef> {
		let mut left = self.parse_table_factor()?;

		// Parse joined relations
		while let Some(join_type) = self.peek_join_type()? {
			self.consume_join_type(join_type)?;
			let right = self.parse_table_factor()?;

			let condition = if join_type == JoinType::Cross {
				None
			} else if self.eat(&TokenKind::Keyword(Keyword::On))? {
				Some(JoinCondition::On(self.parse_expr()?))
			} else if self.eat(&TokenKind::Keyword(Keyword::Using))? {
				self.expect(&TokenKind::LParen, "'('")?;
				let mut cols = Vec::new();
				loop {
					let (col, _) = self.parse_ident()?;
					cols.push(col);
					if !self.eat(&TokenKind::Comma)? {
						break;
					}
				}
				self.expect(&TokenKind::RParen, "')'")?;
				Some(JoinCondition::Using(cols))
			} else {
				None
			};

			let span = left.span().merge(right.span());
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

	/// Parse optional table alias `[AS] name [(col1, col2)]`.
	pub(crate) fn parse_table_alias(&mut self) -> ParseResult<Option<TableAlias>> {
		let has_as = self.eat(&TokenKind::Keyword(Keyword::As))?;
		if has_as || self.is_table_or_col_alias_start() {
			let (name, start_span) = self.parse_ident()?;
			let mut end_span = start_span;
			let mut columns = Vec::new();

			if self.eat(&TokenKind::LParen)? {
				loop {
					let (col, _) = self.parse_ident()?;
					columns.push(col);
					if !self.eat(&TokenKind::Comma)? {
						break;
					}
				}
				let r_paren = self.expect(&TokenKind::RParen, "')'")?;
				end_span = r_paren.span;
			}

			Ok(Some(TableAlias {
				name,
				columns,
				span: start_span.merge(end_span),
			}))
		} else {
			Ok(None)
		}
	}

	/// Check if next token is a join keyword.
	fn peek_join_type(&self) -> ParseResult<Option<JoinType>> {
		match &self.peek().kind {
			TokenKind::Keyword(Keyword::Join) | TokenKind::Keyword(Keyword::Inner) => {
				Ok(Some(JoinType::Inner))
			}
			TokenKind::Keyword(Keyword::Left) => Ok(Some(JoinType::Left)),
			TokenKind::Keyword(Keyword::Right) => Ok(Some(JoinType::Right)),
			TokenKind::Keyword(Keyword::Full) => Ok(Some(JoinType::Full)),
			TokenKind::Keyword(Keyword::Cross) => Ok(Some(JoinType::Cross)),
			_ => Ok(None),
		}
	}

	/// Consume join keywords for a given join type.
	fn consume_join_type(&mut self, join_type: JoinType) -> ParseResult<()> {
		match join_type {
			JoinType::Inner => {
				self.eat(&TokenKind::Keyword(Keyword::Inner))?;
				self.expect(&TokenKind::Keyword(Keyword::Join), "'JOIN'")?;
			}
			JoinType::Left => {
				self.bump()?; // consume LEFT
				self.eat(&TokenKind::Keyword(Keyword::Outer))?;
				self.expect(&TokenKind::Keyword(Keyword::Join), "'JOIN'")?;
			}
			JoinType::Right => {
				self.bump()?; // consume RIGHT
				self.eat(&TokenKind::Keyword(Keyword::Outer))?;
				self.expect(&TokenKind::Keyword(Keyword::Join), "'JOIN'")?;
			}
			JoinType::Full => {
				self.bump()?; // consume FULL
				self.eat(&TokenKind::Keyword(Keyword::Outer))?;
				self.expect(&TokenKind::Keyword(Keyword::Join), "'JOIN'")?;
			}
			JoinType::Cross => {
				self.bump()?; // consume CROSS
				self.expect(&TokenKind::Keyword(Keyword::Join), "'JOIN'")?;
			}
		}
		Ok(())
	}

	/// Determine if current token can be an unquoted/quoted identifier alias.
	fn is_table_or_col_alias_start(&self) -> bool {
		match &self.peek().kind {
			TokenKind::Ident(_) | TokenKind::QuotedIdent(_) => true,
			TokenKind::Keyword(kw) => {
				if kw.category() == KeywordCategory::Reserved {
					return false;
				}
				// Reject keywords that start other clauses
				!matches!(
					kw,
					Keyword::From
						| Keyword::Where | Keyword::Group
						| Keyword::Having | Keyword::Window
						| Keyword::Order | Keyword::Limit
						| Keyword::Offset | Keyword::Union
						| Keyword::Intersect
						| Keyword::Except | Keyword::Join
						| Keyword::Inner | Keyword::Left
						| Keyword::Right | Keyword::Full
						| Keyword::Cross | Keyword::On
						| Keyword::Using | Keyword::Set
						| Keyword::Values | Keyword::Returning
						| Keyword::For
				)
			}
			_ => false,
		}
	}
}

impl TableRef {
	/// Return the span of the table reference.
	pub const fn span(&self) -> Span {
		match self {
			Self::Table {
				span,
				..
			}
			| Self::Subquery {
				span,
				..
			}
			| Self::Join {
				span,
				..
			}
			| Self::GraphTable {
				span,
				..
			} => *span,
			Self::JsonTable(t) => t.span,
		}
	}
}
