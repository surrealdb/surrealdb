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
		let mut root = self.parse_select_single()?;
		let mut set_depth: u32 = 0;

		// Iterative loop over set operations: UNION, INTERSECT, EXCEPT
		let mut current = &mut root;
		loop {
			let set_op_kind = if self.peek().kind == TokenKind::Keyword(Keyword::Union) {
				Some(SetOpKind::Union)
			} else if self.peek().kind == TokenKind::Keyword(Keyword::Intersect) {
				Some(SetOpKind::Intersect)
			} else if self.peek().kind == TokenKind::Keyword(Keyword::Except) {
				Some(SetOpKind::Except)
			} else {
				None
			};

			let Some(op) = set_op_kind else {
				break;
			};
			if current.set_op.is_some() {
				return Err(ParseError::new(
					self.peek().span,
					ErrorKind::InvalidStatement(
						"a set operation cannot be the left operand of another set operation",
					),
				));
			}

			self.enter_recursion(self.peek().span)?;
			set_depth = set_depth.saturating_add(1);

			self.bump()?; // consume UNION / INTERSECT / EXCEPT
			let all = self.eat(&TokenKind::Keyword(Keyword::All))?;
			let right = self.parse_select_single()?;
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
			if self.eat(&TokenKind::Keyword(Keyword::Order))? {
				self.expect(&TokenKind::Keyword(Keyword::By), "'BY'")?;
				root.order_by = self.parse_order_by_clause()?;
			}
			loop {
				if root.limit.is_none() && self.eat(&TokenKind::Keyword(Keyword::Limit))? {
					root.limit = Some(self.parse_expr()?);
					continue;
				}
				if root.offset.is_none() && self.eat(&TokenKind::Keyword(Keyword::Offset))? {
					root.offset = Some(self.parse_expr()?);
					if self.peek().kind == TokenKind::Keyword(Keyword::Row)
						|| self.peek().kind == TokenKind::Keyword(Keyword::Rows)
					{
						self.bump()?;
					}
					continue;
				}
				break;
			}

			// If outer order_by, limit or offset was not parsed separately, hoist from rightmost
			// child
			if root.order_by.is_empty() && root.limit.is_none() && root.offset.is_none() {
				let mut curr = &mut root.set_op;
				let mut hoisted_order = None;
				let mut hoisted_limit = None;
				let mut hoisted_offset = None;
				while let Some(so) = curr {
					if so.right.set_op.is_none() {
						hoisted_order = Some(std::mem::take(&mut so.right.order_by));
						hoisted_limit = so.right.limit.take();
						hoisted_offset = so.right.offset.take();
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
				if hoisted_offset.is_some() {
					root.offset = hoisted_offset;
				}
			}
		}

		self.leave_recursion_by(1 + set_depth);
		Ok(root)
	}

	fn parse_select_single(&mut self) -> ParseResult<SelectStmt> {
		if self.eat(&TokenKind::LParen)? {
			let stmt = self.parse_select_stmt()?;
			self.expect(&TokenKind::RParen, "')'")?;
			return Ok(stmt);
		}

		if self.eat(&TokenKind::Keyword(Keyword::Values))? {
			let mut rows = Vec::new();
			loop {
				self.expect(&TokenKind::LParen, "'('")?;
				let mut row = Vec::new();
				while self.peek().kind != TokenKind::RParen {
					row.push(self.parse_expr()?);
					if !self.eat(&TokenKind::Comma)? {
						break;
					}
				}
				self.expect(&TokenKind::RParen, "')'")?;
				if row.is_empty() {
					return Err(ParseError::new(
						self.peek().span,
						ErrorKind::InvalidStatement("a VALUES row must have at least one column"),
					));
				}
				if rows.first().is_some_and(|first: &Vec<Expr>| first.len() != row.len()) {
					return Err(ParseError::new(
						self.peek().span,
						ErrorKind::InvalidStatement("VALUES lists must all be the same length"),
					));
				}
				rows.push(row);
				if !self.eat(&TokenKind::Comma)? {
					break;
				}
			}
			// The rows become a chain of UNION ALL that every later stage walks one level per row,
			// so the number of rows is held to the recursion limit.
			let chained = rows.len().saturating_sub(1);
			for _ in 0..chained {
				self.enter_recursion(self.peek().span)?;
			}
			self.leave_recursion_by(u32::try_from(chained).unwrap_or(u32::MAX));
			let mut root: Option<SelectStmt> = None;
			for (row_idx, row) in rows.into_iter().enumerate().rev() {
				let targets = row
					.into_iter()
					.enumerate()
					.map(|(col_idx, expr)| ResTarget {
						expr,
						alias: if row_idx == 0 {
							Some(format!("column{}", col_idx + 1))
						} else {
							None
						},
						span: Span::empty(0),
					})
					.collect();
				// Built from the last row back, so each row takes the rest of the chain as its
				// right operand without walking it.
				let set_op = root.take().map(|rest| SetOperation {
					op: SetOpKind::Union,
					all: true,
					right: Box::new(rest),
				});
				root = Some(SelectStmt {
					distinct: None,
					targets,
					from: vec![],
					where_clause: None,
					group_by: vec![],
					having: None,
					window: vec![],
					order_by: vec![],
					limit: None,
					offset: None,
					locking: vec![],
					set_op,
					with: None,
					span: Span::empty(0),
				});
			}
			return Ok(root.unwrap_or_else(|| SelectStmt {
				distinct: None,
				targets: vec![],
				from: vec![],
				where_clause: None,
				group_by: vec![],
				having: None,
				window: vec![],
				order_by: vec![],
				limit: None,
				offset: None,
				locking: vec![],
				set_op: None,
				with: None,
				span: Span::empty(0),
			}));
		}

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
		let targets = if self.peek().kind == TokenKind::Keyword(Keyword::From)
			|| self.peek().kind == TokenKind::Semi
			|| self.peek().kind == TokenKind::RParen
			|| self.peek().is_eof()
		{
			Vec::new()
		} else {
			self.parse_select_list()?
		};

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

		let stmt = SelectStmt {
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

		Ok(stmt)
	}

	/// Parse projection target list (`col AS alias, *`).
	pub fn parse_target_list(&mut self) -> ParseResult<Vec<ResTarget>> {
		self.parse_target_items(false)
	}

	/// Parse the projection list of a `SELECT`: a target list in which an item may also be the `*`
	/// of a relation, `t.*` or `schema.t.*`, as a [`ColumnRef`] whose last field is `*`.
	///
	/// gram.y: target_list, target_el
	///
	/// gram.y reads `.*` as part of any column reference, but only a whole item of a select list
	/// names the columns of a relation. A `.*` anywhere else, such as an operand or a function
	/// argument, is not read here and fails as an expression does. `RETURNING` lists do not
	/// accept it either.
	fn parse_select_list(&mut self) -> ParseResult<Vec<ResTarget>> {
		self.parse_target_items(true)
	}

	/// Parse the `*` of a relation that begins the next projection item: `name.*` or
	/// `schema.name.*`, where each name is an identifier as [`Parser::parse_ident`] reads it.
	///
	/// gram.y: columnref, indirection_el (`'.' '*'`)
	///
	/// Returns `None`, with the parser where it was, when the item begins otherwise. Whether it
	/// begins so is decided on a copy of the parser: the copy ends the search at the first token
	/// that does not continue the pattern, and the error it meets on the way is met again, in the
	/// same place, by the expression parser that then reads the item.
	fn parse_relation_star(&mut self) -> ParseResult<Option<ColumnRef>> {
		if !matches!(
			self.peek().kind,
			TokenKind::Ident(_) | TokenKind::QuotedIdent(_) | TokenKind::Keyword(_)
		) || self.peek2()?.kind != TokenKind::Dot
		{
			return Ok(None);
		}
		let mut probe = self.clone();
		let start = probe.peek().span;
		let mut fields = Vec::new();
		loop {
			let Ok((name, _)) = probe.parse_ident() else {
				return Ok(None);
			};
			fields.push(name);
			if !matches!(probe.eat(&TokenKind::Dot), Ok(true)) {
				return Ok(None);
			}
			if probe.peek().kind == TokenKind::Operator("*") {
				let star = probe.bump()?;
				fields.push("*".to_string());
				*self = probe;
				return Ok(Some(ColumnRef {
					fields,
					span: start.merge(star.span),
				}));
			}
		}
	}

	/// Parse the items of a projection list, which may include the `*` of a relation when
	/// `relation_stars` says so.
	fn parse_target_items(&mut self, relation_stars: bool) -> ParseResult<Vec<ResTarget>> {
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
				let relation_star = if relation_stars {
					self.parse_relation_star()?
				} else {
					None
				};
				let expr = match relation_star {
					Some(star) => Expr::ColumnRef(star),
					None => self.parse_expr()?,
				};
				let mut span = expr.span();

				// Optional AS alias or bare identifier alias
				let has_as = self.eat(&TokenKind::Keyword(Keyword::As))?;
				let alias = if has_as {
					let (name, alias_span) = self.parse_col_label()?;
					span = span.merge(alias_span);
					Some(name)
				} else if self.is_table_or_col_alias_start() {
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
	///
	/// gram.y: from_clause, from_list
	///
	/// The binder joins the listed references one after another, so each reference after the
	/// first is a level of the tree later stages walk and counts against the recursion limit
	/// until the list is complete.
	pub fn parse_from_clause(&mut self) -> ParseResult<Vec<TableRef>> {
		let mut refs = Vec::new();
		let mut list_depth: u32 = 0;

		loop {
			let table_ref = self.parse_table_ref()?;
			refs.push(table_ref);
			if !self.eat(&TokenKind::Comma)? {
				break;
			}
			self.enter_recursion(self.peek().span)?;
			list_depth += 1;
		}

		self.leave_recursion_by(list_depth);
		Ok(refs)
	}

	/// Parse a single table reference factor (table, subquery, GRAPH_TABLE, JSON_TABLE,
	/// parenthesized join).
	/// gram.y: table_ref, func_table, func_alias_clause.
	pub fn parse_table_factor(&mut self) -> ParseResult<TableRef> {
		let lateral = self.eat(&TokenKind::Keyword(Keyword::Lateral))?;
		if lateral && self.peek().kind != TokenKind::LParen {
			return Err(ParseError::new(
				self.peek().span,
				ErrorKind::InvalidStatement("LATERAL is supported on a subquery only"),
			));
		}
		if self.eat(&TokenKind::LParen)? {
			if matches!(self.peek().kind, TokenKind::Keyword(Keyword::Select | Keyword::Values)) {
				// Subquery: `(SELECT ...) AS alias`, or a list of rows: `(VALUES (...), ...) AS
				// alias`
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
					lateral,
					query: Box::new(query),
					alias,
					span,
				})
			} else {
				if lateral {
					return Err(ParseError::new(
						self.peek().span,
						ErrorKind::InvalidStatement("LATERAL is supported on a subquery only"),
					));
				}
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
		} else if matches!(
			self.peek().kind,
			TokenKind::Keyword(
				Keyword::CurrentSchema
					| Keyword::CurrentDate
					| Keyword::CurrentTime
					| Keyword::CurrentTimestamp
					| Keyword::CurrentUser
					| Keyword::SessionUser
					| Keyword::CurrentRole
					| Keyword::CurrentCatalog
					| Keyword::Localtime
					| Keyword::Localtimestamp
					| Keyword::User
			)
		) {
			let function = self.parse_primary_expr()?;
			self.parse_function_table_tail(function)
		} else {
			let name = self.parse_qualified_name()?;
			if self.peek().kind == TokenKind::LParen {
				let function = self.parse_ident_or_func(name)?;
				return self.parse_function_table_tail(function);
			}
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

	/// gram.y: func_table -> func_expr_windowless opt_ordinality; table_ref attaches
	/// func_alias_clause.
	fn parse_function_table_tail(&mut self, function: Expr) -> ParseResult<TableRef> {
		if !matches!(
			&function,
			Expr::FunctionCall {
				over: None,
				..
			}
		) {
			return Err(ParseError::new(
				function.span(),
				ErrorKind::InvalidStatement("FROM requires a windowless function call"),
			));
		}
		let start = function.span();
		let mut end = start;
		let with_ordinality = if self.eat(&TokenKind::Keyword(Keyword::With))? {
			end = self.expect(&TokenKind::Keyword(Keyword::Ordinality), "ORDINALITY")?.span;
			true
		} else {
			false
		};
		let alias = self.parse_table_alias()?;
		if let Some(alias) = &alias {
			end = alias.span;
		}
		Ok(TableRef::Function {
			function: Box::new(function),
			alias,
			with_ordinality,
			span: start.merge(end),
		})
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
	///
	/// gram.y: table_ref, joined_table
	///
	/// The reference counts one level against the recursion limit, and so does each join chained
	/// onto it, until the reference is complete. A parenthesised reference holds a reference of
	/// its own, so it nests as deep as it is written; each join is a level of the tree later
	/// stages walk.
	pub fn parse_table_ref(&mut self) -> ParseResult<TableRef> {
		self.enter_recursion(self.peek().span)?;
		let mut join_depth: u32 = 1;
		let mut left = self.parse_table_factor()?;

		// Parse joined relations
		while let Some(join_type) = self.peek_join_type()? {
			self.enter_recursion(self.peek().span)?;
			join_depth += 1;
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

		self.leave_recursion_by(join_depth);
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
		if matches!(self.peek().kind, TokenKind::Keyword(Keyword::Join | Keyword::Inner)) {
			Ok(Some(JoinType::Inner))
		} else if self.peek().kind == TokenKind::Keyword(Keyword::Left) {
			Ok(Some(JoinType::Left))
		} else if self.peek().kind == TokenKind::Keyword(Keyword::Right) {
			Ok(Some(JoinType::Right))
		} else if self.peek().kind == TokenKind::Keyword(Keyword::Full) {
			Ok(Some(JoinType::Full))
		} else if self.peek().kind == TokenKind::Keyword(Keyword::Cross) {
			Ok(Some(JoinType::Cross))
		} else {
			Ok(None)
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
		if matches!(self.peek().kind, TokenKind::Ident(_) | TokenKind::QuotedIdent(_)) {
			true
		} else if let TokenKind::Keyword(kw) = &self.peek().kind {
			if kw.category() == KeywordCategory::Reserved {
				return false;
			}
			// Reject keywords that start other clauses
			!matches!(
				kw,
				Keyword::From
					| Keyword::Where
					| Keyword::Group
					| Keyword::Having
					| Keyword::Window
					| Keyword::Order
					| Keyword::Limit
					| Keyword::Offset
					| Keyword::Union
					| Keyword::Intersect
					| Keyword::Except
					| Keyword::Join | Keyword::Inner
					| Keyword::Left | Keyword::Right
					| Keyword::Full | Keyword::Cross
					| Keyword::On | Keyword::Using
					| Keyword::Set | Keyword::Values
					| Keyword::Returning
					| Keyword::For
			)
		} else {
			false
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
			| Self::Function {
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
