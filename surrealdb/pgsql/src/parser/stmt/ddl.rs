//! PostgreSQL DDL statements parser (`CREATE TABLE`, `CREATE INDEX`, `CREATE VIEW`, `CREATE
//! SCHEMA`, `CREATE EXTENSION`, `ALTER TABLE`, `DROP`).
//!
//! gram.y: CreateStmt, IndexStmt, ViewStmt, CreateSchemaStmt, CreateExtensionStmt, AlterTableStmt,
//! DropStmt

use crate::ast::expr::{NullsOrder, OrderDirection};
use crate::ast::stmt::{
	AlterTableAction, AlterTableStmt, ColumnConstraint, ColumnDef, CreateExtensionStmt,
	CreateIndexStmt, CreateSchemaStmt, CreateTableStmt, CreateViewStmt, DropObjectType, DropStmt,
	IndexElem, ReferentialAction, RelOption, Statement, TableConstraint,
};
use crate::error::{ErrorKind, ParseError, ParseResult};
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;
use crate::span::Span;

impl<'a> Parser<'a> {
	/// Parse any PostgreSQL `CREATE` statement (TABLE, INDEX, VIEW, SCHEMA).
	pub fn parse_create_stmt(&mut self) -> ParseResult<Statement> {
		let create_tok = self.expect(&TokenKind::Keyword(Keyword::Create), "'CREATE'")?;

		// OR REPLACE -> View
		let or_replace = if self.eat(&TokenKind::Keyword(Keyword::Or))? {
			self.expect(&TokenKind::Keyword(Keyword::Replace), "'REPLACE'")?;
			true
		} else {
			false
		};

		// Optional TEMPORARY / TEMP
		let temporary = self.eat(&TokenKind::Keyword(Keyword::Temporary))?
			|| self.eat(&TokenKind::Keyword(Keyword::Temp))?;

		// Optional RECURSIVE -> View
		let recursive = self.eat(&TokenKind::Keyword(Keyword::Recursive))?;

		if or_replace || recursive || self.peek().kind == TokenKind::Keyword(Keyword::View) {
			let s = self.parse_create_view_stmt_tail(
				create_tok.span,
				or_replace,
				temporary,
				recursive,
			)?;
			return Ok(Statement::CreateView(Box::new(s)));
		}

		if self.peek().kind == TokenKind::Keyword(Keyword::Unique)
			|| self.peek().kind == TokenKind::Keyword(Keyword::Index)
		{
			let unique = self.eat(&TokenKind::Keyword(Keyword::Unique))?;
			let s = self.parse_create_index_stmt_tail(create_tok.span, unique)?;
			return Ok(Statement::CreateIndex(Box::new(s)));
		}

		if self.peek().kind == TokenKind::Keyword(Keyword::Schema) {
			let s = self.parse_create_schema_stmt_tail(create_tok.span)?;
			return Ok(Statement::CreateSchema(s));
		}

		if self.peek().kind == TokenKind::Keyword(Keyword::Extension) {
			let s = self.parse_create_extension_stmt_tail(create_tok.span)?;
			return Ok(Statement::CreateExtension(s));
		}

		if self.peek().kind == TokenKind::Keyword(Keyword::Property) {
			let prop_tok = self.bump()?;
			let s =
				self.parse_create_property_graph_stmt_tail(create_tok.span.merge(prop_tok.span))?;
			return Ok(Statement::CreatePropertyGraph(Box::new(s)));
		}

		// Otherwise CREATE TABLE
		let s = self.parse_create_table_stmt_tail(create_tok.span, temporary)?;
		Ok(Statement::CreateTable(Box::new(s)))
	}

	/// Parse an optional `WITH (name [= value], ...)` list of storage parameters.
	///
	/// gram.y: opt_reloptions
	fn parse_opt_reloptions(&mut self) -> ParseResult<Vec<RelOption>> {
		if !self.eat(&TokenKind::Keyword(Keyword::With))? {
			return Ok(Vec::new());
		}
		self.expect(&TokenKind::LParen, "'('")?;
		let mut options = Vec::new();
		loop {
			let (name, name_span) = self.parse_ident()?;
			let (value, span) = if self.eat(&TokenKind::Operator("="))? {
				let value = self.parse_expr()?;
				let span = name_span.merge(value.span());
				(Some(value), span)
			} else {
				(None, name_span)
			};
			options.push(RelOption {
				name,
				value,
				span,
			});
			if !self.eat(&TokenKind::Comma)? {
				break;
			}
		}
		self.expect(&TokenKind::RParen, "')'")?;
		Ok(options)
	}

	/// Parse a PostgreSQL `CREATE TABLE` statement given an already consumed `CREATE` (and optional
	/// `TEMP`).
	pub fn parse_create_table_stmt_tail(
		&mut self,
		start_span: Span,
		temporary: bool,
	) -> ParseResult<CreateTableStmt> {
		self.expect(&TokenKind::Keyword(Keyword::Table), "'TABLE'")?;

		// Optional IF NOT EXISTS
		let if_not_exists = if self.eat(&TokenKind::Keyword(Keyword::If))? {
			self.expect(&TokenKind::Keyword(Keyword::Not), "'NOT'")?;
			self.expect(&TokenKind::Keyword(Keyword::Exists), "'EXISTS'")?;
			true
		} else {
			false
		};

		let name = self.parse_qualified_name()?;

		// Parenthesized elements (columns & table constraints)
		self.expect(&TokenKind::LParen, "'('")?;

		let mut columns = Vec::new();
		let mut constraints = Vec::new();

		loop {
			if self.peek().kind == TokenKind::Keyword(Keyword::Primary)
				|| self.peek().kind == TokenKind::Keyword(Keyword::Unique)
				|| self.peek().kind == TokenKind::Keyword(Keyword::Foreign)
			{
				constraints.push(self.parse_table_constraint_tail()?);
			} else if self.peek().kind == TokenKind::Keyword(Keyword::Constraint) {
				self.bump()?; // consume CONSTRAINT
				let (_constraint_name, _) = self.parse_ident()?;
				constraints.push(self.parse_table_constraint_tail()?);
			} else {
				columns.push(self.parse_column_def()?);
			}

			if !self.eat(&TokenKind::Comma)? {
				break;
			}
		}

		let r_paren = self.expect(&TokenKind::RParen, "')'")?;
		let span = start_span.merge(r_paren.span);

		// Storage parameters tune a heap layout the engine does not have.
		self.parse_opt_reloptions()?;

		Ok(CreateTableStmt {
			name,
			if_not_exists,
			temporary,
			columns,
			constraints,
			span,
		})
	}

	/// Parse a PostgreSQL `CREATE TABLE` statement.
	///
	/// gram.y: CreateStmt
	pub fn parse_create_table_stmt(&mut self) -> ParseResult<CreateTableStmt> {
		let create_tok = self.expect(&TokenKind::Keyword(Keyword::Create), "'CREATE'")?;
		let temporary = self.eat(&TokenKind::Keyword(Keyword::Temporary))?
			|| self.eat(&TokenKind::Keyword(Keyword::Temp))?;
		self.parse_create_table_stmt_tail(create_tok.span, temporary)
	}

	/// Parse a PostgreSQL `CREATE INDEX` statement given already consumed `CREATE [UNIQUE]`.
	fn parse_create_index_stmt_tail(
		&mut self,
		start_span: Span,
		unique: bool,
	) -> ParseResult<CreateIndexStmt> {
		self.expect(&TokenKind::Keyword(Keyword::Index), "'INDEX'")?;

		let concurrently = self.eat(&TokenKind::Keyword(Keyword::Concurrently))?;

		let if_not_exists = if self.eat(&TokenKind::Keyword(Keyword::If))? {
			self.expect(&TokenKind::Keyword(Keyword::Not), "'NOT'")?;
			self.expect(&TokenKind::Keyword(Keyword::Exists), "'EXISTS'")?;
			true
		} else {
			false
		};

		// Index name (optional in PG if omitted before ON)
		let name = if self.peek().kind != TokenKind::Keyword(Keyword::On) {
			let (n, _) = self.parse_ident()?;
			Some(n)
		} else {
			None
		};

		self.expect(&TokenKind::Keyword(Keyword::On), "'ON'")?;
		let table = self.parse_qualified_name()?;

		// Optional USING method
		let method = if self.eat(&TokenKind::Keyword(Keyword::Using))? {
			let (m, _) = self.parse_ident()?;
			Some(m)
		} else {
			None
		};

		self.expect(&TokenKind::LParen, "'('")?;
		let mut columns = Vec::new();
		loop {
			let expr = self.parse_expr()?;
			let mut span = expr.span();

			// gram.y: index_elem (opt_qualified_name)
			let opclass =
				if matches!(self.peek().kind, TokenKind::Ident(_) | TokenKind::QuotedIdent(_)) {
					Some(self.parse_qualified_name()?)
				} else {
					None
				};

			let direction = if self.eat(&TokenKind::Keyword(Keyword::Asc))? {
				Some(OrderDirection::Asc)
			} else if self.eat(&TokenKind::Keyword(Keyword::Desc))? {
				Some(OrderDirection::Desc)
			} else {
				None
			};

			let nulls = if self.eat(&TokenKind::Keyword(Keyword::Nulls))? {
				if self.eat(&TokenKind::Keyword(Keyword::First))? {
					Some(NullsOrder::First)
				} else if self.eat(&TokenKind::Keyword(Keyword::Last))? {
					Some(NullsOrder::Last)
				} else {
					return Err(ParseError::new(
						self.peek().span,
						ErrorKind::UnexpectedToken {
							expected: "FIRST or LAST",
							found: self.peek().kind.to_string(),
						},
					));
				}
			} else {
				None
			};

			span = span.merge(self.peek().span);
			columns.push(IndexElem {
				expr,
				opclass,
				direction,
				nulls,
				span,
			});

			if !self.eat(&TokenKind::Comma)? {
				break;
			}
		}
		self.expect(&TokenKind::RParen, "')'")?;

		// gram.y: opt_reloptions
		let options = self.parse_opt_reloptions()?;

		let predicate = if self.eat(&TokenKind::Keyword(Keyword::Where))? {
			Some(self.parse_expr()?)
		} else {
			None
		};

		let end_span = predicate.as_ref().map(|e| e.span()).unwrap_or_else(|| self.peek().span);
		let span = start_span.merge(end_span);

		Ok(CreateIndexStmt {
			unique,
			concurrently,
			if_not_exists,
			name,
			table,
			method,
			columns,
			options,
			predicate,
			span,
		})
	}

	/// Parse a PostgreSQL `CREATE VIEW` statement given already consumed `CREATE [OR REPLACE]
	/// [TEMP] [RECURSIVE]`.
	fn parse_create_view_stmt_tail(
		&mut self,
		start_span: Span,
		or_replace: bool,
		temporary: bool,
		recursive: bool,
	) -> ParseResult<CreateViewStmt> {
		self.expect(&TokenKind::Keyword(Keyword::View), "'VIEW'")?;

		let name = self.parse_qualified_name()?;

		// Optional column list: `(col1, col2, ...)`
		let columns = if self.peek().kind == TokenKind::LParen {
			self.parse_column_name_list()?
		} else {
			Vec::new()
		};

		self.expect(&TokenKind::Keyword(Keyword::As), "'AS'")?;
		let query = self.parse_select_stmt()?;
		let span = start_span.merge(query.span);

		Ok(CreateViewStmt {
			or_replace,
			temporary,
			recursive,
			name,
			columns,
			query: Box::new(query),
			span,
		})
	}

	/// Parse a PostgreSQL `CREATE SCHEMA` statement given already consumed `CREATE`.
	fn parse_create_schema_stmt_tail(&mut self, start_span: Span) -> ParseResult<CreateSchemaStmt> {
		self.expect(&TokenKind::Keyword(Keyword::Schema), "'SCHEMA'")?;

		let if_not_exists = if self.eat(&TokenKind::Keyword(Keyword::If))? {
			self.expect(&TokenKind::Keyword(Keyword::Not), "'NOT'")?;
			self.expect(&TokenKind::Keyword(Keyword::Exists), "'EXISTS'")?;
			true
		} else {
			false
		};

		let (name, name_span) = self.parse_ident()?;
		let span = start_span.merge(name_span);

		Ok(CreateSchemaStmt {
			if_not_exists,
			name,
			span,
		})
	}

	/// Parse a PostgreSQL `CREATE EXTENSION` statement given an already consumed `CREATE`.
	///
	/// gram.y: CreateExtensionStmt
	fn parse_create_extension_stmt_tail(
		&mut self,
		start_span: Span,
	) -> ParseResult<CreateExtensionStmt> {
		self.expect(&TokenKind::Keyword(Keyword::Extension), "'EXTENSION'")?;

		let if_not_exists = if self.eat(&TokenKind::Keyword(Keyword::If))? {
			self.expect(&TokenKind::Keyword(Keyword::Not), "'NOT'")?;
			self.expect(&TokenKind::Keyword(Keyword::Exists), "'EXISTS'")?;
			true
		} else {
			false
		};

		let (name, name_span) = self.parse_ident()?;
		let mut span = start_span.merge(name_span);

		// gram.y: opt_with create_extension_opt_list
		self.eat(&TokenKind::Keyword(Keyword::With))?;
		let mut schema = None;
		let mut version = None;
		let mut cascade = false;
		loop {
			if self.eat(&TokenKind::Keyword(Keyword::Schema))? {
				let (value, value_span) = self.parse_ident()?;
				schema = Some(value);
				span = span.merge(value_span);
			} else if self.eat(&TokenKind::Keyword(Keyword::Version))? {
				let (value, value_span) =
					if matches!(self.peek().kind, TokenKind::StringConstant(_)) {
						self.parse_string_constant()?
					} else {
						self.parse_ident()?
					};
				version = Some(value);
				span = span.merge(value_span);
			} else if self.peek().kind == TokenKind::Keyword(Keyword::Cascade) {
				span = span.merge(self.bump()?.span);
				cascade = true;
			} else {
				break;
			}
		}

		Ok(CreateExtensionStmt {
			if_not_exists,
			name,
			schema,
			version,
			cascade,
			span,
		})
	}

	/// Parse an `ALTER TABLE` statement.
	///
	/// gram.y: AlterTableStmt
	pub fn parse_alter_table_stmt(&mut self) -> ParseResult<AlterTableStmt> {
		let alter_tok = self.expect(&TokenKind::Keyword(Keyword::Alter), "'ALTER'")?;
		self.expect(&TokenKind::Keyword(Keyword::Table), "'TABLE'")?;

		let if_exists = if self.eat(&TokenKind::Keyword(Keyword::If))? {
			self.expect(&TokenKind::Keyword(Keyword::Exists), "'EXISTS'")?;
			true
		} else {
			false
		};

		let name = self.parse_qualified_name()?;
		let mut actions = Vec::new();

		loop {
			if self.eat(&TokenKind::Keyword(Keyword::Add))? {
				if self.eat(&TokenKind::Keyword(Keyword::Constraint))? {
					let (_cname, _) = self.parse_ident()?;
					let tc = self.parse_table_constraint_tail()?;
					actions.push(AlterTableAction::AddConstraint(tc));
				} else if self.peek().kind == TokenKind::Keyword(Keyword::Primary)
					|| self.peek().kind == TokenKind::Keyword(Keyword::Unique)
					|| self.peek().kind == TokenKind::Keyword(Keyword::Foreign)
				{
					let tc = self.parse_table_constraint_tail()?;
					actions.push(AlterTableAction::AddConstraint(tc));
				} else {
					// ADD [COLUMN] [IF NOT EXISTS] col_def
					self.eat(&TokenKind::Keyword(Keyword::Column))?;
					let if_not_exists = if self.eat(&TokenKind::Keyword(Keyword::If))? {
						self.expect(&TokenKind::Keyword(Keyword::Not), "'NOT'")?;
						self.expect(&TokenKind::Keyword(Keyword::Exists), "'EXISTS'")?;
						true
					} else {
						false
					};
					let column = self.parse_column_def()?;
					actions.push(AlterTableAction::AddColumn {
						if_not_exists,
						column,
					});
				}
			} else if self.eat(&TokenKind::Keyword(Keyword::Drop))? {
				if self.eat(&TokenKind::Keyword(Keyword::Constraint))? {
					let if_exists = if self.eat(&TokenKind::Keyword(Keyword::If))? {
						self.expect(&TokenKind::Keyword(Keyword::Exists), "'EXISTS'")?;
						true
					} else {
						false
					};
					let (cname, _) = self.parse_ident()?;
					let cascade = if self.eat(&TokenKind::Keyword(Keyword::Cascade))? {
						true
					} else {
						self.eat(&TokenKind::Keyword(Keyword::Restrict))?;
						false
					};
					actions.push(AlterTableAction::DropConstraint {
						if_exists,
						name: cname,
						cascade,
					});
				} else {
					self.eat(&TokenKind::Keyword(Keyword::Column))?;
					let if_exists = if self.eat(&TokenKind::Keyword(Keyword::If))? {
						self.expect(&TokenKind::Keyword(Keyword::Exists), "'EXISTS'")?;
						true
					} else {
						false
					};
					let (cname, _) = self.parse_ident()?;
					let cascade = if self.eat(&TokenKind::Keyword(Keyword::Cascade))? {
						true
					} else {
						self.eat(&TokenKind::Keyword(Keyword::Restrict))?;
						false
					};
					actions.push(AlterTableAction::DropColumn {
						if_exists,
						name: cname,
						cascade,
					});
				}
			} else if self.eat(&TokenKind::Keyword(Keyword::Alter))? {
				self.eat(&TokenKind::Keyword(Keyword::Column))?;
				let (col_name, _) = self.parse_ident()?;

				if self.eat(&TokenKind::Keyword(Keyword::Type))? {
					let data_type = self.parse_type_name()?;
					actions.push(AlterTableAction::AlterColumnType {
						name: col_name,
						data_type,
					});
				} else if self.eat(&TokenKind::Keyword(Keyword::Set))? {
					if self.eat(&TokenKind::Keyword(Keyword::Default))? {
						let default = self.parse_expr()?;
						actions.push(AlterTableAction::SetDefault {
							column: col_name,
							default,
						});
					} else if self.eat(&TokenKind::Keyword(Keyword::Not))? {
						self.expect(&TokenKind::Keyword(Keyword::Null), "'NULL'")?;
						actions.push(AlterTableAction::SetNotNull {
							column: col_name,
						});
					} else {
						return Err(ParseError::new(
							self.peek().span,
							ErrorKind::UnexpectedToken {
								expected: "DEFAULT or NOT NULL",
								found: self.peek().kind.to_string(),
							},
						));
					}
				} else if self.eat(&TokenKind::Keyword(Keyword::Drop))? {
					if self.eat(&TokenKind::Keyword(Keyword::Default))? {
						actions.push(AlterTableAction::DropDefault {
							column: col_name,
						});
					} else if self.eat(&TokenKind::Keyword(Keyword::Not))? {
						self.expect(&TokenKind::Keyword(Keyword::Null), "'NULL'")?;
						actions.push(AlterTableAction::DropNotNull {
							column: col_name,
						});
					} else {
						return Err(ParseError::new(
							self.peek().span,
							ErrorKind::UnexpectedToken {
								expected: "DEFAULT or NOT NULL",
								found: self.peek().kind.to_string(),
							},
						));
					}
				} else {
					return Err(ParseError::new(
						self.peek().span,
						ErrorKind::UnexpectedToken {
							expected: "TYPE, SET, or DROP",
							found: self.peek().kind.to_string(),
						},
					));
				}
			} else if self.eat(&TokenKind::Keyword(Keyword::Rename))? {
				if self.eat(&TokenKind::Keyword(Keyword::Column))? {
					let (old_name, _) = self.parse_ident()?;
					self.expect(&TokenKind::Keyword(Keyword::To), "'TO'")?;
					let (new_name, _) = self.parse_ident()?;
					actions.push(AlterTableAction::RenameColumn {
						old_name,
						new_name,
					});
				} else if self.eat(&TokenKind::Keyword(Keyword::To))? {
					let (new_name, _) = self.parse_ident()?;
					actions.push(AlterTableAction::RenameTable {
						new_name,
					});
				} else {
					let (old_name, _) = self.parse_ident()?;
					self.expect(&TokenKind::Keyword(Keyword::To), "'TO'")?;
					let (new_name, _) = self.parse_ident()?;
					actions.push(AlterTableAction::RenameColumn {
						old_name,
						new_name,
					});
				}
			} else {
				return Err(ParseError::new(
					self.peek().span,
					ErrorKind::UnexpectedToken {
						expected: "ADD, DROP, ALTER, or RENAME",
						found: self.peek().kind.to_string(),
					},
				));
			}

			if !self.eat(&TokenKind::Comma)? {
				break;
			}
		}

		let span = alter_tok.span.merge(self.peek().span);
		Ok(AlterTableStmt {
			if_exists,
			name,
			actions,
			span,
		})
	}

	/// Parse a single column definition: `col_name data_type [col_constraints...]`
	pub fn parse_column_def(&mut self) -> ParseResult<ColumnDef> {
		let (col_name, col_start_span) = self.parse_ident()?;
		let data_type = self.parse_type_name()?;
		let mut col_constraints = Vec::new();
		let mut col_end_span = data_type.span;

		while self.peek().kind != TokenKind::Comma
			&& self.peek().kind != TokenKind::RParen
			&& !self.peek().is_eof()
			&& self.peek().kind != TokenKind::Semi
		{
			if self.eat(&TokenKind::Keyword(Keyword::Primary))? {
				self.expect(&TokenKind::Keyword(Keyword::Key), "'KEY'")?;
				col_constraints.push(ColumnConstraint::PrimaryKey);
			} else if self.eat(&TokenKind::Keyword(Keyword::Not))? {
				self.expect(&TokenKind::Keyword(Keyword::Null), "'NULL'")?;
				col_constraints.push(ColumnConstraint::NotNull);
			} else if self.eat(&TokenKind::Keyword(Keyword::Null))? {
				col_constraints.push(ColumnConstraint::Null);
			} else if self.eat(&TokenKind::Keyword(Keyword::Unique))? {
				col_constraints.push(ColumnConstraint::Unique);
			} else if self.eat(&TokenKind::Keyword(Keyword::Default))? {
				let default_expr = self.parse_expr()?;
				col_end_span = default_expr.span();
				col_constraints.push(ColumnConstraint::Default(default_expr));
			} else if self.eat(&TokenKind::Keyword(Keyword::References))? {
				let ref_table = self.parse_qualified_name()?;
				let column = if self.eat(&TokenKind::LParen)? {
					let (ref_col, _) = self.parse_ident()?;
					self.expect(&TokenKind::RParen, "')'")?;
					Some(ref_col)
				} else {
					None
				};
				col_end_span = ref_table.span;
				let on_delete = self.parse_referential_actions()?;
				col_constraints.push(ColumnConstraint::References {
					table: ref_table,
					column,
					on_delete,
				});
			} else {
				break;
			}
		}

		Ok(ColumnDef {
			name: col_name,
			data_type,
			constraints: col_constraints,
			span: col_start_span.merge(col_end_span),
		})
	}

	/// Parse the `ON DELETE action` and `ON UPDATE action` clauses that may follow a foreign key,
	/// in either order, and return the `ON DELETE` action (`NO ACTION` when absent). The
	/// `ON UPDATE` action is accepted and not kept: a record's id never changes.
	fn parse_referential_actions(&mut self) -> ParseResult<ReferentialAction> {
		let mut on_delete = ReferentialAction::NoAction;
		while self.eat(&TokenKind::Keyword(Keyword::On))? {
			let is_delete = if self.eat(&TokenKind::Keyword(Keyword::Delete))? {
				true
			} else if self.eat(&TokenKind::Keyword(Keyword::Update))? {
				false
			} else {
				return Err(ParseError::new(
					self.peek().span,
					ErrorKind::UnexpectedToken {
						expected: "DELETE or UPDATE",
						found: self.peek().kind.to_string(),
					},
				));
			};
			let action = if self.eat(&TokenKind::Keyword(Keyword::No))? {
				self.expect(&TokenKind::Keyword(Keyword::Action), "'ACTION'")?;
				ReferentialAction::NoAction
			} else if self.eat(&TokenKind::Keyword(Keyword::Restrict))? {
				ReferentialAction::Restrict
			} else if self.eat(&TokenKind::Keyword(Keyword::Cascade))? {
				ReferentialAction::Cascade
			} else if self.eat(&TokenKind::Keyword(Keyword::Set))? {
				if self.eat(&TokenKind::Keyword(Keyword::Null))? {
					ReferentialAction::SetNull
				} else {
					self.expect(&TokenKind::Keyword(Keyword::Default), "'NULL' or 'DEFAULT'")?;
					ReferentialAction::SetDefault
				}
			} else {
				return Err(ParseError::new(
					self.peek().span,
					ErrorKind::UnexpectedToken {
						expected: "NO ACTION, RESTRICT, CASCADE, SET NULL or SET DEFAULT",
						found: self.peek().kind.to_string(),
					},
				));
			};
			if is_delete {
				on_delete = action;
			}
		}
		Ok(on_delete)
	}

	/// Parse a table constraint without the optional `CONSTRAINT name` prefix.
	pub fn parse_table_constraint_tail(&mut self) -> ParseResult<TableConstraint> {
		if self.eat(&TokenKind::Keyword(Keyword::Primary))? {
			self.expect(&TokenKind::Keyword(Keyword::Key), "'KEY'")?;
			let cols = self.parse_column_name_list()?;
			Ok(TableConstraint::PrimaryKey(cols))
		} else if self.eat(&TokenKind::Keyword(Keyword::Unique))? {
			let cols = self.parse_column_name_list()?;
			Ok(TableConstraint::Unique(cols))
		} else if self.eat(&TokenKind::Keyword(Keyword::Foreign))? {
			self.expect(&TokenKind::Keyword(Keyword::Key), "'KEY'")?;
			let columns_list = self.parse_column_name_list()?;
			self.expect(&TokenKind::Keyword(Keyword::References), "'REFERENCES'")?;
			let ref_table = self.parse_qualified_name()?;
			let ref_columns = if self.peek().kind == TokenKind::LParen {
				self.parse_column_name_list()?
			} else {
				Vec::new()
			};
			let on_delete = self.parse_referential_actions()?;
			Ok(TableConstraint::ForeignKey {
				columns: columns_list,
				ref_table,
				ref_columns,
				on_delete,
			})
		} else {
			Err(ParseError::new(
				self.peek().span,
				ErrorKind::UnexpectedToken {
					expected: "PRIMARY KEY, UNIQUE, or FOREIGN KEY",
					found: self.peek().kind.to_string(),
				},
			))
		}
	}

	/// Parse a PostgreSQL `DROP` statement (`DROP TABLE ...`, `DROP INDEX ...`).
	///
	/// gram.y: DropStmt
	pub fn parse_drop_stmt(&mut self) -> ParseResult<Statement> {
		let drop_tok = self.expect(&TokenKind::Keyword(Keyword::Drop), "'DROP'")?;

		if self.eat(&TokenKind::Keyword(Keyword::Property))? {
			let s = self.parse_drop_property_graph_stmt_tail(drop_tok.span)?;
			return Ok(Statement::DropPropertyGraph(Box::new(s)));
		}

		let object_type = if self.peek().kind == TokenKind::Keyword(Keyword::Table) {
			self.bump()?;
			DropObjectType::Table
		} else if self.peek().kind == TokenKind::Keyword(Keyword::Index) {
			self.bump()?;
			DropObjectType::Index
		} else if self.peek().kind == TokenKind::Keyword(Keyword::View) {
			self.bump()?;
			DropObjectType::View
		} else if self.peek().kind == TokenKind::Keyword(Keyword::Schema) {
			self.bump()?;
			DropObjectType::Schema
		} else if self.peek().kind == TokenKind::Keyword(Keyword::Type) {
			self.bump()?;
			DropObjectType::Type
		} else {
			return Err(ParseError::new(
				self.peek().span,
				ErrorKind::UnexpectedToken {
					expected: "TABLE, INDEX, VIEW, SCHEMA, TYPE, or PROPERTY GRAPH",
					found: self.peek().kind.to_string(),
				},
			));
		};

		// Optional IF EXISTS
		let if_exists = if self.eat(&TokenKind::Keyword(Keyword::If))? {
			self.expect(&TokenKind::Keyword(Keyword::Exists), "'EXISTS'")?;
			true
		} else {
			false
		};

		// Target names list
		let mut names = Vec::new();
		loop {
			names.push(self.parse_qualified_name()?);
			if !self.eat(&TokenKind::Comma)? {
				break;
			}
		}

		// Optional CASCADE or RESTRICT
		let cascade = if self.eat(&TokenKind::Keyword(Keyword::Cascade))? {
			true
		} else {
			self.eat(&TokenKind::Keyword(Keyword::Restrict))?;
			false
		};

		let end_span = self.peek().span;
		let span = drop_tok.span.merge(end_span);

		Ok(Statement::Drop(Box::new(DropStmt {
			object_type,
			if_exists,
			names,
			cascade,
			span,
		})))
	}

	/// Helper to parse a parenthesized column name list: `(col1, col2, ...)`.
	fn parse_column_name_list(&mut self) -> ParseResult<Vec<String>> {
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
		Ok(cols)
	}
}
