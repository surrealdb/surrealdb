//! DDL statement parser (CREATE, DROP, USE, SET) for MySQL.

use crate::ast::stmt::{
	AlterTableAction, AlterTableStmt, ColumnDef, CreateDatabaseStmt, CreateIndexStmt,
	CreateTableStmt, CreateUserStmt, CreateViewStmt, DeallocateStmt, DropDatabaseStmt,
	DropIndexStmt, DropTableStmt, DropUserStmt, DropViewStmt, ExecuteStmt, GrantStmt,
	LockTablesStmt, PrepareStmt, RevokeStmt, SetStmt, TableConstraint, TableLock, TableLockType,
	UnlockTablesStmt, UseStmt, VariableTarget,
};
use crate::ast::{Expr, Literal};
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;
use crate::parser::error::{ErrorKind, ParseError, ParseResult};

impl<'a> Parser<'a> {
	/// Parse a column definition.
	pub fn parse_column_def(&mut self) -> ParseResult<ColumnDef> {
		let c_start = self.peek().span;
		let (col_name, _) = self.parse_ident()?;
		let (data_type, _) = self.parse_data_type()?;

		let mut not_null = false;
		let mut default = None;
		let mut auto_increment = false;
		let mut primary_key = false;
		let mut unique = false;

		loop {
			if self.peek_keyword() == Some(Keyword::Not)
				&& self.peek_nth_keyword(1) == Some(Keyword::Null)
			{
				self.bump();
				self.bump();
				not_null = true;
			} else if self.peek_keyword() == Some(Keyword::Null) {
				self.bump();
			} else if self.peek_keyword() == Some(Keyword::Default) {
				self.bump();
				default = Some(self.parse_expr()?);
			} else if self.peek_keyword() == Some(Keyword::AutoIncrement) {
				self.bump();
				auto_increment = true;
			} else if self.peek_keyword() == Some(Keyword::Primary) {
				self.bump();
				if self.peek_keyword() == Some(Keyword::Key) {
					self.bump();
				}
				primary_key = true;
			} else if self.peek_keyword() == Some(Keyword::Unique) {
				self.bump();
				if self.peek_keyword() == Some(Keyword::Key) {
					self.bump();
				}
				unique = true;
			} else {
				break;
			}
		}

		let c_end = self.peek().span;
		Ok(ColumnDef {
			name: col_name,
			data_type,
			not_null,
			default,
			auto_increment,
			primary_key,
			unique,
			span: c_start.merge(c_end),
		})
	}

	/// Parse a CREATE statement.
	pub fn parse_create_stmt(&mut self) -> ParseResult<CreateTableStmt> {
		let start = self.expect_keyword(Keyword::Create, "CREATE")?.span;

		let temporary = if self.peek_keyword() == Some(Keyword::Temporary) {
			self.bump();
			true
		} else {
			false
		};

		self.expect_keyword(Keyword::Table, "TABLE after CREATE")?;

		let if_not_exists = if self.peek_keyword() == Some(Keyword::If)
			&& self.peek_nth_keyword(1) == Some(Keyword::Not)
			&& self.peek_nth_keyword(2) == Some(Keyword::Exists)
		{
			self.bump(); // IF
			self.bump(); // NOT
			self.bump(); // EXISTS
			true
		} else {
			false
		};

		let name = self.parse_qualified_name()?;

		self.expect(&TokenKind::LParen, "'(' after table name in CREATE TABLE")?;

		let mut columns = Vec::new();
		let mut constraints = Vec::new();

		loop {
			// Check optional CONSTRAINT [symbol] prefix
			let constraint_name = if self.peek_keyword() == Some(Keyword::Constraint) {
				self.bump();
				if self.peek_keyword().is_none() && self.peek().kind != TokenKind::LParen {
					Some(self.parse_ident()?.0)
				} else {
					None
				}
			} else {
				None
			};

			// Check table constraints: PRIMARY KEY, UNIQUE, FOREIGN KEY
			if self.peek_keyword() == Some(Keyword::Primary) {
				let c_start = self.bump().span;
				self.expect_keyword(Keyword::Key, "KEY after PRIMARY")?;
				let cols = self.parse_column_name_list()?;
				let c_end = self.peek().span;
				constraints.push(TableConstraint::PrimaryKey {
					name: constraint_name,
					columns: cols,
					span: c_start.merge(c_end),
				});
			} else if self.peek_keyword() == Some(Keyword::Unique) {
				let c_start = self.bump().span;
				if self.peek_keyword() == Some(Keyword::Key)
					|| self.peek_keyword() == Some(Keyword::Index)
				{
					self.bump();
				}
				let name = if self.peek().kind != TokenKind::LParen {
					Some(self.parse_ident()?.0)
				} else {
					constraint_name
				};
				let cols = self.parse_column_name_list()?;
				let c_end = self.peek().span;
				constraints.push(TableConstraint::Unique {
					name,
					columns: cols,
					span: c_start.merge(c_end),
				});
			} else if self.peek_keyword() == Some(Keyword::Foreign) {
				let c_start = self.bump().span;
				self.expect_keyword(Keyword::Key, "KEY after FOREIGN")?;
				let cols = self.parse_column_name_list()?;
				self.expect_keyword(Keyword::References, "REFERENCES in FOREIGN KEY")?;
				let ref_table = self.parse_qualified_name()?;
				let ref_cols = self.parse_column_name_list()?;
				let c_end = self.peek().span;
				constraints.push(TableConstraint::ForeignKey {
					name: constraint_name,
					columns: cols,
					ref_table,
					ref_columns: ref_cols,
					on_delete: None,
					on_update: None,
					span: c_start.merge(c_end),
				});
			} else {
				columns.push(self.parse_column_def()?);
			}

			if self.peek().kind == TokenKind::Comma {
				self.bump();
			} else {
				break;
			}
		}

		self.expect(&TokenKind::RParen, "')' closing column definitions")?;

		// Skip optional table options: ENGINE=..., DEFAULT CHARSET=..., etc.
		while self.peek().kind != TokenKind::Semicolon && self.peek().kind != TokenKind::Eof {
			if matches!(self.peek().kind, TokenKind::Ident(_)) || self.peek_keyword().is_some() {
				self.bump();
				if self.peek().kind == TokenKind::Equal {
					self.bump();
					self.bump();
				}
			} else {
				break;
			}
		}

		let end = self.peek().span;
		let span = start.merge(end);

		Ok(CreateTableStmt {
			if_not_exists,
			temporary,
			name,
			columns,
			constraints,
			span,
		})
	}

	fn parse_column_name_list(&mut self) -> ParseResult<Vec<String>> {
		self.expect(&TokenKind::LParen, "'(' before column names")?;
		let mut cols = Vec::new();
		loop {
			cols.push(self.parse_ident()?.0);
			if self.peek().kind == TokenKind::Comma {
				self.bump();
			} else {
				break;
			}
		}
		self.expect(&TokenKind::RParen, "')' after column names")?;
		Ok(cols)
	}

	fn parse_data_type(&mut self) -> ParseResult<(String, crate::lexer::span::Span)> {
		let (base, start_span) = self.parse_ident()?;
		let mut type_str = base;
		let mut end_span = start_span;

		// Parameters like (255) or (10, 2)
		if self.peek().kind == TokenKind::LParen {
			self.bump();
			type_str.push('(');
			loop {
				let tok = self.bump();
				type_str.push_str(&tok.kind.to_string());
				if self.peek().kind == TokenKind::Comma {
					self.bump();
					type_str.push_str(", ");
				} else if self.peek().kind == TokenKind::RParen {
					end_span = self.bump().span;
					type_str.push(')');
					break;
				} else if self.peek().kind == TokenKind::Eof {
					break;
				}
			}
		}

		// Optional UNSIGNED / ZEROFILL
		if self.peek().kind == TokenKind::Ident("unsigned".to_string())
			|| self.peek().kind == TokenKind::Ident("UNSIGNED".to_string())
		{
			let span = self.bump().span;
			end_span = end_span.merge(span);
			type_str.push_str(" UNSIGNED");
		}

		Ok((type_str, start_span.merge(end_span)))
	}

	/// Parse a DROP statement.
	pub fn parse_drop_stmt(&mut self) -> ParseResult<DropTableStmt> {
		let start = self.expect_keyword(Keyword::Drop, "DROP")?.span;

		let temporary = if self.peek_keyword() == Some(Keyword::Temporary) {
			self.bump();
			true
		} else {
			false
		};

		if self.peek_keyword() == Some(Keyword::Table)
			|| self.peek_keyword() == Some(Keyword::Tables)
		{
			self.bump();
		} else {
			self.expect_keyword(Keyword::Table, "TABLE after DROP")?;
		}

		let if_exists = if self.peek_keyword() == Some(Keyword::If)
			&& self.peek_nth_keyword(1) == Some(Keyword::Exists)
		{
			self.bump();
			self.bump();
			true
		} else {
			false
		};

		let mut names = Vec::new();
		loop {
			names.push(self.parse_qualified_name()?);
			if self.peek().kind == TokenKind::Comma {
				self.bump();
			} else {
				break;
			}
		}

		if self.peek_keyword() == Some(Keyword::Cascade)
			|| self.peek_keyword() == Some(Keyword::Restrict)
		{
			self.bump();
		}

		let end = self.peek().span;
		let span = start.merge(end);

		Ok(DropTableStmt {
			if_exists,
			temporary,
			names,
			span,
		})
	}

	/// Parse a USE statement.
	pub fn parse_use_stmt(&mut self) -> ParseResult<UseStmt> {
		let start = self.expect_keyword(Keyword::Use, "USE")?.span;
		let (database, db_span) = self.parse_ident()?;
		let span = start.merge(db_span);
		Ok(UseStmt {
			database,
			span,
		})
	}

	/// Parse a SET statement.
	pub fn parse_set_stmt(&mut self) -> ParseResult<SetStmt> {
		let start = self.expect_keyword(Keyword::Set, "SET")?.span;
		let mut assignments = Vec::new();

		if matches!(&self.peek().kind, TokenKind::Ident(s) if s.eq_ignore_ascii_case("names")) {
			self.bump();
			if self.peek().kind == TokenKind::Equal {
				self.bump();
			}
			let (cs, cs_span) = if let TokenKind::StringLiteral(s) = &self.peek().kind {
				let span = self.peek().span;
				let str_val = s.clone();
				self.bump();
				(str_val, span)
			} else {
				self.parse_ident()?
			};
			assignments.push((
				VariableTarget::System {
					name: "names".to_string(),
					is_global: false,
				},
				Expr::Literal(Literal::String(cs), cs_span),
			));
			let end = self.peek().span;
			return Ok(SetStmt {
				assignments,
				span: start.merge(end),
			});
		}

		loop {
			let target = if let TokenKind::UserVar(v) = &self.peek().kind {
				let name = v.clone();
				self.bump();
				VariableTarget::User(name)
			} else if let TokenKind::SystemVar {
				name,
				is_global,
			} = &self.peek().kind
			{
				let n = name.clone();
				let g = *is_global;
				self.bump();
				VariableTarget::System {
					name: n,
					is_global: g,
				}
			} else if self.peek_keyword() == Some(Keyword::Global) {
				self.bump();
				let (n, _) = self.parse_ident()?;
				VariableTarget::System {
					name: n,
					is_global: true,
				}
			} else if self.peek_keyword() == Some(Keyword::Session)
				|| self.peek_keyword() == Some(Keyword::Local)
			{
				self.bump();
				let (n, _) = self.parse_ident()?;
				VariableTarget::System {
					name: n,
					is_global: false,
				}
			} else {
				let (n, _) = self.parse_ident()?;
				VariableTarget::System {
					name: n,
					is_global: false,
				}
			};

			if self.peek().kind == TokenKind::Equal || self.peek().kind == TokenKind::ColonEqual {
				self.bump();
			} else {
				return Err(ParseError::new(
					self.peek().span,
					ErrorKind::UnexpectedToken {
						expected: "= or := in SET assignment",
						found: self.peek().kind.to_string(),
					},
				));
			}

			let val = self.parse_expr()?;
			assignments.push((target, val));

			if self.peek().kind == TokenKind::Comma {
				self.bump();
			} else {
				break;
			}
		}

		let end = self.peek().span;
		let span = start.merge(end);

		Ok(SetStmt {
			assignments,
			span,
		})
	}

	/// Parse a CREATE DATABASE / SCHEMA statement.
	pub fn parse_create_database_stmt(&mut self) -> ParseResult<CreateDatabaseStmt> {
		let start = self.expect_keyword(Keyword::Create, "CREATE")?.span;
		if self.peek_keyword() == Some(Keyword::Database)
			|| self.peek_keyword() == Some(Keyword::Databases)
			|| self.peek_keyword() == Some(Keyword::Schema)
			|| self.peek_keyword() == Some(Keyword::Schemas)
		{
			self.bump();
		} else {
			self.expect_keyword(Keyword::Database, "DATABASE or SCHEMA after CREATE")?;
		}

		let if_not_exists = if self.peek_keyword() == Some(Keyword::If)
			&& self.peek_nth_keyword(1) == Some(Keyword::Not)
			&& self.peek_nth_keyword(2) == Some(Keyword::Exists)
		{
			self.bump();
			self.bump();
			self.bump();
			true
		} else {
			false
		};

		let (name, name_span) = self.parse_ident()?;
		// Skip optional [DEFAULT] CHARACTER SET / COLLATE
		while self.peek_keyword() == Some(Keyword::Default)
			|| self.peek_keyword() == Some(Keyword::Character)
			|| self.peek_keyword() == Some(Keyword::Charset)
			|| self.peek_keyword() == Some(Keyword::Collate)
			|| self.peek_keyword() == Some(Keyword::Collation)
		{
			self.bump();
			if self.peek().kind == TokenKind::Equal {
				self.bump();
			}
			if matches!(self.peek().kind, TokenKind::Ident(_)) || self.peek_keyword().is_some() {
				self.bump();
			}
		}

		let span = start.merge(name_span);
		Ok(CreateDatabaseStmt {
			if_not_exists,
			name,
			span,
		})
	}

	/// Parse a DROP DATABASE / SCHEMA statement.
	pub fn parse_drop_database_stmt(&mut self) -> ParseResult<DropDatabaseStmt> {
		let start = self.expect_keyword(Keyword::Drop, "DROP")?.span;
		if self.peek_keyword() == Some(Keyword::Database)
			|| self.peek_keyword() == Some(Keyword::Databases)
			|| self.peek_keyword() == Some(Keyword::Schema)
			|| self.peek_keyword() == Some(Keyword::Schemas)
		{
			self.bump();
		} else {
			self.expect_keyword(Keyword::Database, "DATABASE or SCHEMA after DROP")?;
		}

		let if_exists = if self.peek_keyword() == Some(Keyword::If)
			&& self.peek_nth_keyword(1) == Some(Keyword::Exists)
		{
			self.bump();
			self.bump();
			true
		} else {
			false
		};

		let (name, name_span) = self.parse_ident()?;
		let span = start.merge(name_span);
		Ok(DropDatabaseStmt {
			if_exists,
			name,
			span,
		})
	}

	/// Parse a CREATE [UNIQUE] INDEX statement.
	pub fn parse_create_index_stmt(&mut self, unique: bool) -> ParseResult<CreateIndexStmt> {
		let start = self.expect_keyword(Keyword::Create, "CREATE")?.span;
		if unique {
			self.expect_keyword(Keyword::Unique, "UNIQUE")?;
		}
		self.expect_keyword(Keyword::Index, "INDEX")?;
		let (name, _) = self.parse_ident()?;
		self.expect_keyword(Keyword::On, "ON after index name")?;
		let table = self.parse_qualified_name()?;
		let columns = self.parse_column_name_list()?;
		let end = self.peek().span;
		Ok(CreateIndexStmt {
			unique,
			name,
			table,
			columns,
			span: start.merge(end),
		})
	}

	/// Parse a DROP INDEX statement.
	pub fn parse_drop_index_stmt(&mut self) -> ParseResult<DropIndexStmt> {
		let start = self.expect_keyword(Keyword::Drop, "DROP")?.span;
		self.expect_keyword(Keyword::Index, "INDEX")?;
		let (name, _) = self.parse_ident()?;
		self.expect_keyword(Keyword::On, "ON after index name")?;
		let table = self.parse_qualified_name()?;
		let span = start.merge(table.span);
		Ok(DropIndexStmt {
			name,
			table,
			span,
		})
	}

	/// Parse a CREATE [OR REPLACE] VIEW statement.
	pub fn parse_create_view_stmt(&mut self, or_replace: bool) -> ParseResult<CreateViewStmt> {
		let start = self.expect_keyword(Keyword::Create, "CREATE")?.span;
		if or_replace {
			self.expect_keyword(Keyword::Or, "OR")?;
			self.expect_keyword(Keyword::Replace, "REPLACE")?;
		}
		self.expect_keyword(Keyword::View, "VIEW")?;
		let name = self.parse_qualified_name()?;
		let columns = if self.peek().kind == TokenKind::LParen {
			self.parse_column_name_list()?
		} else {
			Vec::new()
		};
		self.expect_keyword(Keyword::As, "AS in CREATE VIEW")?;
		let query = Box::new(self.parse_select_stmt()?);
		let span = start.merge(query.span);
		Ok(CreateViewStmt {
			or_replace,
			name,
			columns,
			query,
			span,
		})
	}

	/// Parse a DROP VIEW statement.
	pub fn parse_drop_view_stmt(&mut self) -> ParseResult<DropViewStmt> {
		let start = self.expect_keyword(Keyword::Drop, "DROP")?.span;
		self.expect_keyword(Keyword::View, "VIEW")?;
		let if_exists = if self.peek_keyword() == Some(Keyword::If)
			&& self.peek_nth_keyword(1) == Some(Keyword::Exists)
		{
			self.bump();
			self.bump();
			true
		} else {
			false
		};

		let mut names = Vec::new();
		loop {
			names.push(self.parse_qualified_name()?);
			if self.peek().kind == TokenKind::Comma {
				self.bump();
			} else {
				break;
			}
		}

		let end = self.peek().span;
		Ok(DropViewStmt {
			if_exists,
			names,
			span: start.merge(end),
		})
	}

	/// Parse an ALTER TABLE statement.
	pub fn parse_alter_table_stmt(&mut self) -> ParseResult<AlterTableStmt> {
		let start = self.expect_keyword(Keyword::Alter, "ALTER")?.span;
		self.expect_keyword(Keyword::Table, "TABLE")?;
		let table = self.parse_qualified_name()?;

		let mut actions = Vec::new();
		loop {
			let action = if self.peek_keyword() == Some(Keyword::Add) {
				self.bump(); // ADD
				if self.peek_keyword() == Some(Keyword::Column) {
					self.bump();
					let col = self.parse_column_def()?;
					AlterTableAction::AddColumn(col)
				} else if self.peek_keyword() == Some(Keyword::Primary) {
					self.bump();
					self.expect_keyword(Keyword::Key, "KEY after PRIMARY")?;
					let cols = self.parse_column_name_list()?;
					let c_end = self.peek().span;
					AlterTableAction::AddConstraint(TableConstraint::PrimaryKey {
						name: None,
						columns: cols,
						span: start.merge(c_end),
					})
				} else if self.peek_keyword() == Some(Keyword::Unique) {
					self.bump();
					if self.peek_keyword() == Some(Keyword::Key)
						|| self.peek_keyword() == Some(Keyword::Index)
					{
						self.bump();
					}
					let name = if self.peek().kind != TokenKind::LParen {
						Some(self.parse_ident()?.0)
					} else {
						None
					};
					let cols = self.parse_column_name_list()?;
					let c_end = self.peek().span;
					AlterTableAction::AddConstraint(TableConstraint::Unique {
						name,
						columns: cols,
						span: start.merge(c_end),
					})
				} else if self.peek_keyword() == Some(Keyword::Constraint) {
					self.bump();
					let c_name =
						if self.peek().kind != TokenKind::LParen && self.peek_keyword().is_none() {
							Some(self.parse_ident()?.0)
						} else {
							None
						};
					if self.peek_keyword() == Some(Keyword::Primary) {
						self.bump();
						self.expect_keyword(Keyword::Key, "KEY after PRIMARY")?;
						let cols = self.parse_column_name_list()?;
						let c_end = self.peek().span;
						AlterTableAction::AddConstraint(TableConstraint::PrimaryKey {
							name: c_name,
							columns: cols,
							span: start.merge(c_end),
						})
					} else if self.peek_keyword() == Some(Keyword::Foreign) {
						self.bump();
						self.expect_keyword(Keyword::Key, "KEY after FOREIGN")?;
						let cols = self.parse_column_name_list()?;
						self.expect_keyword(Keyword::References, "REFERENCES in FOREIGN KEY")?;
						let ref_table = self.parse_qualified_name()?;
						let ref_cols = self.parse_column_name_list()?;
						let c_end = self.peek().span;
						AlterTableAction::AddConstraint(TableConstraint::ForeignKey {
							name: c_name,
							columns: cols,
							ref_table,
							ref_columns: ref_cols,
							on_delete: None,
							on_update: None,
							span: start.merge(c_end),
						})
					} else {
						let col = self.parse_column_def()?;
						AlterTableAction::AddColumn(col)
					}
				} else {
					let col = self.parse_column_def()?;
					AlterTableAction::AddColumn(col)
				}
			} else if self.peek_keyword() == Some(Keyword::Drop) {
				self.bump(); // DROP
				if self.peek_keyword() == Some(Keyword::Column) {
					self.bump();
					let (col_name, _) = self.parse_ident()?;
					AlterTableAction::DropColumn {
						name: col_name,
					}
				} else if self.peek_keyword() == Some(Keyword::Primary) {
					self.bump();
					self.expect_keyword(Keyword::Key, "KEY after PRIMARY")?;
					AlterTableAction::DropPrimaryKey
				} else if self.peek_keyword() == Some(Keyword::Index)
					|| self.peek_keyword() == Some(Keyword::Key)
				{
					self.bump();
					let (idx_name, _) = self.parse_ident()?;
					AlterTableAction::DropIndex {
						name: idx_name,
					}
				} else {
					let (col_name, _) = self.parse_ident()?;
					AlterTableAction::DropColumn {
						name: col_name,
					}
				}
			} else if self.peek_keyword() == Some(Keyword::Modify) {
				self.bump(); // MODIFY
				if self.peek_keyword() == Some(Keyword::Column) {
					self.bump();
				}
				let col = self.parse_column_def()?;
				AlterTableAction::ModifyColumn(col)
			} else if self.peek_keyword() == Some(Keyword::Change) {
				self.bump(); // CHANGE
				if self.peek_keyword() == Some(Keyword::Column) {
					self.bump();
				}
				let (old_name, _) = self.parse_ident()?;
				let new_def = self.parse_column_def()?;
				AlterTableAction::ChangeColumn {
					old_name,
					new_def,
				}
			} else if self.peek_keyword() == Some(Keyword::Rename) {
				self.bump(); // RENAME
				if self.peek_keyword() == Some(Keyword::To)
					|| self.peek_keyword() == Some(Keyword::As)
				{
					self.bump();
				}
				let new_name = self.parse_qualified_name()?;
				AlterTableAction::RenameTable {
					new_name,
				}
			} else {
				return Err(ParseError::new(
					self.peek().span,
					ErrorKind::UnexpectedToken {
						expected: "ADD, DROP, MODIFY, CHANGE, or RENAME in ALTER TABLE",
						found: self.peek().kind.to_string(),
					},
				));
			};

			actions.push(action);
			if self.peek().kind == TokenKind::Comma {
				self.bump();
			} else {
				break;
			}
		}

		let end = self.peek().span;
		Ok(AlterTableStmt {
			table,
			actions,
			span: start.merge(end),
		})
	}

	fn parse_user_host(&mut self) -> ParseResult<(String, Option<String>)> {
		let user = match &self.peek().kind {
			TokenKind::StringLiteral(s) => {
				let u = s.clone();
				self.bump();
				u
			}
			_ => self.parse_ident()?.0,
		};

		let host = if let TokenKind::UserVar(ref h) = self.peek().kind {
			let host_str = h.clone();
			self.bump();
			Some(host_str)
		} else if self.peek().kind == TokenKind::At {
			self.bump();
			let h = match &self.peek().kind {
				TokenKind::StringLiteral(s) => {
					let h_str = s.clone();
					self.bump();
					h_str
				}
				_ => self.parse_ident()?.0,
			};
			Some(h)
		} else {
			None
		};

		Ok((user, host))
	}

	/// Parse a CREATE USER statement.
	pub fn parse_create_user_stmt(&mut self) -> ParseResult<CreateUserStmt> {
		let start = self.expect_keyword(Keyword::Create, "CREATE")?.span;
		self.expect_keyword(Keyword::User, "USER")?;

		let if_not_exists = if self.peek_keyword() == Some(Keyword::If)
			&& self.peek_nth_keyword(1) == Some(Keyword::Not)
			&& self.peek_nth_keyword(2) == Some(Keyword::Exists)
		{
			self.bump();
			self.bump();
			self.bump();
			true
		} else {
			false
		};

		let (user, host) = self.parse_user_host()?;

		let identified_by = if self.peek_keyword() == Some(Keyword::Identified) {
			self.bump();
			if self.peek_keyword() == Some(Keyword::By) {
				self.bump();
			}
			match &self.peek().kind {
				TokenKind::StringLiteral(s) => {
					let pass = s.clone();
					self.bump();
					Some(pass)
				}
				_ => Some(self.parse_ident()?.0),
			}
		} else {
			None
		};

		let end = self.peek().span;
		Ok(CreateUserStmt {
			if_not_exists,
			user,
			host,
			identified_by,
			span: start.merge(end),
		})
	}

	/// Parse a DROP USER statement.
	pub fn parse_drop_user_stmt(&mut self) -> ParseResult<DropUserStmt> {
		let start = self.expect_keyword(Keyword::Drop, "DROP")?.span;
		self.expect_keyword(Keyword::User, "USER")?;

		let if_exists = if self.peek_keyword() == Some(Keyword::If)
			&& self.peek_nth_keyword(1) == Some(Keyword::Exists)
		{
			self.bump();
			self.bump();
			true
		} else {
			false
		};

		let mut users = Vec::new();
		loop {
			users.push(self.parse_user_host()?);
			if self.peek().kind == TokenKind::Comma {
				self.bump();
			} else {
				break;
			}
		}

		let end = self.peek().span;
		Ok(DropUserStmt {
			if_exists,
			users,
			span: start.merge(end),
		})
	}

	/// Parse a GRANT statement.
	pub fn parse_grant_stmt(&mut self) -> ParseResult<GrantStmt> {
		let start = self.expect_keyword(Keyword::Grant, "GRANT")?.span;
		let mut privileges = Vec::new();
		loop {
			if self.peek_keyword() == Some(Keyword::All) {
				self.bump();
				if let TokenKind::Ident(ref s) = self.peek().kind
					&& s.eq_ignore_ascii_case("privileges")
				{
					self.bump();
				}
				privileges.push("ALL".to_string());
			} else {
				privileges.push(self.parse_ident()?.0);
			}

			if self.peek().kind == TokenKind::Comma {
				self.bump();
			} else {
				break;
			}
		}

		let object = if self.peek_keyword() == Some(Keyword::On) {
			self.bump();
			let mut obj = String::new();
			while self.peek_keyword() != Some(Keyword::To)
				&& self.peek().kind != TokenKind::Semicolon
				&& self.peek().kind != TokenKind::Eof
			{
				let tok = self.bump();
				obj.push_str(&tok.kind.to_string());
			}
			Some(obj)
		} else {
			None
		};

		self.expect_keyword(Keyword::To, "TO in GRANT")?;
		let (user, host) = self.parse_user_host()?;

		let end = self.peek().span;
		Ok(GrantStmt {
			privileges,
			object,
			user,
			host,
			span: start.merge(end),
		})
	}

	/// Parse a REVOKE statement.
	pub fn parse_revoke_stmt(&mut self) -> ParseResult<RevokeStmt> {
		let start = self.expect_keyword(Keyword::Revoke, "REVOKE")?.span;
		let mut privileges = Vec::new();
		loop {
			if self.peek_keyword() == Some(Keyword::All) {
				self.bump();
				if let TokenKind::Ident(ref s) = self.peek().kind
					&& s.eq_ignore_ascii_case("privileges")
				{
					self.bump();
				}
				privileges.push("ALL".to_string());
			} else {
				privileges.push(self.parse_ident()?.0);
			}

			if self.peek().kind == TokenKind::Comma {
				self.bump();
			} else {
				break;
			}
		}

		let object = if self.peek_keyword() == Some(Keyword::On) {
			self.bump();
			let mut obj = String::new();
			while self.peek_keyword() != Some(Keyword::From)
				&& self.peek().kind != TokenKind::Semicolon
				&& self.peek().kind != TokenKind::Eof
			{
				let tok = self.bump();
				obj.push_str(&tok.kind.to_string());
			}
			Some(obj)
		} else {
			None
		};

		self.expect_keyword(Keyword::From, "FROM in REVOKE")?;
		let (user, host) = self.parse_user_host()?;

		let end = self.peek().span;
		Ok(RevokeStmt {
			privileges,
			object,
			user,
			host,
			span: start.merge(end),
		})
	}

	/// Parse a LOCK TABLES statement.
	pub fn parse_lock_tables_stmt(&mut self) -> ParseResult<LockTablesStmt> {
		let start = self.expect_keyword(Keyword::Lock, "LOCK")?.span;
		if self.peek_keyword() == Some(Keyword::Tables)
			|| self.peek_keyword() == Some(Keyword::Table)
		{
			self.bump();
		} else {
			self.expect_keyword(Keyword::Tables, "TABLES after LOCK")?;
		}

		let mut locks = Vec::new();
		loop {
			let table = self.parse_qualified_name()?;
			let lock_type = if self.peek_keyword() == Some(Keyword::Read) {
				self.bump();
				TableLockType::Read
			} else if self.peek_keyword() == Some(Keyword::Write) {
				self.bump();
				TableLockType::Write
			} else {
				TableLockType::Read
			};
			locks.push(TableLock {
				table,
				lock_type,
			});
			if self.peek().kind == TokenKind::Comma {
				self.bump();
			} else {
				break;
			}
		}

		let end = self.peek().span;
		Ok(LockTablesStmt {
			locks,
			span: start.merge(end),
		})
	}

	/// Parse an UNLOCK TABLES statement.
	pub fn parse_unlock_tables_stmt(&mut self) -> ParseResult<UnlockTablesStmt> {
		let start = self.expect_keyword(Keyword::Unlock, "UNLOCK")?.span;
		if self.peek_keyword() == Some(Keyword::Tables)
			|| self.peek_keyword() == Some(Keyword::Table)
		{
			self.bump();
		} else {
			self.expect_keyword(Keyword::Tables, "TABLES after UNLOCK")?;
		}
		let end = self.peek().span;
		Ok(UnlockTablesStmt {
			span: start.merge(end),
		})
	}

	/// Parse a PREPARE statement.
	pub fn parse_prepare_stmt(&mut self) -> ParseResult<PrepareStmt> {
		let start = self.expect_keyword(Keyword::Prepare, "PREPARE")?.span;
		let (name, _) = self.parse_ident()?;
		self.expect_keyword(Keyword::From, "FROM in PREPARE")?;
		let query = match &self.peek().kind {
			TokenKind::StringLiteral(s) => {
				let q = s.clone();
				self.bump();
				q
			}
			TokenKind::UserVar(v) => {
				let q = format!("@{v}");
				self.bump();
				q
			}
			_ => self.parse_ident()?.0,
		};
		let end = self.peek().span;
		Ok(PrepareStmt {
			name,
			query,
			span: start.merge(end),
		})
	}

	/// Parse an EXECUTE statement.
	pub fn parse_execute_stmt(&mut self) -> ParseResult<ExecuteStmt> {
		let start = self.expect_keyword(Keyword::Execute, "EXECUTE")?.span;
		let (name, _) = self.parse_ident()?;
		let mut using = Vec::new();
		if self.peek_keyword() == Some(Keyword::Using) {
			self.bump();
			loop {
				using.push(self.parse_expr()?);
				if self.peek().kind == TokenKind::Comma {
					self.bump();
				} else {
					break;
				}
			}
		}
		let end = self.peek().span;
		Ok(ExecuteStmt {
			name,
			using,
			span: start.merge(end),
		})
	}

	/// Parse a DEALLOCATE PREPARE statement.
	pub fn parse_deallocate_stmt(&mut self) -> ParseResult<DeallocateStmt> {
		let start = if self.peek_keyword() == Some(Keyword::Deallocate) {
			self.bump().span
		} else {
			self.expect_keyword(Keyword::Drop, "DROP or DEALLOCATE")?.span
		};
		self.expect_keyword(Keyword::Prepare, "PREPARE")?;
		let (name, _) = self.parse_ident()?;
		let end = self.peek().span;
		Ok(DeallocateStmt {
			name,
			span: start.merge(end),
		})
	}
}
