//! DDL statement parser (CREATE, DROP, USE, SET) for MySQL.

use crate::ast::stmt::{
	ColumnDef, CreateTableStmt, DropTableStmt, SetStmt, TableConstraint, UseStmt, VariableTarget,
};
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;
use crate::parser::error::{ErrorKind, ParseError, ParseResult};

impl<'a> Parser<'a> {
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
			// Check table constraints: PRIMARY KEY, UNIQUE, FOREIGN KEY, CONSTRAINT
			if self.peek_keyword() == Some(Keyword::Primary) {
				let c_start = self.bump().span;
				self.expect_keyword(Keyword::Key, "KEY after PRIMARY")?;
				let cols = self.parse_column_name_list()?;
				let c_end = self.peek().span;
				constraints.push(TableConstraint::PrimaryKey {
					name: None,
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
				let cols = self.parse_column_name_list()?;
				let c_end = self.peek().span;
				constraints.push(TableConstraint::Unique {
					name: None,
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
					name: None,
					columns: cols,
					ref_table,
					ref_columns: ref_cols,
					on_delete: None,
					on_update: None,
					span: c_start.merge(c_end),
				});
			} else {
				// Column definition
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
				columns.push(ColumnDef {
					name: col_name,
					data_type,
					not_null,
					default,
					auto_increment,
					primary_key,
					unique,
					span: c_start.merge(c_end),
				});
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
}
