//! PostgreSQL DDL statements parser (`CREATE TABLE`, `DROP`).
//!
//! gram.y: CreateStmt, DropStmt

use crate::ast::stmt::{
	ColumnConstraint, ColumnDef, CreateTableStmt, DropObjectType, DropStmt, TableConstraint,
};
use crate::error::{ErrorKind, ParseError, ParseResult};
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;

impl<'a> Parser<'a> {
	/// Parse a PostgreSQL `CREATE TABLE` statement.
	///
	/// gram.y: CreateStmt
	pub fn parse_create_table_stmt(&mut self) -> ParseResult<CreateTableStmt> {
		let create_tok = self.expect(&TokenKind::Keyword(Keyword::Create), "'CREATE'")?;

		// Optional TEMPORARY / TEMP
		let temporary = self.eat(&TokenKind::Keyword(Keyword::Temporary))?
			|| self.eat(&TokenKind::Keyword(Keyword::Temp))?;

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
			// Check if table constraint
			if self.peek().kind == TokenKind::Keyword(Keyword::Primary) {
				self.bump()?; // consume PRIMARY
				self.expect(&TokenKind::Keyword(Keyword::Key), "'KEY'")?;
				let cols = self.parse_column_name_list()?;
				constraints.push(TableConstraint::PrimaryKey(cols));
			} else if self.peek().kind == TokenKind::Keyword(Keyword::Unique) {
				self.bump()?; // consume UNIQUE
				let cols = self.parse_column_name_list()?;
				constraints.push(TableConstraint::Unique(cols));
			} else if self.peek().kind == TokenKind::Keyword(Keyword::Foreign) {
				self.bump()?; // consume FOREIGN
				self.expect(&TokenKind::Keyword(Keyword::Key), "'KEY'")?;
				let columns_list = self.parse_column_name_list()?;
				self.expect(&TokenKind::Keyword(Keyword::References), "'REFERENCES'")?;
				let ref_table = self.parse_qualified_name()?;
				let ref_columns = if self.peek().kind == TokenKind::LParen {
					self.parse_column_name_list()?
				} else {
					Vec::new()
				};
				constraints.push(TableConstraint::ForeignKey {
					columns: columns_list,
					ref_table,
					ref_columns,
				});
			} else if self.peek().kind == TokenKind::Keyword(Keyword::Constraint) {
				self.bump()?; // consume CONSTRAINT
				let (_constraint_name, _) = self.parse_ident()?;
				// Continue to constraint definition
				if self.eat(&TokenKind::Keyword(Keyword::Primary))? {
					self.expect(&TokenKind::Keyword(Keyword::Key), "'KEY'")?;
					let cols = self.parse_column_name_list()?;
					constraints.push(TableConstraint::PrimaryKey(cols));
				} else if self.eat(&TokenKind::Keyword(Keyword::Unique))? {
					let cols = self.parse_column_name_list()?;
					constraints.push(TableConstraint::Unique(cols));
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
					constraints.push(TableConstraint::ForeignKey {
						columns: columns_list,
						ref_table,
						ref_columns,
					});
				}
			} else {
				// Column definition
				let (col_name, col_start_span) = self.parse_ident()?;
				let data_type = self.parse_type_name()?;
				let mut col_constraints = Vec::new();
				let mut col_end_span = data_type.span;

				while self.peek().kind != TokenKind::Comma && self.peek().kind != TokenKind::RParen
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
						col_constraints.push(ColumnConstraint::References {
							table: ref_table,
							column,
						});
					} else {
						break;
					}
				}

				columns.push(ColumnDef {
					name: col_name,
					data_type,
					constraints: col_constraints,
					span: col_start_span.merge(col_end_span),
				});
			}

			if !self.eat(&TokenKind::Comma)? {
				break;
			}
		}

		let r_paren = self.expect(&TokenKind::RParen, "')'")?;
		let span = create_tok.span.merge(r_paren.span);

		Ok(CreateTableStmt {
			name,
			if_not_exists,
			temporary,
			columns,
			constraints,
			span,
		})
	}

	/// Parse a PostgreSQL `DROP` statement (`DROP TABLE ...`, `DROP INDEX ...`).
	///
	/// gram.y: DropStmt
	pub fn parse_drop_stmt(&mut self) -> ParseResult<DropStmt> {
		let drop_tok = self.expect(&TokenKind::Keyword(Keyword::Drop), "'DROP'")?;

		let object_type = match &self.peek().kind {
			TokenKind::Keyword(Keyword::Table) => {
				self.bump()?;
				DropObjectType::Table
			}
			TokenKind::Keyword(Keyword::Index) => {
				self.bump()?;
				DropObjectType::Index
			}
			TokenKind::Keyword(Keyword::View) => {
				self.bump()?;
				DropObjectType::View
			}
			TokenKind::Keyword(Keyword::Schema) => {
				self.bump()?;
				DropObjectType::Schema
			}
			TokenKind::Keyword(Keyword::Type) => {
				self.bump()?;
				DropObjectType::Type
			}
			_ => {
				return Err(ParseError::new(
					self.peek().span,
					ErrorKind::UnexpectedToken {
						expected: "TABLE, INDEX, VIEW, SCHEMA, or TYPE",
						found: self.peek().kind.to_string(),
					},
				));
			}
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

		Ok(DropStmt {
			object_type,
			if_exists,
			names,
			cascade,
			span,
		})
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
