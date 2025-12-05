use reblessive::Stk;

use crate::sql::TableType;
use crate::sql::statements::alter::field::AlterDefault;
use crate::sql::statements::alter::{
	AlterDatabaseStatement, AlterFieldStatement, AlterIndexStatement, AlterKind,
	AlterNamespaceStatement, AlterSequenceStatement, AlterSystemStatement,
};
use crate::sql::statements::{AlterStatement, AlterTableStatement};
use crate::syn::parser::mac::{expected, unexpected};
use crate::syn::parser::{ParseResult, Parser};
use crate::syn::token::t;

impl Parser<'_> {
	pub(crate) async fn parse_alter_stmt(&mut self, stk: &mut Stk) -> ParseResult<AlterStatement> {
		let next = self.next();
		match next.kind {
			t!("SYSTEM") => self.parse_alter_system(stk).await.map(AlterStatement::System),
			t!("NAMESPACE") => self.parse_alter_namespace().await.map(AlterStatement::Namespace),
			t!("DATABASE") => self.parse_alter_database().await.map(AlterStatement::Database),
			t!("TABLE") => self.parse_alter_table(stk).await.map(AlterStatement::Table),
			t!("INDEX") => self.parse_alter_index().await.map(AlterStatement::Index),
			t!("FIELD") => self.parse_alter_field(stk).await.map(AlterStatement::Field),
			t!("SEQUENCE") => self.parse_alter_sequence(stk).await.map(AlterStatement::Sequence),
			_ => unexpected!(self, next, "a alter statement keyword"),
		}
	}

	pub(crate) async fn parse_alter_system(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<AlterSystemStatement> {
		let mut res = AlterSystemStatement::default();

		loop {
			match self.peek_kind() {
				t!("DROP") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("QUERY_TIMEOUT") => {
							self.pop_peek();
							res.query_timeout = AlterKind::Drop;
						}
						_ => {
							unexpected!(self, peek, "`QUERY_TIMEOUT`")
						}
					}
				}
				t!("QUERY_TIMEOUT") => {
					self.pop_peek();
					let duration = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
					res.query_timeout = AlterKind::Set(duration);
				}
				t!("COMPACT") => {
					self.pop_peek();
					res.compact = true;
				}
				o => {
					println!("{o}");
					break;
				}
			}
		}

		if !res.compact && matches!(res.query_timeout, AlterKind::None) {
			unexpected!(self, self.peek(), "`COMPACT`, `DROP` or `QUERY_TIMEOUT`")
		}
		Ok(res)
	}

	pub(crate) async fn parse_alter_namespace(&mut self) -> ParseResult<AlterNamespaceStatement> {
		if !self.eat(t!("COMPACT")) {
			unexpected!(self, self.peek(), "`COMPACT`")
		}
		Ok(AlterNamespaceStatement {
			compact: true,
		})
	}

	pub(crate) async fn parse_alter_database(&mut self) -> ParseResult<AlterDatabaseStatement> {
		if !self.eat(t!("COMPACT")) {
			unexpected!(self, self.peek(), "`COMPACT`")
		}
		Ok(AlterDatabaseStatement {
			compact: true,
		})
	}

	pub(crate) async fn parse_alter_table(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<AlterTableStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_ident()?;
		let mut res = AlterTableStatement {
			name,
			if_exists,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("DROP") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("COMMENT") => {
							self.pop_peek();
							res.comment = AlterKind::Drop;
						}
						t!("CHANGEFEED") => {
							self.pop_peek();
							res.changefeed = AlterKind::Drop;
						}
						_ => {
							unexpected!(self, peek, "`COMMENT` or `CHANGEFEED`")
						}
					}
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = AlterKind::Set(self.parse_string_lit()?);
				}
				t!("COMPACT") => {
					self.pop_peek();
					res.compact = true;
				}
				t!("TYPE") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("NORMAL") => {
							self.pop_peek();
							res.kind = Some(TableType::Normal);
						}
						t!("RELATION") => {
							self.pop_peek();
							res.kind = Some(TableType::Relation(self.parse_relation_schema()?));
						}
						t!("ANY") => {
							self.pop_peek();
							res.kind = Some(TableType::Any);
						}
						_ => unexpected!(self, peek, "`NORMAL`, `RELATION`, or `ANY`"),
					}
				}
				t!("SCHEMALESS") => {
					self.pop_peek();
					res.schemafull = AlterKind::Drop;
				}
				t!("SCHEMAFULL") => {
					self.pop_peek();
					res.schemafull = AlterKind::Set(());
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = Some(stk.run(|stk| self.parse_permission(stk, false)).await?);
				}
				t!("CHANGEFEED") => {
					self.pop_peek();
					res.changefeed = AlterKind::Set(self.parse_changefeed()?)
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub(crate) async fn parse_alter_index(&mut self) -> ParseResult<AlterIndexStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_ident()?;
		expected!(self, t!("ON"));
		self.eat(t!("TABLE"));
		let table = self.parse_ident()?;

		let mut res = AlterIndexStatement {
			name,
			table,
			if_exists,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("DROP") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("COMMENT") => {
							self.pop_peek();
							res.comment = AlterKind::Drop;
						}
						_ => {
							unexpected!(self, peek, "`COMMENT`")
						}
					}
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = AlterKind::Set(self.parse_string_lit()?);
				}
				t!("PREPARE") => {
					self.pop_peek();
					self.eat(t!("REMOVE"));
					res.prepare_remove = true;
				}
				_ => break,
			}
		}

		if !res.prepare_remove && matches!(res.comment, AlterKind::None) {
			unexpected!(self, self.peek(), "`PREPARE`, `DROP` or `COMMENT`")
		}
		Ok(res)
	}

	pub(crate) async fn parse_alter_field(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<AlterFieldStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_local_idiom()?;
		expected!(self, t!("ON"));
		self.eat(t!("TABLE"));
		let what = self.parse_ident()?;
		let mut res = AlterFieldStatement {
			name,
			what,
			if_exists,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("DROP") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("TYPE") => {
							self.pop_peek();
							res.kind = AlterKind::Drop;
						}
						t!("FLEXIBLE") => {
							self.pop_peek();
							res.flexible = AlterKind::Drop;
						}
						t!("READONLY") => {
							self.pop_peek();
							res.readonly = AlterKind::Drop;
						}
						t!("VALUE") => {
							self.pop_peek();
							res.value = AlterKind::Drop;
						}
						t!("ASSERT") => {
							self.pop_peek();
							res.assert = AlterKind::Drop;
						}
						t!("DEFAULT") => {
							self.pop_peek();
							res.default = AlterDefault::Drop;
						}
						t!("COMMENT") => {
							self.pop_peek();
							res.comment = AlterKind::Drop;
						}
						t!("REFERENCE") => {
							self.pop_peek();
							res.reference = AlterKind::Drop;
						}
						_ => {
							unexpected!(
								self,
								peek,
								"`FLEXIBLE`, `TYPE`, `READONLY`, `VALUE`, `ASSERT`, `DEFAULT`, `COMMENT`, or `REFERENCE`"
							)
						}
					}
				}
				t!("TYPE") => {
					self.pop_peek();
					res.kind = AlterKind::Set(stk.run(|stk| self.parse_inner_kind(stk)).await?);
				}
				t!("FLEXIBLE") => {
					self.pop_peek();
					res.flexible = AlterKind::Set(());
				}
				t!("READONLY") => {
					self.pop_peek();
					res.readonly = AlterKind::Set(());
				}
				t!("VALUE") => {
					self.pop_peek();
					res.value = AlterKind::Set(stk.run(|stk| self.parse_expr_field(stk)).await?);
				}
				t!("ASSERT") => {
					self.pop_peek();
					res.assert = AlterKind::Set(stk.run(|stk| self.parse_expr_field(stk)).await?);
				}
				t!("DEFAULT") => {
					self.pop_peek();
					if self.eat(t!("ALWAYS")) {
						res.default =
							AlterDefault::Always(stk.run(|stk| self.parse_expr_field(stk)).await?);
					} else {
						res.default =
							AlterDefault::Set(stk.run(|stk| self.parse_expr_field(stk)).await?);
					}
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = Some(stk.run(|stk| self.parse_permission(stk, false)).await?);
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = AlterKind::Set(self.parse_string_lit()?);
				}
				t!("REFERENCE") => {
					self.pop_peek();
					res.reference = AlterKind::Set(self.parse_reference(stk).await?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub(crate) async fn parse_alter_sequence(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<AlterSequenceStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_ident()?;
		let mut res = AlterSequenceStatement {
			name,
			if_exists,
			..Default::default()
		};

		if self.eat(t!("TIMEOUT")) {
			res.timeout = Some(stk.run(|stk| self.parse_expr_field(stk)).await?);
		}

		Ok(res)
	}
}
