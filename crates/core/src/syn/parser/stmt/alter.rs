use reblessive::Stk;

use crate::sql::TableType;
use crate::sql::statements::alter::field::AlterDefault;
use crate::sql::statements::alter::{AlterFieldStatement, AlterKind, AlterSequenceStatement};
use crate::sql::statements::{AlterStatement, AlterTableStatement};
use crate::syn::error::bail;
use crate::syn::parser::mac::{expected, unexpected};
use crate::syn::parser::{ParseResult, Parser};
use crate::syn::token::t;

impl Parser<'_> {
	pub(crate) async fn parse_alter_stmt(&mut self, stk: &mut Stk) -> ParseResult<AlterStatement> {
		let next = self.next();
		match next.kind {
			t!("TABLE") => self.parse_alter_table(stk).await.map(AlterStatement::Table),
			t!("FIELD") => self.parse_alter_field(stk).await.map(AlterStatement::Field),
			t!("SEQUENCE") => self.parse_alter_sequence().await.map(AlterStatement::Sequence),
			_ => unexpected!(self, next, "a alter statement keyword"),
		}
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
		let name = self.next_token_value()?;
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
					res.comment = AlterKind::Set(self.next_token_value()?);
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
		let name = self.parse_local_idiom(stk).await?;
		expected!(self, t!("ON"));
		self.eat(t!("TABLE"));
		let what = self.next_token_value()?;
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
						t!("FLEXIBLE") => {
							self.pop_peek();
							res.flex = AlterKind::Drop;
						}
						t!("TYPE") => {
							self.pop_peek();
							res.kind = AlterKind::Drop;
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
							if !self.settings.references_enabled {
								bail!(
									"Experimental capability `record_references` is not enabled",
									@self.last_span() => "Use of `REFERENCE` keyword is still experimental"
								)
							}

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
				t!("FLEXIBLE") => {
					self.pop_peek();
					res.flex = AlterKind::Set(());
				}
				t!("TYPE") => {
					self.pop_peek();
					res.kind = AlterKind::Set(stk.run(|stk| self.parse_inner_kind(stk)).await?);
				}
				t!("READONLY") => {
					self.pop_peek();
					res.flex = AlterKind::Set(());
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
					res.comment = AlterKind::Set(self.next_token_value()?);
				}
				t!("REFERENCE") => {
					if !self.settings.references_enabled {
						bail!(
							"Experimental capability `record_references` is not enabled",
							@self.last_span() => "Use of `REFERENCE` keyword is still experimental"
						)
					}

					self.pop_peek();
					res.reference = AlterKind::Set(self.parse_reference(stk).await?);
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub(crate) async fn parse_alter_sequence(&mut self) -> ParseResult<AlterSequenceStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.next_token_value()?;
		let mut res = AlterSequenceStatement {
			name,
			if_exists,
			..Default::default()
		};

		if let Some(to) = self.try_parse_timeout()? {
			res.timeout = Some(to);
		}

		Ok(res)
	}
}
