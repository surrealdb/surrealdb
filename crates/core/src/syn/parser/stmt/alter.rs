use reblessive::Stk;

use crate::sql::statements::alter::{AlterFieldStatement, AlterSequenceStatement};
use crate::syn::error::bail;
use crate::{
	sql::{
		statements::{AlterStatement, AlterTableStatement},
		TableType,
	},
	syn::{
		parser::{
			mac::{expected, unexpected},
			ParseResult, Parser,
		},
		token::t,
	},
};

impl Parser<'_> {
	pub(crate) async fn parse_alter_stmt(&mut self, ctx: &mut Stk) -> ParseResult<AlterStatement> {
		let next = self.next();
		match next.kind {
			t!("TABLE") => self.parse_alter_table(ctx).await.map(AlterStatement::Table),
			t!("FIELD") => self.parse_alter_field(ctx).await.map(AlterStatement::Field),
			t!("SEQUENCE") => self.parse_alter_sequence().await.map(AlterStatement::Sequence),
			_ => unexpected!(self, next, "a alter statement keyword"),
		}
	}

	pub(crate) async fn parse_alter_table(
		&mut self,
		ctx: &mut Stk,
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
				t!("COMMENT") => {
					self.pop_peek();
					if self.eat(t!("NONE")) {
						res.comment = Some(None);
					} else {
						res.comment = Some(Some(self.next_token_value()?));
					}
				}
				t!("DROP") => {
					self.pop_peek();
					if self.eat(t!("false")) {
						res.drop = Some(false);
					} else {
						res.drop = Some(true);
					}
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
					res.full = Some(false);
				}
				t!("SCHEMAFULL") => {
					self.pop_peek();
					res.full = Some(true);
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = Some(ctx.run(|ctx| self.parse_permission(ctx, false)).await?);
				}
				t!("CHANGEFEED") => {
					self.pop_peek();
					if self.eat(t!("NONE")) {
						res.changefeed = Some(None);
					} else {
						res.changefeed = Some(Some(self.parse_changefeed()?));
					}
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub(crate) async fn parse_alter_field(
		&mut self,
		ctx: &mut Stk,
	) -> ParseResult<AlterFieldStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_local_idiom(ctx).await?;
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
			let drop = self.eat(t!("DROP"));
			let peek = self.peek();
			match peek.kind {
				t!("FLEXIBLE") => {
					self.pop_peek();
					res.flex = Some(!drop)
				}
				t!("TYPE") => {
					self.pop_peek();
					res.kind = Some(if drop {
						None
					} else {
						Some(ctx.run(|ctx| self.parse_inner_kind(ctx)).await?)
					});
				}
				t!("READONLY") => {
					self.pop_peek();
					res.flex = Some(!drop)
				}
				t!("VALUE") => {
					self.pop_peek();
					res.value = Some(if drop {
						None
					} else {
						Some(ctx.run(|ctx| self.parse_value_field(ctx)).await?)
					});
				}
				t!("ASSERT") => {
					self.pop_peek();
					res.value = Some(if drop {
						None
					} else {
						Some(ctx.run(|ctx| self.parse_value_field(ctx)).await?)
					});
				}
				t!("DEFAULT") => {
					self.pop_peek();

					if drop {
						res.default = Some(None);
					} else {
						res.default_always = Some(self.eat(t!("ALWAYS")));
						res.default = Some(Some(ctx.run(|ctx| self.parse_value_field(ctx)).await?));
					}
				}
				t!("PERMISSIONS") if !drop => {
					self.pop_peek();
					res.permissions = Some(ctx.run(|ctx| self.parse_permission(ctx, false)).await?);
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = Some(if drop {
						None
					} else {
						Some(self.next_token_value()?)
					})
				}
				t!("REFERENCE") => {
					if !self.settings.references_enabled {
						bail!(
							"Experimental capability `record_references` is not enabled",
							@self.last_span() => "Use of `REFERENCE` keyword is still experimental"
						)
					}

					self.pop_peek();
					res.reference = Some(if drop {
						None
					} else {
						Some(self.parse_reference(ctx).await?)
					});
				}
				_ if drop => {
					unexpected!(self, peek, "`FLEXIBLE`, `TYPE`, `READONLY`, `VALUE`, `ASSERT`, `DEFAULT`, `COMMENT`, or `REFERENCE`")
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
