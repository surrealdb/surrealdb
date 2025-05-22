use reblessive::Stk;

use crate::sql::statements::alter::{AlterFieldStatement, AlterSequenceStatement};
use crate::syn::error::bail;
use crate::{
	sql::{
		TableType,
		statements::{AlterStatement, AlterTableStatement},
	},
	syn::{
		parser::{
			ParseResult, Parser,
			mac::{expected, unexpected},
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
				t!("DROP") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("COMMENT") => {
							self.pop_peek();
							res.comment = Some(None);
						}
						t!("CHANGEFEED") => {
							self.pop_peek();
							res.changefeed = Some(None);
						}
						_ => {
							unexpected!(self, peek, "`COMMENT` or `CHANGEFEED`")
						}
					}
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = Some(Some(self.next_token_value()?))
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
					res.changefeed = Some(Some(self.parse_changefeed()?))
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
			match self.peek_kind() {
				t!("DROP") => {
					self.pop_peek();
					let peek = self.peek();
					match peek.kind {
						t!("FLEXIBLE") => {
							self.pop_peek();
							res.flex = Some(false);
						}
						t!("TYPE") => {
							self.pop_peek();
							res.kind = Some(None);
						}
						t!("READONLY") => {
							self.pop_peek();
							res.readonly = Some(false);
						}
						t!("VALUE") => {
							self.pop_peek();
							res.value = Some(None);
						}
						t!("ASSERT") => {
							self.pop_peek();
							res.assert = Some(None);
						}
						t!("DEFAULT") => {
							self.pop_peek();
							res.default = Some(None);
							res.default_always = Some(false);
						}
						t!("COMMENT") => {
							self.pop_peek();
							res.comment = Some(None);
						}
						t!("REFERENCE") => {
							if !self.settings.references_enabled {
								bail!(
									"Experimental capability `record_references` is not enabled",
									@self.last_span() => "Use of `REFERENCE` keyword is still experimental"
								)
							}

							self.pop_peek();
							res.reference = Some(None);
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
					res.flex = Some(true)
				}
				t!("TYPE") => {
					self.pop_peek();
					res.kind = Some(Some(ctx.run(|ctx| self.parse_inner_kind(ctx)).await?));
				}
				t!("READONLY") => {
					self.pop_peek();
					res.flex = Some(true)
				}
				t!("VALUE") => {
					self.pop_peek();
					res.value = Some(Some(ctx.run(|ctx| self.parse_value_field(ctx)).await?));
				}
				t!("ASSERT") => {
					self.pop_peek();
					res.assert = Some(Some(ctx.run(|ctx| self.parse_value_field(ctx)).await?));
				}
				t!("DEFAULT") => {
					self.pop_peek();
					res.default_always = Some(self.eat(t!("ALWAYS")));
					res.default = Some(Some(ctx.run(|ctx| self.parse_value_field(ctx)).await?));
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = Some(ctx.run(|ctx| self.parse_permission(ctx, false)).await?);
				}
				t!("COMMENT") => {
					self.pop_peek();
					res.comment = Some(Some(self.next_token_value()?))
				}
				t!("REFERENCE") => {
					if !self.settings.references_enabled {
						bail!(
							"Experimental capability `record_references` is not enabled",
							@self.last_span() => "Use of `REFERENCE` keyword is still experimental"
						)
					}

					self.pop_peek();
					res.reference = Some(Some(self.parse_reference(ctx).await?));
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
