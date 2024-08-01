use reblessive::Stk;

use crate::{
	sql::{
		statements::{
			AlterFieldStatement, AlterParamStatement, AlterStatement, AlterTableStatement,
		},
		Param, TableType,
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
	pub async fn parse_alter_stmt(&mut self, ctx: &mut Stk) -> ParseResult<AlterStatement> {
		match self.next().kind {
			t!("FIELD") => self.parse_alter_field(ctx).await.map(AlterStatement::Field),
			t!("PARAM") => self.parse_alter_param(ctx).await.map(AlterStatement::Param),
			t!("TABLE") => self.parse_alter_table(ctx).await.map(AlterStatement::Table),
			x => unexpected!(self, x, "a alter statement keyword"),
		}
	}

	pub async fn parse_alter_table(&mut self, ctx: &mut Stk) -> ParseResult<AlterTableStatement> {
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
					if self.eat(t!("UNSET")) {
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
						self.eat(t!("true"));
						res.drop = Some(true);
					}
				}
				t!("TYPE") => {
					self.pop_peek();
					match self.peek_kind() {
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
						x => unexpected!(self, x, "`NORMAL`, `RELATION`, or `ANY`"),
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
					if self.eat(t!("UNSET")) {
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

	pub async fn parse_alter_field(&mut self, ctx: &mut Stk) -> ParseResult<AlterFieldStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.parse_local_idiom()?;
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
				t!("FLEXIBLE") => {
					self.pop_peek();
					if self.eat(t!("false")) {
						res.flex = Some(false);
					} else {
						self.eat(t!("true"));
						res.flex = Some(true);
					}
				}
				t!("TYPE") => {
					self.pop_peek();
					if self.eat(t!("UNSET")) {
						res.kind = Some(None);
					} else {
						res.kind = Some(Some(ctx.run(|ctx| self.parse_inner_kind(ctx)).await?));
					}
				}
				t!("READONLY") => {
					self.pop_peek();
					if self.eat(t!("false")) {
						res.readonly = Some(false);
					} else {
						self.eat(t!("true"));
						res.readonly = Some(true);
					}
				}
				t!("VALUE") => {
					self.pop_peek();
					if self.eat(t!("UNSET")) {
						res.value = Some(None);
					} else {
						res.value = Some(Some(ctx.run(|ctx| self.parse_value(ctx)).await?));
					}
				}
				t!("ASSERT") => {
					self.pop_peek();
					if self.eat(t!("UNSET")) {
						res.assert = Some(None);
					} else {
						res.assert = Some(Some(ctx.run(|ctx| self.parse_value(ctx)).await?));
					}
				}
				t!("DEFAULT") => {
					self.pop_peek();
					if self.eat(t!("UNSET")) {
						res.default = Some(None);
					} else {
						res.default = Some(Some(ctx.run(|ctx| self.parse_value(ctx)).await?));
					}
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = Some(ctx.run(|ctx| self.parse_permission(ctx, false)).await?);
				}
				t!("COMMENT") => {
					self.pop_peek();
					if self.eat(t!("UNSET")) {
						res.comment = Some(None);
					} else {
						res.comment = Some(Some(self.next_token_value()?));
					}
				}
				_ => break,
			}
		}

		Ok(res)
	}

	pub async fn parse_alter_param(&mut self, ctx: &mut Stk) -> ParseResult<AlterParamStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let name = self.next_token_value::<Param>()?.0;

		let mut res = AlterParamStatement {
			name,
			if_exists,
			..Default::default()
		};

		loop {
			match self.peek_kind() {
				t!("VALUE") => {
					self.pop_peek();
					res.value = Some(ctx.run(|ctx| self.parse_value(ctx)).await?);
				}
				t!("PERMISSIONS") => {
					self.pop_peek();
					res.permissions = Some(ctx.run(|ctx| self.parse_permission_value(ctx)).await?);
				}
				t!("COMMENT") => {
					self.pop_peek();
					if self.eat(t!("UNSET")) {
						res.comment = Some(None);
					} else {
						res.comment = Some(Some(self.next_token_value()?));
					}
				}
				_ => break,
			}
		}

		Ok(res)
	}
}
