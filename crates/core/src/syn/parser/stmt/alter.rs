use reblessive::Stk;

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
}
