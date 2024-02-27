use reblessive::Ctx;

use crate::{
	sql::{statements::RelateStatement, Subquery, Value},
	syn::v2::{
		parser::{
			mac::{expected, unexpected},
			ParseResult, Parser,
		},
		token::t,
	},
};

impl Parser<'_> {
	pub async fn parse_relate_stmt(&mut self, mut ctx: Ctx<'_>) -> ParseResult<RelateStatement> {
		let only = self.eat(t!("ONLY"));
		let (kind, from, with) = ctx.run(|ctx| self.parse_relation(ctx)).await?;
		let uniq = self.eat(t!("UNIQUE"));

		let data = self.try_parse_data(&mut ctx).await?;
		let output = self.try_parse_output(&mut ctx).await?;
		let timeout = self.try_parse_timeout()?;
		let parallel = self.eat(t!("PARALLEL"));
		Ok(RelateStatement {
			only,
			kind,
			from,
			with,
			uniq,
			data,
			output,
			timeout,
			parallel,
		})
	}

	pub async fn parse_relation(&mut self, mut ctx: Ctx<'_>) -> ParseResult<(Value, Value, Value)> {
		let first = self.parse_relate_value(&mut ctx).await?;
		let is_o = match self.next().kind {
			t!("->") => true,
			t!("<-") => false,
			x => unexpected!(self, x, "a relation arrow"),
		};
		let kind = self.parse_thing_or_table(&mut ctx).await?;
		if is_o {
			expected!(self, t!("->"))
		} else {
			expected!(self, t!("<-"))
		};
		let second = self.parse_relate_value(&mut ctx).await?;
		if is_o {
			Ok((kind, first, second))
		} else {
			Ok((kind, second, first))
		}
	}

	pub async fn parse_relate_value(&mut self, ctx: &mut Ctx<'_>) -> ParseResult<Value> {
		match self.peek_kind() {
			t!("[") => {
				let start = self.pop_peek().span;
				self.parse_array(ctx, start).await.map(Value::Array)
			}
			t!("$param") => self.next_token_value().map(Value::Param),
			t!("RETURN")
			| t!("SELECT")
			| t!("CREATE")
			| t!("UPDATE")
			| t!("DELETE")
			| t!("RELATE")
			| t!("DEFINE")
			| t!("REMOVE") => {
				self.parse_inner_subquery(ctx, None).await.map(|x| Value::Subquery(Box::new(x)))
			}
			t!("IF") => {
				self.pop_peek();
				ctx.run(|ctx| self.parse_if_stmt(ctx))
					.await
					.map(|x| Value::Subquery(Box::new(Subquery::Ifelse(x))))
			}
			t!("(") => {
				let span = self.pop_peek().span;
				let res = self
					.parse_inner_subquery(ctx, Some(span))
					.await
					.map(|x| Value::Subquery(Box::new(x)))?;
				Ok(res)
			}
			_ => self.parse_thing(ctx).await.map(Value::Thing),
		}
	}

	pub async fn parse_thing_or_table(&mut self, ctx: &mut Ctx<'_>) -> ParseResult<Value> {
		if self.peek_token_at(1).kind == t!(":") {
			self.parse_thing(ctx).await.map(Value::Thing)
		} else {
			self.next_token_value().map(Value::Table)
		}
	}
}
