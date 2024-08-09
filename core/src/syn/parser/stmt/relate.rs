use reblessive::Stk;

use crate::{
	sql::{statements::RelateStatement, Subquery, Value},
	syn::{
		parser::{
			mac::{expected, unexpected},
			ParseResult, Parser,
		},
		token::t,
	},
};

impl Parser<'_> {
	pub async fn parse_relate_stmt(&mut self, stk: &mut Stk) -> ParseResult<RelateStatement> {
		let only = self.eat(t!("ONLY"));
		let (kind, from, with) = stk.run(|stk| self.parse_relation(stk)).await?;
		let uniq = self.eat(t!("UNIQUE"));

		let data = self.try_parse_data(stk).await?;
		let output = self.try_parse_output(stk).await?;
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

	pub async fn parse_relation(&mut self, stk: &mut Stk) -> ParseResult<(Value, Value, Value)> {
		let first = self.parse_relate_value(stk).await?;
		let is_o = match self.next().kind {
			t!("->") => true,
			t!("<-") => false,
			x => unexpected!(self, x, "a relation arrow"),
		};
		let kind = self.parse_relate_kind(stk).await?;
		if is_o {
			expected!(self, t!("->"))
		} else {
			expected!(self, t!("<-"))
		};
		let second = self.parse_relate_value(stk).await?;
		if is_o {
			Ok((kind, first, second))
		} else {
			Ok((kind, second, first))
		}
	}

	pub async fn parse_relate_kind(&mut self, ctx: &mut Stk) -> ParseResult<Value> {
		match self.peek_kind() {
			t!("$param") => self.next_token_value().map(Value::Param),
			t!("(") => {
				let span = self.pop_peek().span;
				let res = self
					.parse_inner_subquery(ctx, Some(span))
					.await
					.map(|x| Value::Subquery(Box::new(x)))?;
				Ok(res)
			}
			_ => self.parse_thing_or_table(ctx).await,
		}
	}
	pub async fn parse_relate_value(&mut self, ctx: &mut Stk) -> ParseResult<Value> {
		match self.peek_kind() {
			t!("[") => {
				let start = self.pop_peek().span;
				self.parse_array(ctx, start).await.map(Value::Array)
			}
			t!("$param") => self.next_token_value().map(Value::Param),
			t!("RETURN")
			| t!("SELECT")
			| t!("CREATE")
			| t!("UPSERT")
			| t!("UPDATE")
			| t!("DELETE")
			| t!("RELATE")
			| t!("DEFINE")
			| t!("ALTER")
			| t!("REMOVE")
			| t!("REBUILD") => {
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

	pub async fn parse_thing_or_table(&mut self, ctx: &mut Stk) -> ParseResult<Value> {
		self.glue()?;
		if self.peek_token_at(1).kind == t!(":") {
			self.parse_thing(ctx).await.map(Value::Thing)
		} else {
			self.next_token_value().map(Value::Table)
		}
	}
}
