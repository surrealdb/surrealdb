use reblessive::Stk;

use crate::{
	sql::statements::IfelseStatement,
	syn::{
		parser::{
			mac::{expected, unexpected},
			ParseResult, Parser,
		},
		token::t,
	},
};

impl Parser<'_> {
	pub async fn parse_if_stmt(&mut self, ctx: &mut Stk) -> ParseResult<IfelseStatement> {
		let condition = ctx.run(|ctx| self.parse_value_field(ctx)).await?;

		let mut res = IfelseStatement {
			exprs: Vec::new(),
			close: None,
		};

		let next = self.next();
		match next.kind {
			t!("THEN") => {
				let body = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
				self.eat(t!(";"));
				res.exprs.push((condition, body));
				self.parse_worded_tail(ctx, &mut res).await?;
			}
			t!("{") => {
				let body = self.parse_block(ctx, next.span).await?;
				res.exprs.push((condition, body.into()));
				self.parse_bracketed_tail(ctx, &mut res).await?;
			}
			x => unexpected!(self, x, "THEN or '{'"),
		}

		Ok(res)
	}

	async fn parse_worded_tail(
		&mut self,
		ctx: &mut Stk,
		res: &mut IfelseStatement,
	) -> ParseResult<()> {
		loop {
			match self.next().kind {
				t!("END") => return Ok(()),
				t!("ELSE") => {
					if self.eat(t!("IF")) {
						let condition = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
						expected!(self, t!("THEN"));
						let body = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
						self.eat(t!(";"));
						res.exprs.push((condition, body));
					} else {
						let value = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
						self.eat(t!(";"));
						expected!(self, t!("END"));
						res.close = Some(value);
						return Ok(());
					}
				}
				x => unexpected!(self, x, "if to end"),
			}
		}
	}

	async fn parse_bracketed_tail(
		&mut self,
		ctx: &mut Stk,
		res: &mut IfelseStatement,
	) -> ParseResult<()> {
		loop {
			match self.peek_kind() {
				t!("ELSE") => {
					self.pop_peek();
					if self.eat(t!("IF")) {
						let condition = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
						let span = expected!(self, t!("{")).span;
						let body = self.parse_block(ctx, span).await?;
						res.exprs.push((condition, body.into()));
					} else {
						let span = expected!(self, t!("{")).span;
						let value = self.parse_block(ctx, span).await?;
						res.close = Some(value.into());
						return Ok(());
					}
				}
				_ => return Ok(()),
			}
		}
	}
}
