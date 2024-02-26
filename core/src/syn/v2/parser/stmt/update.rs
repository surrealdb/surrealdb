use reblessive::Ctx;

use crate::{
	sql::{statements::UpdateStatement, Values},
	syn::v2::{
		parser::{ParseResult, Parser},
		token::t,
	},
};

impl Parser<'_> {
	pub async fn parse_update_stmt(&mut self, mut ctx: Ctx<'_>) -> ParseResult<UpdateStatement> {
		let only = self.eat(t!("ONLY"));
		let what = Values(ctx.run(|ctx| self.parse_what_list(ctx)).await?);
		let data = ctx.run(|ctx| self.try_parse_data(ctx)).await?;
		let cond = ctx.run(|ctx| self.try_parse_condition(ctx)).await?;
		let output = ctx.run(|ctx| self.try_parse_output(ctx)).await?;
		let timeout = self.try_parse_timeout()?;
		let parallel = self.eat(t!("PARALLEL"));

		Ok(UpdateStatement {
			only,
			what,
			data,
			cond,
			output,
			timeout,
			parallel,
		})
	}
}
