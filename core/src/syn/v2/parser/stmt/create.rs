use reblessive::Ctx;

use crate::{
	sql::{statements::CreateStatement, Values},
	syn::v2::{
		parser::{ParseResult, Parser},
		token::t,
	},
};

impl Parser<'_> {
	pub async fn parse_create_stmt(&mut self, mut ctx: Ctx<'_>) -> ParseResult<CreateStatement> {
		let only = self.eat(t!("ONLY"));
		let what = Values(ctx.run(|ctx| self.parse_what_list(ctx)).await?);
		let data = ctx.run(|ctx| self.try_parse_data(ctx)).await?;
		let output = ctx.run(|ctx| self.try_parse_output(ctx)).await?;
		let timeout = self.try_parse_timeout()?;
		let parallel = self.eat(t!("PARALLEL"));

		Ok(CreateStatement {
			only,
			what,
			data,
			output,
			timeout,
			parallel,
		})
	}
}
