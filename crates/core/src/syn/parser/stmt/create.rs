use reblessive::Stk;

use crate::{
	sql::{statements::CreateStatement, Values},
	syn::{
		parser::{ParseResult, Parser},
		token::t,
	},
};

impl Parser<'_> {
	pub async fn parse_create_stmt(&mut self, ctx: &mut Stk) -> ParseResult<CreateStatement> {
		let only = self.eat(t!("ONLY"));
		let what = Values(self.parse_what_list(ctx).await?);
		let data = self.try_parse_data(ctx).await?;
		let output = self.try_parse_output(ctx).await?;
		let version = self.try_parse_version()?;
		let timeout = self.try_parse_timeout()?;
		let parallel = self.eat(t!("PARALLEL"));

		Ok(CreateStatement {
			only,
			what,
			data,
			output,
			timeout,
			parallel,
			version,
		})
	}
}
