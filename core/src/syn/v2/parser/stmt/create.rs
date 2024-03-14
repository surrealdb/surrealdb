use reblessive::Stk;

use crate::{
	sql::{statements::CreateStatement, Values},
	syn::v2::{
		parser::{ParseResult, Parser},
		token::t,
	},
};

impl Parser<'_> {
	pub async fn parse_create_stmt(&mut self, mut ctx: Stk) -> ParseResult<CreateStatement> {
		let only = self.eat(t!("ONLY"));
		let what = Values(self.parse_what_list(&mut ctx).await?);
		let data = self.try_parse_data(&mut ctx).await?;
		let output = self.try_parse_output(&mut ctx).await?;
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
