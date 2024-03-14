use reblessive::Stk;

use crate::{
	sql::{statements::UpdateStatement, Values},
	syn::v2::{
		parser::{ParseResult, Parser},
		token::t,
	},
};

impl Parser<'_> {
	pub async fn parse_update_stmt(&mut self, mut ctx: Stk) -> ParseResult<UpdateStatement> {
		let only = self.eat(t!("ONLY"));
		let what = Values(self.parse_what_list(&mut ctx).await?);
		let data = self.try_parse_data(&mut ctx).await?;
		let cond = self.try_parse_condition(&mut ctx).await?;
		let output = self.try_parse_output(&mut ctx).await?;
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
