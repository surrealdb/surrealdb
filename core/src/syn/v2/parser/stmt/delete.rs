use reblessive::Ctx;

use crate::{
	sql::{statements::DeleteStatement, Values},
	syn::v2::{
		parser::{ParseResult, Parser},
		token::t,
	},
};

impl Parser<'_> {
	pub async fn parse_delete_stmt(&mut self, ctx: Ctx<'_>) -> ParseResult<DeleteStatement> {
		self.eat(t!("FROM"));
		let only = self.eat(t!("ONLY"));
		let what = Values(ctx.run(|ctx| self.parse_what_list(&mut ctx))?);
		let cond = ctx.run(|ctx| self.try_parse_condition(ctx)).await?;
		let output = ctx.run(|ctx| self.try_parse_output(ctx)).await?;
		let timeout = self.try_parse_timeout()?;
		let parallel = self.eat(t!("PARALLEL"));

		Ok(DeleteStatement {
			only,
			what,
			cond,
			output,
			timeout,
			parallel,
		})
	}
}

#[cfg(test)]
mod test {}
