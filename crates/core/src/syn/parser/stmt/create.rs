use reblessive::Stk;

use crate::{
	sql::{SqlValues, statements::CreateStatement},
	syn::{
		parser::{ParseResult, Parser},
		token::t,
	},
};

impl Parser<'_> {
	pub(crate) async fn parse_create_stmt(
		&mut self,
		ctx: &mut Stk,
	) -> ParseResult<CreateStatement> {
		let only = self.eat(t!("ONLY"));
		let what = SqlValues(self.parse_what_list(ctx).await?);
		let data = self.try_parse_data(ctx).await?;
		let output = self.try_parse_output(ctx).await?;
		let version = self.try_parse_version(ctx).await?;
		let timeout = self.try_parse_timeout()?;
		let expire = self.try_parse_expire()?;
		let parallel = self.eat(t!("PARALLEL"));

		Ok(CreateStatement {
			only,
			what,
			data,
			output,
			timeout,
			expire,
			parallel,
			version,
		})
	}
}
