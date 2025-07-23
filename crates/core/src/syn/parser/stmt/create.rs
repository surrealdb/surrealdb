use reblessive::Stk;

use crate::sql::CreateStatement;
use crate::syn::parser::{ParseResult, Parser};
use crate::syn::token::t;

impl Parser<'_> {
	pub(crate) async fn parse_create_stmt(
		&mut self,
		ctx: &mut Stk,
	) -> ParseResult<CreateStatement> {
		let only = self.eat(t!("ONLY"));
		let what = self.parse_what_list(ctx).await?;
		let data = self.try_parse_data(ctx).await?;
		let output = self.try_parse_output(ctx).await?;
		let version = if self.eat(t!("VERSION")) {
			Some(self.parse_expr_field(ctx).await?)
		} else {
			None
		};
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
