use reblessive::Stk;

use crate::{
	sql::{statements::CreateStatement, Values, Kind},
	syn::{
		parser::{ParseResult, Parser},
		token::t,
		token::Span,
	},
};

impl Parser<'_> {
	pub(crate) async fn parse_create_stmt(
		&mut self,
		ctx: &mut Stk,
		delim: Span,
	) -> ParseResult<CreateStatement> {
		let only = self.eat(t!("ONLY"));
		let what = Values(self.parse_what_list(ctx).await?);
		let data = self.try_parse_data(ctx).await?;
		let output = self.try_parse_output(ctx).await?;
		let timeout = self.try_parse_timeout()?;
		let parallel = self.eat(t!("PARALLEL"));
		let version = self.try_parse_version(ctx).await?;
		let kind = if self.peek_kind() == t!("<") {
			Some(self.parse_kind(ctx, delim).await?)
		} else {
			Some(Kind::Any)
		};

		Ok(CreateStatement {
			only,
			what,
			data,
			output,
			timeout,
			parallel,
			version,
			kind,
		})
	}
}
