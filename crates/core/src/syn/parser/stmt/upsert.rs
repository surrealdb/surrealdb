use reblessive::Stk;

use crate::sql::statements::UpsertStatement;
use crate::syn::parser::{ParseResult, Parser};
use crate::syn::token::t;

impl Parser<'_> {
	pub async fn parse_upsert_stmt(&mut self, stk: &mut Stk) -> ParseResult<UpsertStatement> {
		let only = self.eat(t!("ONLY"));
		let what = self.parse_what_list(stk).await?;
		let with = self.try_parse_with()?;
		let data = self.try_parse_data(stk).await?;
		let cond = self.try_parse_condition(stk).await?;
		let output = self.try_parse_output(stk).await?;
		let timeout = self.try_parse_timeout()?;
		let parallel = self.eat(t!("PARALLEL"));
		let explain = self.try_parse_explain()?;

		Ok(UpsertStatement {
			only,
			what,
			with,
			data,
			cond,
			output,
			timeout,
			parallel,
			explain,
		})
	}
}
