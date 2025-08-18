use reblessive::Stk;

use crate::sql::statements::DeleteStatement;
use crate::syn::parser::{ParseResult, Parser};
use crate::syn::token::t;

impl Parser<'_> {
	pub(crate) async fn parse_delete_stmt(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<DeleteStatement> {
		self.eat(t!("FROM"));
		let only = self.eat(t!("ONLY"));
		let what = self.parse_what_list(stk).await?;
		let with = self.try_parse_with()?;
		let cond = self.try_parse_condition(stk).await?;
		let output = self.try_parse_output(stk).await?;
		let timeout = self.try_parse_timeout()?;
		let parallel = self.eat(t!("PARALLEL"));
		let explain = self.try_parse_explain()?;

		Ok(DeleteStatement {
			only,
			what,
			with,
			cond,
			output,
			timeout,
			parallel,
			explain,
		})
	}
}

#[cfg(test)]
mod test {}
