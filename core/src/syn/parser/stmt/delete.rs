use reblessive::Stk;

use crate::syn::parser::mac::expected;
use crate::{
	sql::{statements::DeleteStatement, Values},
	syn::{
		parser::{ParseResult, Parser},
		token::t,
	},
};

impl Parser<'_> {
	pub async fn parse_delete_stmt(&mut self, ctx: &mut Stk) -> ParseResult<DeleteStatement> {
		self.eat(t!("FROM"));
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let only = self.eat(t!("ONLY"));
		let what = Values(self.parse_what_list(ctx).await?);
		let cond = self.try_parse_condition(ctx).await?;
		let output = self.try_parse_output(ctx).await?;
		let timeout = self.try_parse_timeout()?;
		let parallel = self.eat(t!("PARALLEL"));

		Ok(DeleteStatement {
			if_exists,
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
