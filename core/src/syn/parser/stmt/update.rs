use reblessive::Stk;

use crate::syn::parser::mac::expected;
use crate::{
	sql::{statements::UpdateStatement, Values},
	syn::{
		parser::{ParseResult, Parser},
		token::t,
	},
};

impl Parser<'_> {
	pub async fn parse_update_stmt(&mut self, stk: &mut Stk) -> ParseResult<UpdateStatement> {
		let if_exists = if self.eat(t!("IF")) {
			expected!(self, t!("EXISTS"));
			true
		} else {
			false
		};
		let only = self.eat(t!("ONLY"));
		let what = Values(self.parse_what_list(stk).await?);
		let data = self.try_parse_data(stk).await?;
		let cond = self.try_parse_condition(stk).await?;
		let output = self.try_parse_output(stk).await?;
		let timeout = self.try_parse_timeout()?;
		let parallel = self.eat(t!("PARALLEL"));

		Ok(UpdateStatement {
			if_exists,
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
