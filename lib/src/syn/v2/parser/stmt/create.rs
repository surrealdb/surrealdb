use crate::{
	sql::{statements::CreateStatement, Values},
	syn::v2::{
		parser::{ParseResult, Parser},
		token::t,
	},
};

impl Parser<'_> {
	pub fn parse_create_stmt(&mut self) -> ParseResult<CreateStatement> {
		let only = self.eat(t!("ONLY"));
		let what = Values(self.parse_what_list()?);
		let data = self.try_parse_data()?;
		let output = self.try_parse_output()?;
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
