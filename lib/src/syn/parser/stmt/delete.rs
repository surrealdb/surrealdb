use crate::{
	sql::{statements::DeleteStatement, Values},
	syn::{
		parser::{ParseResult, Parser},
		token::t,
	},
};

impl Parser<'_> {
	pub fn parse_delete_stmt(&mut self) -> ParseResult<DeleteStatement> {
		let keyword = self.next();
		debug_assert_eq!(keyword.kind, t!("DELETE"));
		self.eat(t!("FROM"));
		let only = self.eat(t!("ONLY"));
		let what = Values(self.parse_what_list()?);
		let cond = self.try_parse_condition()?;
		let output = self.try_parse_output()?;
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
