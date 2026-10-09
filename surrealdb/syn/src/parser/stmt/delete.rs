use surrealdb_sql::statements::DeleteStatement;

use crate::parser::{ParseResult, Parser};
use crate::token::t;

impl Parser<'_> {
	pub fn parse_delete_stmt(&mut self) -> ParseResult<DeleteStatement> {
		self.eat(t!("FROM"));
		let only = self.eat(t!("ONLY"));
		let what = self.parse_what_list()?;
		let with = self.try_parse_with()?;
		let cond = self.try_parse_condition()?;
		let output = self.try_parse_output()?;
		let timeout = self.try_parse_timeout()?;
		let explain = self.try_parse_explain()?;

		Ok(DeleteStatement {
			only,
			what,
			with,
			cond,
			output,
			timeout,
			explain,
		})
	}
}
