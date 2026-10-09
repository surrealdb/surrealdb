use surrealdb_sql::statements::UpsertStatement;

use crate::parser::{ParseResult, Parser};
use crate::token::t;

impl Parser<'_> {
	pub fn parse_upsert_stmt(&mut self) -> ParseResult<UpsertStatement> {
		let only = self.eat(t!("ONLY"));
		let what = self.parse_what_list()?;
		let with = self.try_parse_with()?;
		let data = self.try_parse_data()?;
		let cond = self.try_parse_condition()?;
		let output = self.try_parse_output()?;
		let timeout = self.try_parse_timeout()?;
		let explain = self.try_parse_explain()?;

		Ok(UpsertStatement {
			only,
			what,
			with,
			data,
			cond,
			output,
			timeout,
			explain,
		})
	}
}
