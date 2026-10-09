use surrealdb_sql::CreateStatement;

use crate::parser::{ParseResult, Parser};
use crate::token::t;

impl Parser<'_> {
	pub fn parse_create_stmt(&mut self) -> ParseResult<CreateStatement> {
		let only = self.eat(t!("ONLY"));
		let what = self.parse_what_list()?;
		let data = self.try_parse_data()?;
		let output = self.try_parse_output()?;
		// VERSION is no longer supported in CREATE statements, it is left here for backwards
		// compatibility.
		if self.eat(t!("VERSION")) {
			self.parse_expr_field()?;
		};
		let timeout = self.try_parse_timeout()?;

		Ok(CreateStatement {
			only,
			what,
			data,
			output,
			timeout,
		})
	}
}
