use crate::{
	sql::{statements::RelateStatement, Value},
	syn::{
		parser::{
			mac::{expected, to_do, unexpected},
			ParseResult, Parser,
		},
		token::t,
	},
};

impl Parser<'_> {
	pub fn parse_relate_stmt(&mut self) -> ParseResult<RelateStatement> {
		let only = self.eat(t!("ONLY"));
		let (kind, from, with) = self.parse_relation()?;
		let uniq = self.eat(t!("UNIQUE"));

		let data = self.try_parse_data()?;
		let output = self.try_parse_output()?;
		let timeout = self.try_parse_timeout()?;
		let parallel = self.eat(t!("PARALLEL"));
		Ok(RelateStatement {
			only,
			kind,
			from,
			with,
			uniq,
			data,
			output,
			timeout,
			parallel,
		})
	}

	pub fn parse_relation(&mut self) -> ParseResult<(Value, Value, Value)> {
		let first = self.parse_relate_value()?;
		let is_o = match self.next().kind {
			t!("->") => true,
			t!("<-") => false,
			x => unexpected!(self, x, "a relation arrow"),
		};
		let kind = self.parse_thing_or_table()?;
		if is_o {
			expected!(self, "->")
		} else {
			expected!(self, "<-")
		};
		let second = self.parse_relate_value()?;
		if is_o {
			Ok((kind, first, second))
		} else {
			Ok((kind, second, first))
		}
	}

	pub fn parse_relate_value(&mut self) -> ParseResult<Value> {
		to_do!(self)
	}

	pub fn parse_thing_or_table(&mut self) -> ParseResult<Value> {
		to_do!(self)
	}
}
