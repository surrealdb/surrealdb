use crate::{
	sql::{statements::RelateStatement, Subquery, Value},
	syn::v2::{
		parser::{
			mac::{expected, unexpected},
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
			expected!(self, t!("->"))
		} else {
			expected!(self, t!("<-"))
		};
		let second = self.parse_relate_value()?;
		if is_o {
			Ok((kind, first, second))
		} else {
			Ok((kind, second, first))
		}
	}

	pub fn parse_relate_value(&mut self) -> ParseResult<Value> {
		match self.peek_kind() {
			t!("[") => {
				let start = self.pop_peek().span;
				self.parse_array(start).map(Value::Array)
			}
			t!("$param") => self.next_token_value().map(Value::Param),
			t!("RETURN")
			| t!("SELECT")
			| t!("CREATE")
			| t!("UPDATE")
			| t!("DELETE")
			| t!("RELATE")
			| t!("DEFINE")
			| t!("REMOVE") => self.parse_inner_subquery(None).map(|x| Value::Subquery(Box::new(x))),
			t!("IF") => {
				self.pop_peek();
				self.parse_if_stmt().map(|x| Value::Subquery(Box::new(Subquery::Ifelse(x))))
			}
			t!("(") => {
				let span = self.pop_peek().span;
				let res =
					self.parse_inner_subquery(Some(span)).map(|x| Value::Subquery(Box::new(x)))?;
				Ok(res)
			}
			_ => self.parse_thing().map(Value::Thing),
		}
	}

	pub fn parse_thing_or_table(&mut self) -> ParseResult<Value> {
		if self.peek_token_at(1).kind == t!(":") {
			self.parse_thing().map(Value::Thing)
		} else {
			self.next_token_value().map(Value::Table)
		}
	}
}
