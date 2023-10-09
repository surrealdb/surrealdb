use crate::{
	sql::{Field, Fields},
	syn::{
		parser::{ParseResult, Parser},
		token::t,
	},
};

impl Parser<'_> {
	pub(super) fn parse_fields(&mut self) -> ParseResult<Fields> {
		let backup = self.peek_token().span;
		if self.eat(t!("VALUE")) {
			match self.parse_alone() {
				Ok(x) => {
					// TODO: check for ',' and return error?
					return Ok(Fields(vec![x], true));
				}
				Err(_) => {
					// TODO: store and handle error
					self.backup_before(backup);
				}
			}
		}

		let mut fields = vec![self.parse_alone()?];
		while self.eat(t!(",")) {
			fields.push(self.parse_alone()?)
		}
		return Ok(Fields(fields, false));
	}

	fn parse_alone(&mut self) -> ParseResult<Field> {
		let value = self.parse_value()?;
		let alias = self.eat(t!("AS")).then(|| self.parse_plain_idiom()).transpose()?;
		Ok(Field::Single {
			expr: value,
			alias,
		})
	}
}
