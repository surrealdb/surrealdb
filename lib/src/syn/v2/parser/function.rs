use crate::{
	sql::{Function, Ident, Model},
	syn::v2::{parser::mac::expected, token::t},
};

use super::{ParseResult, Parser};

impl Parser<'_> {
	pub fn parse_custom_function(&mut self) -> ParseResult<Function> {
		expected!(self, "::");
		let mut name = self.parse_token_value::<Ident>()?.0;
		while self.eat(t!("::")) {
			name.push_str("::");
			name.push_str(&self.parse_token_value::<Ident>()?.0)
		}
		let start = expected!(self, "(").span;
		let mut args = Vec::new();
		loop {
			if self.eat(t!(")")) {
				break;
			}

			args.push(self.parse_value_field()?);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!(")"), start)?;
				break;
			}
		}

		Ok(Function::Custom(name, args))
	}

	pub fn parse_model(&mut self) -> ParseResult<Model> {
		expected!(self, "::");
		let mut name = self.parse_token_value::<Ident>()?.0;
		while self.eat(t!("::")) {
			name.push_str("::");
			name.push_str(&self.parse_token_value::<Ident>()?.0)
		}
		let start = expected!(self, "<").span;
		let major = self.parse_token_value::<u64>()?;
		expected!(self, ".");
		let minor = self.parse_token_value::<u64>()?;
		expected!(self, ".");
		let patch = self.parse_token_value::<u64>()?;
		self.expect_closing_delimiter(t!(">"), start)?;

		let start = expected!(self, "(").span;
		let mut args = Vec::new();
		loop {
			if self.eat(t!(")")) {
				break;
			}

			args.push(self.parse_value_field()?);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!(")"), start)?;
				break;
			}
		}
		Ok(Model {
			name,
			version: format!("{}.{}.{}", major, minor, patch),
			args,
		})
	}
}
