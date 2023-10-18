use crate::sql::Value;
use crate::syn::parser::{mac::to_do, ParseResult, Parser};

impl Parser<'_> {
	pub fn parse_value(&mut self) -> ParseResult<Value> {
		to_do!(self)
	}
}
