use crate::sql::{
	parser2::{mac::to_do, ParseResult, Parser},
	token::Token,
	Value,
};

impl Parser<'_> {
	pub fn parse_fallback_value(&mut self, tokens: &[Token]) -> ParseResult<Value> {
		to_do!(self)
	}
}
