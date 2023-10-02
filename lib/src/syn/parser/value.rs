use crate::sql::{Table, Value, Values};
use crate::syn::{
	parser::{mac::to_do, ParseResult, Parser},
	token::{t, Token, TokenKind},
};

impl Parser<'_> {
	/// Retry to parse a statement which failed to parse as a value.
	///
	/// All statements start with a keyword which cover an identifier which could be the start of a
	/// value production. If a statement fails to parse, we retry to parse here.
	pub fn parse_fallback_value(&mut self, _start: Token) -> ParseResult<Value> {
		to_do!(self)
	}

	pub fn parse_value(&mut self) -> ParseResult<Value> {
		to_do!(self)
	}

	pub fn parse_whats(&mut self) -> ParseResult<Values> {
		let mut whats = vec![self.parse_what()?];
		while self.eat(t!(",")) {
			whats.push(self.parse_what()?);
		}
		Ok(Values(whats))
	}

	pub fn parse_what(&mut self) -> ParseResult<Value> {
		let token = self.peek_token();
		match token.kind {
			t!("<->") => to_do!(self),    // graph
			t!("->") => to_do!(self),     // graph
			t!("<-") => to_do!(self),     // graph
			t!("|") => to_do!(self),      // mock
			t!("$param") => to_do!(self), // mock
			t!("{") => to_do!(self),      // block
			TokenKind::Identifier => {
				let ident = self.parse_raw_ident()?;
				if self.eat(t!(":")) {
					return self.parse_thing_tail(ident);
				}
				Ok(Value::Table(Table(ident)))
			}
			_ => to_do!(self),
		}
	}

	pub fn parse_thing_tail(&mut self, _start: String) -> ParseResult<Value> {
		to_do!(self)
	}
}
