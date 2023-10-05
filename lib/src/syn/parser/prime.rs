use crate::{
	sql::{Array, Block, Future, Mock, Value},
	syn::{
		parser::mac::{expected, to_do},
		token::{t, TokenKind},
	},
};

use super::{ParseResult, Parser};

impl Parser<'_> {
	/// Parses a value that operators operate on.
	fn parse_prime_value(&mut self) -> ParseResult<Value> {
		let token = self.peek_token();
		let value = match token.kind {
			t!("<") => {
				self.pop_peek();
				// At this point casting should already have been parsed.
				// So this must be a future
				expected!(self, "FUTURE");
				self.expect_closing_delimiter(t!(">"), token.span)?;
				expected!(self, "{");
				let block = self.parse_block()?;
				let future = Box::new(Future(block));
				// future can't start an idiom so return immediately.
				return Ok(Value::Future(future));
			}
			t!("|") => {
				self.pop_peek();
				let t = self.parse_raw_ident()?;
				expected!(self, ":");
				let number = self.parse_u64()?;
				// mock can't start an idiom so return immediately.
				if self.eat(t!("|")) {
					return Ok(Value::Mock(Mock::Count(t, number)));
				} else {
					expected!(self, "..");
					let to = self.parse_u64()?;
					expected!(self, "|");
					return Ok(Value::Mock(Mock::Range(t, number, to)));
				}
			}
			t!("123") => {
				let number = self.parse_number()?;
				Value::Number(number)
			}
			t!("[") => Value::Array(self.parse_array()?),
			t!("{") => self.parse_object_like()?,
			t!("/") => {
				// regex
				to_do!(self)
			}
			t!("$param") => {
				let param = self.parse_param()?;
				Value::Param(param)
			}
			TokenKind::Strand => {
				let strand = self.parse_strand()?;
				Value::Strand(strand)
			}
			TokenKind::Duration {
				..
			} => {
				let duration = self.parse_duration()?;
				Value::Duration(duration)
			}
			_ => todo!(),
		};

		Ok(value)
	}

	fn parse_array(&mut self) -> ParseResult<Array> {
		let start = expected!(self, "[").span;
		let mut res = Vec::new();
		// basic parsing of a delimited list with optional trailing separator.
		// First check if the list ends, then parse a value followed by asserting that the list
		// ends if there is no next separator.
		loop {
			if self.eat(t!("]")) {
				break;
			}
			let value = self.parse_value()?;
			res.push(value);
			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!("]"), start)?;
				break;
			}
		}
		Ok(Array(res))
	}

	fn parse_block(&mut self) -> ParseResult<Block> {
		expected!(self, "{");
		to_do!(self)
	}
}
