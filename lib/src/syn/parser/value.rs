use crate::sql::{Strand, Value};
use crate::syn::parser::{mac::to_do, ParseResult, Parser};
use crate::syn::token::{t, TokenKind};

impl Parser<'_> {
	pub fn parse_value(&mut self) -> ParseResult<Value> {
		self.parse_expression()
	}

	pub fn parse_idiom_expression(&mut self) -> ParseResult<Value> {
		let next = self.next();
		let _value = match next.kind {
			t!("NONE") => return Ok(Value::None),
			t!("NULL") => return Ok(Value::Null),
			t!("true") => return Ok(Value::Bool(true)),
			t!("false") => return Ok(Value::Bool(false)),
			t!("[") => self.parse_array(next.span),
			TokenKind::Strand => {
				let text = self.lexer.strings[u32::from(next.data_index.unwrap()) as usize].clone();
				return Ok(Value::Strand(Strand(text)));
			}
			_ => to_do!(self),
		};
		to_do!(self)
	}
}
