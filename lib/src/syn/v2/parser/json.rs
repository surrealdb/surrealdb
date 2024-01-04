use std::collections::BTreeMap;

use crate::{
	sql::{Array, Ident, Object, Strand, Value},
	syn::v2::{
		parser::mac::expected,
		token::{t, Span, TokenKind},
	},
};

use super::{ParseResult, Parser};

impl Parser<'_> {
	pub fn parse_json(&mut self) -> ParseResult<Value> {
		let token = self.next();
		match token.kind {
			t!("NULL") => Ok(Value::Null),
			t!("true") => Ok(Value::Bool(true)),
			t!("false") => Ok(Value::Bool(false)),
			t!("{") => self.parse_json_object(token.span).map(Value::Object),
			t!("[") => self.parse_json_array(token.span).map(Value::Array),
			TokenKind::Duration => self.token_value(token).map(Value::Duration),
			TokenKind::DateTime => self.token_value(token).map(Value::Datetime),
			TokenKind::Strand => {
				if self.legacy_strands {
					self.parse_legacy_strand()
				} else {
					Ok(Value::Strand(Strand(self.lexer.string.take().unwrap())))
				}
			}
			TokenKind::Number(_) => self.token_value(token).map(Value::Number),
			TokenKind::Uuid => self.token_value(token).map(Value::Uuid),
			_ => {
				let ident = self.token_value::<Ident>(token)?.0;
				self.parse_thing_from_ident(ident).map(Value::Thing)
			}
		}
	}

	fn parse_json_object(&mut self, start: Span) -> ParseResult<Object> {
		let mut obj = BTreeMap::new();
		loop {
			if self.eat(t!("}")) {
				return Ok(Object(obj));
			}
			let key = self.parse_object_key()?;
			expected!(self, t!(":"));
			let value = self.parse_json()?;
			obj.insert(key, value);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!("}"), start)?;
				return Ok(Object(obj));
			}
		}
	}

	fn parse_json_array(&mut self, start: Span) -> ParseResult<Array> {
		let mut array = Vec::new();
		loop {
			if self.eat(t!("]")) {
				return Ok(Array(array));
			}
			let value = self.parse_json()?;
			array.push(value);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!("]"), start)?;
				return Ok(Array(array));
			}
		}
	}
}
