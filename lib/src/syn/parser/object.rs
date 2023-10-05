use std::collections::BTreeMap;

use crate::{
	sql::{Object, Value},
	syn::{
		parser::mac::{expected, to_do},
		token::{t, TokenKind},
	},
};

use super::{mac::unexpected, ParseResult, Parser};

impl Parser<'_> {
	/// Parse an production which starts with an `{`
	///
	/// Either a block statemnt, a object or geometry.
	pub(super) fn parse_object_like(&mut self) -> ParseResult<Value> {
		let object_start = expected!(self, "{").span;

		// Check first if it can be an object.
		if self.peek_token_at(1).kind == t!(":") {
			// Could actually be an object, try that first
			// TODO: Do something with the error produced from trying to parse the object
			if let Ok(object) = self.parse_object() {
				return Ok(Value::Object(object));
			}
			self.backup_after(object_start);
		}

		// not an object so instead parse as a block.
		to_do!(self)
	}

	fn parse_object(&mut self) -> ParseResult<Object> {
		let mut map = BTreeMap::new();
		loop {
			if self.eat(t!("}")) {
				return Ok(Object(map));
			}

			let (key, value) = self.parse_object_entry()?;
			// TODO: Error on duplicate key?
			map.insert(key, value);

			if !self.eat(t!(",")) {
				expected!(self, "}");
				return Ok(Object(map));
			}
		}
	}

	fn parse_object_entry(&mut self) -> ParseResult<(String, Value)> {
		let text = self.parse_object_key()?;
		expected!(self, ":");
		let value = self.parse_value()?;
		Ok((text, value))
	}

	fn parse_object_key(&mut self) -> ParseResult<String> {
		let token = self.peek_token();
		match token.kind {
			TokenKind::Keyword(_)
			| TokenKind::Number
			| TokenKind::Duration {
				valid_identifier: true,
			} => {
				let str = self.lexer.reader.span(token.span);
				// Lexer should ensure that the token is valid utf-8
				let str = std::str::from_utf8(str).unwrap().to_owned();
				Ok(str)
			}
			TokenKind::Identifier | TokenKind::Strand => {
				let data_index = token.data_index.unwrap();
				let idx = u32::from(data_index) as usize;
				let str = self.lexer.strings[idx].clone();
				Ok(str)
			}
			x => unexpected!(self, x, "an object key"),
		}
	}
}
