use std::collections::BTreeMap;

use crate::{
	sql::{Block, Object, Value},
	syn::{
		parser::{mac::expected, ParseError, ParseErrorKind, ParseResult, Parser},
		token::{t, Span, TokenKind},
	},
};

use super::mac::unexpected;

impl Parser<'_> {
	/// Parse an production which starts with an `{`
	///
	/// Either a block statemnt, a object or geometry.
	pub(super) fn parse_object_like(&mut self, start: Span) -> ParseResult<Value> {
		if self.eat(t!("}")) {
			// empty object, just return
			return Ok(Value::Object(Object::default()));
		}

		// Check first if it can be an object.
		if self.peek_token_at(1).kind == t!(":") {
			return self.parse_object(start).map(Value::Object);
		}

		// not an object so instead parse as a block.
		self.parse_block(start).map(Box::new).map(Value::Block)
	}

	/// Parses an object.
	///
	/// Expects the span of the starting `{` as an argument.
	///
	/// # Parser state
	/// Expects the first `{` to already have been eaten.
	pub(super) fn parse_object(&mut self, start: Span) -> ParseResult<Object> {
		let mut map = BTreeMap::new();
		loop {
			if self.eat(t!("}")) {
				return Ok(Object(map));
			}

			let (key, value) = self.parse_object_entry()?;
			// TODO: Error on duplicate key?
			map.insert(key, value);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!("}"), start)?;
				return Ok(Object(map));
			}
		}
	}

	/// Parses a block of statements
	///
	/// # Parser State
	/// Expects the starting `{` to have already been eaten and its span to be handed to this
	/// functions as the `start` parameter.
	pub(super) fn parse_block(&mut self, start: Span) -> ParseResult<Block> {
		let mut statements = Vec::new();
		loop {
			while self.eat(t!(";")) {}
			if self.eat(t!("}")) {
				break;
			}

			let statement_span = self.peek().span;
			let stmt = self.parse_statement()?;
			if let Some(x) = stmt.into_entry() {
				statements.push(x);
			} else {
				return Err(ParseError::new(ParseErrorKind::DisallowedStatement, statement_span));
			}
			if !self.eat(t!(";")) {
				self.expect_closing_delimiter(t!("}"), start)?;
				break;
			}
		}
		Ok(Block(statements))
	}

	/// Parse a single entry in the object, i.e. `field: value + 1` in the object `{ field: value +
	/// 1 }`
	fn parse_object_entry(&mut self) -> ParseResult<(String, Value)> {
		let text = self.parse_object_key()?;
		expected!(self, ":");
		let value = self.parse_value()?;
		Ok((text, value))
	}

	/// Parses the key of an object, i.e. `field` in the object `{ field: 1 }`.
	fn parse_object_key(&mut self) -> ParseResult<String> {
		let token = self.peek();
		match token.kind {
			TokenKind::Keyword(_) => {
				self.pop_peek();
				let str = self.lexer.reader.span(token.span);
				// Lexer should ensure that the token is valid utf-8
				let str = std::str::from_utf8(str).unwrap().to_owned();
				Ok(str)
			}
			TokenKind::Identifier | TokenKind::Strand => {
				self.pop_peek();
				let data_index = token.data_index.unwrap();
				let idx = u32::from(data_index) as usize;
				let str = self.lexer.strings[idx].clone();
				Ok(str)
			}
			x => unexpected!(self, x, "an object key"),
		}
	}
}
