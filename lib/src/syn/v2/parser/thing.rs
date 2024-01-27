use super::{ParseResult, Parser};
use crate::{
	sql::{id::Gen, Id, Ident, Range, Thing, Value},
	syn::v2::{
		parser::{
			mac::{expected, unexpected},
			ParseError, ParseErrorKind,
		},
		token::{t, NumberKind, TokenKind},
	},
};
use std::ops::Bound;

impl Parser<'_> {
	pub fn parse_record_string(&mut self, double: bool) -> ParseResult<Thing> {
		let thing = self.parse_thing()?;
		// can't have any tokens in the buffer, since the next token must be produced by a specific
		// call.
		debug_assert_eq!(self.token_buffer.len(), 0);
		// manually handle the trailing `"`.
		let token = self.lexer.lex_record_string_close();
		if token.kind == TokenKind::Invalid {
			return Err(ParseError::new(
				ParseErrorKind::InvalidToken(self.lexer.error.take().unwrap()),
				token.span,
			));
		}
		if token.kind == t!("'r") && double {
			unexpected!(self, token.kind, "a single quote")
		}
		if token.kind == t!("\"r") && !double {
			unexpected!(self, token.kind, "a double quote")
		}
		debug_assert!(matches!(token.kind, TokenKind::CloseRecordString { .. }));
		Ok(thing)
	}

	pub fn parse_thing_or_range(&mut self, ident: String) -> ParseResult<Value> {
		expected!(self, t!(":"));

		self.peek();
		self.no_whitespace()?;

		if self.eat(t!("..")) {
			let end = if self.eat(t!("=")) {
				self.no_whitespace()?;
				Bound::Included(self.parse_id()?)
			} else if self.peek_can_be_ident()
				|| matches!(self.peek_kind(), TokenKind::Number(_) | t!("{") | t!("["))
			{
				self.no_whitespace()?;
				Bound::Excluded(self.parse_id()?)
			} else {
				Bound::Unbounded
			};
			return Ok(Value::Range(Box::new(Range {
				tb: ident,
				beg: Bound::Unbounded,
				end,
			})));
		}

		let beg = if self.peek_can_be_ident()
			|| matches!(self.peek_kind(), TokenKind::Number(_) | t!("{") | t!("["))
		{
			let id = self.parse_id()?;

			if self.eat(t!(">")) {
				self.no_whitespace()?;
				Bound::Excluded(id)
			} else {
				Bound::Included(id)
			}
		} else {
			Bound::Unbounded
		};

		if self.eat(t!("..")) {
			let end = if self.eat(t!("=")) {
				self.no_whitespace()?;
				Bound::Included(self.parse_id()?)
			} else if self.peek_can_be_ident()
				|| matches!(self.peek_kind(), TokenKind::Number(_) | t!("{") | t!("["))
			{
				self.no_whitespace()?;
				Bound::Excluded(self.parse_id()?)
			} else {
				Bound::Unbounded
			};
			Ok(Value::Range(Box::new(Range {
				tb: ident,
				beg,
				end,
			})))
		} else {
			let Bound::Included(id) = beg else {
				unexpected!(self, self.peek_kind(), "the range operator '..'")
			};
			Ok(Value::Thing(Thing {
				tb: ident,
				id,
			}))
		}
	}

	pub fn parse_range(&mut self) -> ParseResult<Range> {
		let tb = self.next_token_value::<Ident>()?.0;

		expected!(self, t!(":"));

		self.peek();
		self.no_whitespace()?;

		let beg = if self.peek_can_be_ident() {
			self.peek();
			self.no_whitespace()?;

			let id = self.parse_id()?;

			self.peek();
			self.no_whitespace()?;

			if self.eat(t!(">")) {
				Bound::Excluded(id)
			} else {
				Bound::Included(id)
			}
		} else {
			Bound::Unbounded
		};

		self.peek();
		self.no_whitespace()?;

		expected!(self, t!(".."));

		self.peek();
		self.no_whitespace()?;

		let inclusive = self.eat(t!("="));

		self.peek();
		self.no_whitespace()?;

		let end = if self.peek_can_be_ident() {
			let id = self.parse_id()?;
			if inclusive {
				Bound::Included(id)
			} else {
				Bound::Excluded(id)
			}
		} else {
			Bound::Unbounded
		};

		Ok(Range {
			tb,
			beg,
			end,
		})
	}

	pub fn parse_thing(&mut self) -> ParseResult<Thing> {
		let ident = self.next_token_value::<Ident>()?.0;
		self.parse_thing_from_ident(ident)
	}

	pub fn parse_thing_from_ident(&mut self, ident: String) -> ParseResult<Thing> {
		expected!(self, t!(":"));

		self.peek();
		self.no_whitespace()?;

		let id = self.parse_id()?;
		Ok(Thing {
			tb: ident,
			id,
		})
	}

	pub fn parse_id(&mut self) -> ParseResult<Id> {
		let token = self.next();
		match token.kind {
			t!("{") => {
				let object = self.parse_object(token.span)?;
				Ok(Id::Object(object))
			}
			t!("[") => {
				let array = self.parse_array(token.span)?;
				Ok(Id::Array(array))
			}
			TokenKind::Number(NumberKind::Integer) => {
				// Id handle numbers more loose then other parts of the code.
				// If number can't fit in a i64 it will instead be parsed as a string.
				let text = self.lexer.string.take().unwrap();
				if let Ok(number) = text.parse() {
					Ok(Id::Number(number))
				} else {
					Ok(Id::String(text))
				}
			}
			t!("ULID") => {
				// TODO: error message about how to use `ulid` as an identifier.
				expected!(self, t!("("));
				expected!(self, t!(")"));
				Ok(Id::Generate(Gen::Ulid))
			}
			t!("UUID") => {
				expected!(self, t!("("));
				expected!(self, t!(")"));
				Ok(Id::Generate(Gen::Uuid))
			}
			t!("RAND") => {
				expected!(self, t!("("));
				expected!(self, t!(")"));
				Ok(Id::Generate(Gen::Rand))
			}
			_ => {
				let ident = self.token_value::<Ident>(token)?.0;
				Ok(Id::String(ident))
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::array::Array;
	use crate::sql::object::Object;
	use crate::sql::value::Value;
	use crate::syn::Parse;

	fn thing(i: &str) -> ParseResult<Thing> {
		let mut parser = Parser::new(i.as_bytes());
		parser.parse_thing()
	}

	#[test]
	fn thing_normal() {
		let sql = "test:id";
		let res = thing(sql);
		let out = res.unwrap();
		assert_eq!("test:id", format!("{}", out));
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::from("id"),
			}
		);
	}

	#[test]
	fn thing_integer() {
		let sql = "test:001";
		let res = thing(sql);
		let out = res.unwrap();
		assert_eq!("test:1", format!("{}", out));
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::from(1),
			}
		);
	}

	#[test]
	fn thing_string() {
		let sql = "r'test:001'";
		let res = Value::parse(sql);
		let Value::Thing(out) = res else {
			panic!()
		};
		assert_eq!("test:1", format!("{}", out));
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::from(1),
			}
		);

		let sql = "r'test:001'";
		let res = Value::parse(sql);
		let Value::Thing(out) = res else {
			panic!()
		};
		assert_eq!("test:1", format!("{}", out));
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::from(1),
			}
		);
	}

	#[test]
	fn thing_quoted_backtick() {
		let sql = "`test`:`id`";
		let res = thing(sql);
		let out = res.unwrap();
		assert_eq!("test:id", format!("{}", out));
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::from("id"),
			}
		);
	}

	#[test]
	fn thing_quoted_brackets() {
		let sql = "⟨test⟩:⟨id⟩";
		let res = thing(sql);
		let out = res.unwrap();
		assert_eq!("test:id", format!("{}", out));
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::from("id"),
			}
		);
	}

	#[test]
	fn thing_object() {
		let sql = "test:{ location: 'GBR', year: 2022 }";
		let res = thing(sql);
		let out = res.unwrap();
		assert_eq!("test:{ location: 'GBR', year: 2022 }", format!("{}", out));
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::Object(Object::from(map! {
					"location".to_string() => Value::from("GBR"),
					"year".to_string() => Value::from(2022),
				})),
			}
		);
	}

	#[test]
	fn thing_array() {
		let sql = "test:['GBR', 2022]";
		let res = thing(sql);
		let out = res.unwrap();
		assert_eq!("test:['GBR', 2022]", format!("{}", out));
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::Array(Array::from(vec![Value::from("GBR"), Value::from(2022)])),
			}
		);
	}
}
