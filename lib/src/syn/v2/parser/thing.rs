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
		expected!(self, ":");

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
		let tb = self.parse_token_value::<Ident>()?.0;

		expected!(self, ":");

		self.peek();
		self.no_whitespace()?;

		let exclusive = self.eat(t!(">"));

		self.peek();
		self.no_whitespace()?;

		let id = self.parse_id()?;

		self.peek();
		self.no_whitespace()?;

		expected!(self, "..");

		self.peek();
		self.no_whitespace()?;

		let inclusive = self.eat(t!("="));

		self.peek();
		self.no_whitespace()?;

		let end = self.parse_id()?;
		Ok(Range {
			tb,
			beg: if exclusive {
				Bound::Excluded(id)
			} else {
				Bound::Included(id)
			},
			end: if inclusive {
				Bound::Included(end)
			} else {
				Bound::Excluded(end)
			},
		})
	}

	pub fn parse_thing(&mut self) -> ParseResult<Thing> {
		let ident = self.parse_token_value::<Ident>()?.0;
		self.parse_thing_from_ident(ident)
	}

	pub fn parse_thing_from_ident(&mut self, ident: String) -> ParseResult<Thing> {
		expected!(self, ":");

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
				expected!(self, "(");
				expected!(self, ")");
				Ok(Id::Generate(Gen::Ulid))
			}
			t!("UUID") => {
				expected!(self, "(");
				expected!(self, ")");
				Ok(Id::Generate(Gen::Uuid))
			}
			t!("RAND") => {
				expected!(self, "(");
				expected!(self, ")");
				Ok(Id::Generate(Gen::Rand))
			}
			_ => {
				let ident = self.token_value::<Ident>(token)?.0;
				Ok(Id::String(ident))
			}
		}
	}
}
