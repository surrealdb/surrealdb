use super::{ParseResult, Parser};
use crate::{
	sql::{id::Gen, Id, Ident, Range, Thing, Value},
	syn::v2::{
		parser::{
			mac::{expected, unexpected},
			ParseError, ParseErrorKind,
		},
		token::{t, TokenKind},
	},
};
use std::ops::Bound;

impl Parser<'_> {
	pub fn parse_record_string(&mut self) -> ParseResult<Thing> {
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
		debug_assert_eq!(token.kind, TokenKind::CloseRecordString);
		Ok(thing)
	}

	pub fn parse_thing_or_range(&mut self, ident: String) -> ParseResult<Value> {
		expected!(self, ":");
		let exclusive = self.eat(t!(">"));
		let id = self.parse_id()?;
		if self.eat(t!("..")) {
			let inclusive = self.eat(t!("="));
			let end = self.parse_id()?;
			Ok(Value::Range(Box::new(Range {
				tb: ident,
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
			})))
		} else {
			if exclusive {
				unexpected!(self, self.peek_kind(), "the range operator '..'")
			}
			Ok(Value::Thing(Thing {
				tb: ident,
				id,
			}))
		}
	}

	pub fn parse_range(&mut self) -> ParseResult<Range> {
		let tb = self.parse_token_value::<Ident>()?.0;
		expected!(self, ":");
		let exclusive = self.eat(t!(">"));
		let id = self.parse_id()?;
		expected!(self, "..");
		let inclusive = self.eat(t!("="));
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
		let id = self.parse_id()?;
		Ok(Thing {
			tb: ident,
			id,
		})
	}

	pub fn parse_id(&mut self) -> ParseResult<Id> {
		match self.peek_kind() {
			t!("{") => {
				let start = self.pop_peek().span;
				let object = self.parse_object(start)?;
				Ok(Id::Object(object))
			}
			t!("[") => {
				let start = self.pop_peek().span;
				let array = self.parse_array(start)?;
				Ok(Id::Array(array))
			}
			// TODO: negative numbers.
			TokenKind::Number => {
				let number = self.parse_token_value::<u64>()?;
				Ok(Id::Number(number as i64))
			}
			t!("ULID") => {
				self.pop_peek();
				// TODO: error message about how to use `ulid` as an identifier.
				expected!(self, "(");
				expected!(self, ")");
				Ok(Id::Generate(Gen::Ulid))
			}
			t!("UUID") => {
				self.pop_peek();
				expected!(self, "(");
				expected!(self, ")");
				Ok(Id::Generate(Gen::Uuid))
			}
			t!("RAND") => {
				self.pop_peek();
				expected!(self, "(");
				expected!(self, ")");
				Ok(Id::Generate(Gen::Rand))
			}
			_ => {
				let ident = self.parse_token_value::<Ident>()?.0;
				Ok(Id::String(ident))
			}
		}
	}
}
