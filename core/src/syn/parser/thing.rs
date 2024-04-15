use reblessive::Stk;

use super::{ParseResult, Parser};
use crate::{
	sql::{id::Gen, Id, Ident, Range, Thing, Value},
	syn::{
		parser::{
			mac::{expected, unexpected},
			ParseError, ParseErrorKind,
		},
		token::{t, NumberKind, TokenKind},
	},
};
use std::{cmp::Ordering, ops::Bound};

impl Parser<'_> {
	pub async fn parse_record_string(&mut self, ctx: &mut Stk, double: bool) -> ParseResult<Thing> {
		let thing = self.parse_thing(ctx).await?;
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

	pub async fn parse_thing_or_range(
		&mut self,
		stk: &mut Stk,
		ident: String,
	) -> ParseResult<Value> {
		expected!(self, t!(":"));

		self.peek();
		self.no_whitespace()?;

		if self.eat(t!("..")) {
			let end = if self.eat(t!("=")) {
				self.no_whitespace()?;
				let id = stk.run(|stk| self.parse_id(stk)).await?;
				Bound::Included(id)
			} else if self.peek_can_be_ident()
				|| matches!(self.peek_kind(), TokenKind::Number(_) | t!("{") | t!("["))
			{
				self.no_whitespace()?;
				let id = stk.run(|stk| self.parse_id(stk)).await?;
				Bound::Excluded(id)
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
			let id = stk.run(|ctx| self.parse_id(ctx)).await?;

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
				let id = stk.run(|ctx| self.parse_id(ctx)).await?;
				Bound::Included(id)
			} else if self.peek_can_be_ident()
				|| matches!(self.peek_kind(), TokenKind::Number(_) | t!("{") | t!("["))
			{
				self.no_whitespace()?;
				let id = stk.run(|ctx| self.parse_id(ctx)).await?;
				Bound::Excluded(id)
			} else {
				Bound::Unbounded
			};
			Ok(Value::Range(Box::new(Range {
				tb: ident,
				beg,
				end,
			})))
		} else {
			let id = match beg {
				Bound::Unbounded => {
					if self.peek_kind() == t!("$param") {
						return Err(ParseError::new(
							ParseErrorKind::UnexpectedExplain {
								found: t!("$param"),
								expected: "a record-id id",
								explain: "you can create a record-id from a param with the function 'type::thing'",
							},
							self.recent_span(),
						));
					}

					// we haven't matched anythong so far so we still want any type of id.
					unexpected!(self, self.peek_kind(), "a record-id id")
				}
				Bound::Excluded(_) => {
					// we have matched a bounded id but we don't see an range operator.
					unexpected!(self, self.peek_kind(), "the range operator `..`")
				}
				Bound::Included(id) => id,
			};
			Ok(Value::Thing(Thing {
				tb: ident,
				id,
			}))
		}
	}

	pub async fn parse_range(&mut self, ctx: &mut Stk) -> ParseResult<Range> {
		let tb = self.next_token_value::<Ident>()?.0;

		expected!(self, t!(":"));

		self.peek();
		self.no_whitespace()?;

		let beg = if self.peek_can_be_ident() {
			self.peek();
			self.no_whitespace()?;

			let id = ctx.run(|ctx| self.parse_id(ctx)).await?;

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
			let id = ctx.run(|ctx| self.parse_id(ctx)).await?;
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

	pub async fn parse_thing(&mut self, ctx: &mut Stk) -> ParseResult<Thing> {
		let ident = self.next_token_value::<Ident>()?.0;
		self.parse_thing_from_ident(ctx, ident).await
	}

	pub async fn parse_thing_from_ident(
		&mut self,
		ctx: &mut Stk,
		ident: String,
	) -> ParseResult<Thing> {
		expected!(self, t!(":"));

		self.peek();
		self.no_whitespace()?;

		let id = ctx.run(|ctx| self.parse_id(ctx)).await?;
		Ok(Thing {
			tb: ident,
			id,
		})
	}

	pub async fn parse_id(&mut self, stk: &mut Stk) -> ParseResult<Id> {
		let token = self.next();
		match token.kind {
			t!("{") => {
				let object = self.parse_object(stk, token.span).await?;
				Ok(Id::Object(object))
			}
			t!("[") => {
				let array = self.parse_array(stk, token.span).await?;
				Ok(Id::Array(array))
			}
			t!("+") => {
				self.peek();
				self.no_whitespace()?;
				expected!(self, TokenKind::Number(NumberKind::Integer));
				let text = self.lexer.string.take().unwrap();
				if let Ok(number) = text.parse() {
					Ok(Id::Number(number))
				} else {
					Ok(Id::String(text))
				}
			}
			t!("-") => {
				self.peek();
				self.no_whitespace()?;
				expected!(self, TokenKind::Number(NumberKind::Integer));
				let text = self.lexer.string.take().unwrap();
				if let Ok(number) = text.parse::<u64>() {
					// Parse to u64 and check if the value is equal to `-i64::MIN` via u64 as
					// `-i64::MIN` doesn't fit in an i64
					match number.cmp(&((i64::MAX as u64) + 1)) {
						Ordering::Less => Ok(Id::Number(-(number as i64))),
						Ordering::Equal => Ok(Id::Number(i64::MIN)),
						Ordering::Greater => Ok(Id::String(format!("-{}", text))),
					}
				} else {
					Ok(Id::String(text))
				}
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
	use reblessive::Stack;

	use super::*;
	use crate::sql::array::Array;
	use crate::sql::object::Object;
	use crate::syn::Parse as _;

	fn thing(i: &str) -> ParseResult<Thing> {
		let mut parser = Parser::new(i.as_bytes());
		let mut stack = Stack::new();
		stack.enter(|ctx| async move { parser.parse_thing(ctx).await }).finish()
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
	fn thing_integer_min() {
		let sql = format!("test:{}", i64::MIN);
		let res = thing(&sql);
		let out = res.unwrap();
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::from(i64::MIN),
			}
		);
	}

	#[test]
	fn thing_integer_max() {
		let sql = format!("test:{}", i64::MAX);
		let res = thing(&sql);
		let out = res.unwrap();
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::from(i64::MAX),
			}
		);
	}

	#[test]
	fn thing_integer_more_then_max() {
		let max_str = format!("{}", (i64::MAX as u64) + 1);
		let sql = format!("test:{}", max_str);
		let res = thing(&sql);
		let out = res.unwrap();
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::from(max_str),
			}
		);
	}

	#[test]
	fn thing_integer_more_then_min() {
		let min_str = format!("-{}", (i64::MAX as u64) + 2);
		let sql = format!("test:{}", min_str);
		let res = thing(&sql);
		let out = res.unwrap();
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::from(min_str),
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
