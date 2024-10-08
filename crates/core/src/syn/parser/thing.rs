use reblessive::Stk;

use super::{ParseResult, Parser};
use crate::{
	sql::{
		id::{range::IdRange, Gen},
		Id, Ident, Param, Range, Thing,
	},
	syn::{
		error::bail,
		lexer::compound,
		parser::mac::{expected, expected_whitespace, unexpected},
		token::{t, Glued, TokenKind},
	},
};
use std::{cmp::Ordering, ops::Bound};

impl Parser<'_> {
	pub(crate) async fn parse_record_string(
		&mut self,
		ctx: &mut Stk,
		double: bool,
	) -> ParseResult<Thing> {
		let thing = self.parse_thing(ctx).await?;

		debug_assert!(self.last_span().is_followed_by(&self.peek_whitespace().span));

		if double {
			expected_whitespace!(self, t!("\""));
		} else {
			expected_whitespace!(self, t!("'"));
		};
		Ok(thing)
	}

	pub(crate) async fn parse_thing_or_range(
		&mut self,
		stk: &mut Stk,
		ident: String,
	) -> ParseResult<Thing> {
		expected_whitespace!(self, t!(":"));

		// If self starts with a range operator self is a range with no start bound
		if self.eat_whitespace(t!("..")) {
			// Check for inclusive
			let end = if self.eat_whitespace(t!("=")) {
				let id = stk.run(|stk| self.parse_id(stk)).await?;
				Bound::Included(id)
			} else if Self::kind_starts_record_id_key(self.peek_whitespace().kind) {
				let id = stk.run(|stk| self.parse_id(stk)).await?;
				Bound::Excluded(id)
			} else {
				Bound::Unbounded
			};
			return Ok(Thing {
				tb: ident,
				id: Id::Range(Box::new(IdRange {
					beg: Bound::Unbounded,
					end,
				})),
			});
		}

		// Didn't eat range yet so we need to parse the id.
		let beg = if Self::kind_starts_record_id_key(self.peek_whitespace().kind) {
			let v = stk.run(|stk| self.parse_id(stk)).await?;

			// check for exclusive
			if self.eat_whitespace(t!(">")) {
				Bound::Excluded(v)
			} else {
				Bound::Included(v)
			}
		} else {
			Bound::Unbounded
		};

		// Check if self is actually a range.
		// If we already ate the exclusive it must be a range.
		if self.eat_whitespace(t!("..")) {
			let end = if self.eat_whitespace(t!("=")) {
				let id = stk.run(|stk| self.parse_id(stk)).await?;
				Bound::Included(id)
			} else if Self::kind_starts_record_id_key(self.peek_whitespace().kind) {
				let id = stk.run(|stk| self.parse_id(stk)).await?;
				Bound::Excluded(id)
			} else {
				Bound::Unbounded
			};
			Ok(Thing {
				tb: ident,
				id: Id::Range(Box::new(IdRange {
					beg,
					end,
				})),
			})
		} else {
			let id = match beg {
				Bound::Unbounded => {
					let token = self.peek_whitespace();
					if token.kind == t!("$param") {
						let param = self.next_token_value::<Param>()?;
						bail!("Unexpected token `$param` expected a record-id key",
							@token.span => "Record-id's can be create from a param with `type::thing(\"{}\",{})`", ident,param);
					}

					// we haven't matched anything so far so we still want any type of id.
					unexpected!(self, token, "a record-id key")
				}
				Bound::Excluded(_) => {
					// we have matched a bounded id but we don't see an range operator.
					unexpected!(self, self.peek_whitespace(), "the range operator `..`")
				}
				// We previously converted the `Id` value to `Value` so it's safe to unwrap here.
				Bound::Included(v) => v,
			};
			Ok(Thing {
				tb: ident,
				id,
			})
		}
	}

	/// Parse an range
	pub(crate) async fn parse_range(&mut self, ctx: &mut Stk) -> ParseResult<Range> {
		// Check for beginning id
		let beg = if Self::kind_is_identifier(self.peek_whitespace().kind) {
			let v = ctx.run(|ctx| self.parse_value_inherit(ctx)).await?;

			if self.eat_whitespace(t!(">")) {
				Bound::Excluded(v)
			} else {
				Bound::Included(v)
			}
		} else {
			Bound::Unbounded
		};

		expected_whitespace!(self, t!(".."));

		let inclusive = self.eat_whitespace(t!("="));

		// parse ending id.
		let end = if Self::kind_is_identifier(self.peek_whitespace().kind) {
			let v = ctx.run(|ctx| self.parse_value_inherit(ctx)).await?;
			if inclusive {
				Bound::Included(v)
			} else {
				Bound::Excluded(v)
			}
		} else {
			Bound::Unbounded
		};

		Ok(Range {
			beg,
			end,
		})
	}

	pub(crate) async fn parse_thing_with_range(&mut self, ctx: &mut Stk) -> ParseResult<Thing> {
		let ident = self.next_token_value::<Ident>()?.0;
		self.parse_thing_or_range(ctx, ident).await
	}

	pub(crate) async fn parse_thing(&mut self, ctx: &mut Stk) -> ParseResult<Thing> {
		let ident = self.next_token_value::<Ident>()?.0;
		self.parse_thing_from_ident(ctx, ident).await
	}

	pub(crate) async fn parse_thing_from_ident(
		&mut self,
		ctx: &mut Stk,
		ident: String,
	) -> ParseResult<Thing> {
		expected!(self, t!(":"));

		let id = ctx.run(|ctx| self.parse_id(ctx)).await?;

		Ok(Thing {
			tb: ident,
			id,
		})
	}

	pub(crate) async fn parse_id(&mut self, stk: &mut Stk) -> ParseResult<Id> {
		let token = self.peek_whitespace();
		match token.kind {
			t!("u'") | t!("u\"") => Ok(Id::Uuid(self.next_token_value()?)),
			t!("{") => {
				self.pop_peek();
				// object record id
				let object = self.parse_object(stk, token.span).await?;
				Ok(Id::Object(object))
			}
			t!("[") => {
				self.pop_peek();
				// array record id
				let array = self.parse_array(stk, token.span).await?;
				Ok(Id::Array(array))
			}
			t!("+") => {
				self.pop_peek();
				// starting with a + so it must be a number
				let digits_token = self.peek_whitespace();
				match digits_token.kind {
					TokenKind::Digits => {}
					_ => unexpected!(self, digits_token, "an integer"),
				}

				let next = self.peek_whitespace();
				match next.kind {
					t!(".") => {
						// TODO(delskayn) explain that record-id's cant have matissas,
						// exponents or a number suffix
						unexpected!(self, next, "an integer", => "Numeric Record-id keys can only be integers");
					}
					x if Self::kind_is_identifier(x) => {
						let span = token.span.covers(next.span);
						bail!("Unexpected token `{x}` expected an integer", @span);
					}
					// allowed
					_ => {}
				}

				let digits_str = self.lexer.span_str(digits_token.span);
				if let Ok(number) = digits_str.parse() {
					Ok(Id::Number(number))
				} else {
					Ok(Id::String(digits_str.to_owned()))
				}
			}
			t!("-") => {
				self.pop_peek();
				let token = expected!(self, TokenKind::Digits);
				if let Ok(number) = self.lexer.lex_compound(token, compound::integer::<u64>) {
					// Parse to u64 and check if the value is equal to `-i64::MIN` via u64 as
					// `-i64::MIN` doesn't fit in an i64
					match number.value.cmp(&((i64::MAX as u64) + 1)) {
						Ordering::Less => Ok(Id::Number(-(number.value as i64))),
						Ordering::Equal => Ok(Id::Number(i64::MIN)),
						Ordering::Greater => {
							Ok(Id::String(format!("-{}", self.lexer.span_str(number.span))))
						}
					}
				} else {
					Ok(Id::String(format!("-{}", self.lexer.span_str(token.span))))
				}
			}
			TokenKind::Digits => {
				if self.flexible_record_id {
					let next = self.peek_whitespace1();
					if Self::kind_is_identifier(next.kind) {
						let ident = self.parse_flexible_ident()?.0;
						return Ok(Id::String(ident));
					}
				}

				self.pop_peek();

				let digits_str = self.lexer.span_str(token.span);
				if let Ok(number) = digits_str.parse::<i64>() {
					Ok(Id::Number(number))
				} else {
					Ok(Id::String(digits_str.to_owned()))
				}
			}
			TokenKind::Glued(Glued::Duration) if self.flexible_record_id => {
				let slice = self.lexer.reader.span(token.span);
				if slice.iter().any(|x| !x.is_ascii()) {
					unexpected!(self, token, "a identifier");
				}
				// Should be valid utf-8 as it was already parsed by the lexer
				let text = String::from_utf8(slice.to_vec()).unwrap();
				Ok(Id::String(text))
			}
			TokenKind::Glued(_) => {
				// If we glue before a parsing a record id, for example 123s456z would return an error as it is
				// an invalid duration, however it is a valid flexible record id identifier.
				// So calling glue before using that token to create a record id is not allowed.
				panic!(
					"Glueing tokens used in parsing a record id would result in inproper parsing"
				)
			}
			t!("ULID") => {
				self.pop_peek();
				// TODO: error message about how to use `ulid` as an identifier.
				expected!(self, t!("("));
				expected!(self, t!(")"));
				Ok(Id::Generate(Gen::Ulid))
			}
			t!("UUID") => {
				self.pop_peek();
				expected!(self, t!("("));
				expected!(self, t!(")"));
				Ok(Id::Generate(Gen::Uuid))
			}
			t!("RAND") => {
				self.pop_peek();
				expected!(self, t!("("));
				expected!(self, t!(")"));
				Ok(Id::Generate(Gen::Rand))
			}
			_ => {
				let ident = if self.flexible_record_id {
					self.parse_flexible_ident()?.0
				} else {
					self.next_token_value::<Ident>()?.0
				};
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
	use crate::sql::Value;
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
				id: Id::from(Object::from(map! {
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
				id: Id::from(Array::from(vec![Value::from("GBR"), Value::from(2022)])),
			}
		);
	}

	#[test]
	fn weird_things() {
		use crate::sql;

		fn assert_ident_parses_correctly(ident: &str) {
			let thing = format!("t:{}", ident);
			let mut parser = Parser::new(thing.as_bytes());
			parser.allow_fexible_record_id(true);
			let mut stack = Stack::new();
			let r = stack
				.enter(|ctx| async move { parser.parse_thing(ctx).await })
				.finish()
				.unwrap_or_else(|_| panic!("failed on {}", ident))
				.id;
			assert_eq!(r, Id::from(ident.to_string()),);

			let mut parser = Parser::new(thing.as_bytes());
			let r = stack
				.enter(|ctx| async move { parser.parse_query(ctx).await })
				.finish()
				.unwrap_or_else(|_| panic!("failed on {}", ident));

			assert_eq!(
				r,
				sql::Query(sql::Statements(vec![sql::Statement::Value(sql::Value::Thing(
					sql::Thing {
						tb: "t".to_string(),
						id: Id::from(ident.to_string())
					}
				))]))
			)
		}

		assert_ident_parses_correctly("123abc");
		assert_ident_parses_correctly("123d");
		assert_ident_parses_correctly("123de");
		assert_ident_parses_correctly("123dec");
		assert_ident_parses_correctly("1e23dec");
		assert_ident_parses_correctly("1e23f");
		assert_ident_parses_correctly("123f");
		assert_ident_parses_correctly("1ns");
		assert_ident_parses_correctly("1ns1");
		assert_ident_parses_correctly("1ns1h");
		assert_ident_parses_correctly("000e8");
		assert_ident_parses_correctly("000e8bla");

		assert_ident_parses_correctly("y123");
		assert_ident_parses_correctly("w123");
		assert_ident_parses_correctly("d123");
		assert_ident_parses_correctly("h123");
		assert_ident_parses_correctly("m123");
		assert_ident_parses_correctly("s123");
		assert_ident_parses_correctly("ms123");
		assert_ident_parses_correctly("us123");
		assert_ident_parses_correctly("ns123");
		assert_ident_parses_correctly("dec123");
		assert_ident_parses_correctly("f123");
		assert_ident_parses_correctly("e123");
	}
}
