use std::cmp::Ordering;
use std::ops::Bound;

use reblessive::Stk;

use super::{ParseResult, Parser};
use crate::sql::lookup::LookupSubject;
use crate::sql::{Ident, Param, RecordIdKeyGen, RecordIdKeyLit, RecordIdKeyRangeLit, RecordIdLit};
use crate::syn::error::bail;
use crate::syn::lexer::compound;
use crate::syn::parser::mac::{expected, expected_whitespace, unexpected};
use crate::syn::token::{Glued, TokenKind, t};
use crate::val::Strand;

impl Parser<'_> {
	pub(crate) async fn parse_record_string(
		&mut self,
		stk: &mut Stk,
		double: bool,
	) -> ParseResult<RecordIdLit> {
		let thing = self.parse_record_id(stk).await?;

		if double {
			expected_whitespace!(self, t!("\""));
		} else {
			expected_whitespace!(self, t!("'"));
		};
		Ok(thing)
	}

	pub(crate) async fn parse_record_id_or_range(
		&mut self,
		stk: &mut Stk,
		ident: Ident,
	) -> ParseResult<RecordIdLit> {
		expected_whitespace!(self, t!(":"));

		// If self starts with a range operator self is a range with no start bound
		if self.eat_whitespace(t!("..")) {
			// Check for inclusive
			let end = if self.eat_whitespace(t!("=")) {
				let id = stk.run(|stk| self.parse_record_id_key(stk)).await?;
				Bound::Included(id)
			} else if Self::kind_starts_record_id_key(self.peek_whitespace().kind) {
				let id = stk.run(|stk| self.parse_record_id_key(stk)).await?;
				Bound::Excluded(id)
			} else {
				Bound::Unbounded
			};
			return Ok(RecordIdLit {
				table: ident.into_string(),
				key: RecordIdKeyLit::Range(Box::new(RecordIdKeyRangeLit {
					start: Bound::Unbounded,
					end,
				})),
			});
		}

		// Didn't eat range yet so we need to parse the id.
		let beg = if Self::kind_starts_record_id_key(self.peek_whitespace().kind) {
			let v = stk.run(|stk| self.parse_record_id_key(stk)).await?;

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
				let id = stk.run(|stk| self.parse_record_id_key(stk)).await?;
				Bound::Included(id)
			} else if Self::kind_starts_record_id_key(self.peek_whitespace().kind) {
				let id = stk.run(|stk| self.parse_record_id_key(stk)).await?;
				Bound::Excluded(id)
			} else {
				Bound::Unbounded
			};
			Ok(RecordIdLit {
				table: ident.into_string(),
				key: RecordIdKeyLit::Range(Box::new(RecordIdKeyRangeLit {
					start: beg,
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
			Ok(RecordIdLit {
				table: ident.into_string(),
				key: id,
			})
		}
	}

	pub(crate) async fn parse_id_range(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<RecordIdKeyRangeLit> {
		let beg = if Self::kind_starts_record_id_key(self.peek_whitespace().kind) {
			let v = stk.run(|stk| self.parse_record_id_key(stk)).await?;

			// check for exclusive
			if self.eat_whitespace(t!(">")) {
				Bound::Excluded(v)
			} else {
				Bound::Included(v)
			}
		} else {
			Bound::Unbounded
		};

		expected!(self, t!(".."));

		let end = if self.eat_whitespace(t!("=")) {
			let id = stk.run(|stk| self.parse_record_id_key(stk)).await?;
			Bound::Included(id)
		} else if Self::kind_starts_record_id_key(self.peek_whitespace().kind) {
			let id = stk.run(|stk| self.parse_record_id_key(stk)).await?;
			Bound::Excluded(id)
		} else {
			Bound::Unbounded
		};

		Ok(RecordIdKeyRangeLit {
			start: beg,
			end,
		})
	}

	pub(crate) async fn parse_lookup_subject(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<LookupSubject> {
		let tb = self.next_token_value()?;
		if self.eat_whitespace(t!(":")) {
			let rng = self.parse_id_range(stk).await?;
			Ok(LookupSubject::Range(tb, rng))
		} else {
			Ok(LookupSubject::Table(tb))
		}
	}

	pub(crate) async fn parse_record_id_with_range(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<RecordIdLit> {
		let ident = self.next_token_value::<Ident>()?;
		self.parse_record_id_or_range(stk, ident).await
	}

	pub(crate) async fn parse_record_id(&mut self, stk: &mut Stk) -> ParseResult<RecordIdLit> {
		let ident = self.next_token_value::<Ident>()?.into_string();
		self.parse_record_id_from_ident(stk, ident).await
	}

	pub(crate) async fn parse_record_id_from_ident(
		&mut self,
		stk: &mut Stk,
		ident: String,
	) -> ParseResult<RecordIdLit> {
		expected!(self, t!(":"));

		let id = stk.run(|ctx| self.parse_record_id_key(ctx)).await?;

		Ok(RecordIdLit {
			table: ident,
			key: id,
		})
	}

	pub(crate) async fn parse_record_id_key(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<RecordIdKeyLit> {
		let token = self.peek_whitespace();
		match token.kind {
			t!("u'") | t!("u\"") => Ok(RecordIdKeyLit::Uuid(self.next_token_value()?)),
			t!("{") => {
				self.pop_peek();
				// object record id
				let object = self.parse_object(stk, token.span).await?;
				Ok(RecordIdKeyLit::Object(object))
			}
			t!("[") => {
				self.pop_peek();
				// array record id
				let array = self.parse_array(stk, token.span).await?;
				Ok(RecordIdKeyLit::Array(array))
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
					Ok(RecordIdKeyLit::Number(number))
				} else {
					// Safety: Parser guarentees no null bytes present in string.
					Ok(RecordIdKeyLit::String(unsafe {
						Strand::new_unchecked(digits_str.to_owned())
					}))
				}
			}
			t!("-") => {
				self.pop_peek();
				let token = expected!(self, TokenKind::Digits);
				if let Ok(number) = self.lexer.lex_compound(token, compound::integer::<u64>) {
					// Parse to u64 and check if the value is equal to `-i64::MIN` via u64 as
					// `-i64::MIN` doesn't fit in an i64
					match number.value.cmp(&((i64::MAX as u64) + 1)) {
						Ordering::Less => Ok(RecordIdKeyLit::Number(-(number.value as i64))),
						Ordering::Equal => Ok(RecordIdKeyLit::Number(i64::MIN)),
						// Safety: Parser guarentees no null bytes present in string.
						Ordering::Greater => Ok(RecordIdKeyLit::String(unsafe {
							Strand::new_unchecked(format!("-{}", self.lexer.span_str(number.span)))
						})),
					}
				} else {
					// Safety: Parser guarentees no null bytes present in string.
					let strand = unsafe {
						Strand::new_unchecked(format!("-{}", self.lexer.span_str(token.span)))
					};
					Ok(RecordIdKeyLit::String(strand))
				}
			}
			TokenKind::Digits => {
				if self.settings.flexible_record_id {
					let next = self.peek_whitespace1();
					if Self::kind_is_identifier(next.kind) {
						let ident = self.parse_flexible_ident()?;
						return Ok(RecordIdKeyLit::String(ident.into_strand()));
					}
				}

				self.pop_peek();

				let digits_str = self.lexer.span_str(token.span);
				if let Ok(number) = digits_str.parse::<i64>() {
					Ok(RecordIdKeyLit::Number(number))
				} else {
					// Safety: Parser guarentees no null bytes present in string.
					Ok(RecordIdKeyLit::String(unsafe {
						Strand::new_unchecked(digits_str.to_owned())
					}))
				}
			}
			TokenKind::Glued(Glued::Duration) if self.settings.flexible_record_id => {
				let slice = self.lexer.reader.span(token.span);
				if slice.iter().any(|x| !x.is_ascii()) {
					unexpected!(self, token, "a identifier");
				}
				// Should be valid utf-8 as it was already parsed by the lexer
				let text = String::from_utf8(slice.to_vec()).unwrap();
				// Safety: Parser guarentees no null bytes present in string.
				let text = unsafe { Strand::new_unchecked(text) };
				Ok(RecordIdKeyLit::String(text))
			}
			TokenKind::Glued(_) => {
				// If we glue before a parsing a record id, for example 123s456z would return an
				// error as it is an invalid duration, however it is a valid flexible record
				// id identifier. So calling glue before using that token to create a record
				// id is not allowed.
				panic!(
					"Glueing tokens used in parsing a record id would result in inproper parsing"
				)
			}
			t!("ULID") => {
				let token = self.pop_peek();
				if self.eat(t!("(")) {
					expected!(self, t!(")"));
					Ok(RecordIdKeyLit::Generate(RecordIdKeyGen::Ulid))
				} else {
					let slice = self.lexer.span_str(token.span);
					// Safety: Parser guarentees no null bytes present in string.
					let text = unsafe { Strand::new_unchecked(slice.to_owned()) };
					Ok(RecordIdKeyLit::String(text))
				}
			}
			t!("UUID") => {
				let token = self.pop_peek();
				if self.eat(t!("(")) {
					expected!(self, t!(")"));
					Ok(RecordIdKeyLit::Generate(RecordIdKeyGen::Uuid))
				} else {
					let slice = self.lexer.span_str(token.span);
					// Safety: Parser guarentees no null bytes present in string.
					let text = unsafe { Strand::new_unchecked(slice.to_owned()) };
					Ok(RecordIdKeyLit::String(text))
				}
			}
			t!("RAND") => {
				let token = self.pop_peek();
				if self.eat(t!("(")) {
					expected!(self, t!(")"));
					Ok(RecordIdKeyLit::Generate(RecordIdKeyGen::Rand))
				} else {
					let slice = self.lexer.span_str(token.span);
					// Safety: Parser guarentees no null bytes present in string.
					let text = unsafe { Strand::new_unchecked(slice.to_owned()) };
					Ok(RecordIdKeyLit::String(text))
				}
			}
			_ => {
				let ident = if self.settings.flexible_record_id {
					self.parse_flexible_ident()?
				} else {
					self.next_token_value::<Ident>()?
				};
				Ok(RecordIdKeyLit::String(ident.into_strand()))
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use reblessive::Stack;

	use super::*;
	use crate::sql::{Expr, Literal};
	use crate::syn::parser::ParserSettings;
	use crate::{sql, syn};

	fn thing(i: &str) -> ParseResult<RecordIdLit> {
		let mut parser = Parser::new(i.as_bytes());
		let mut stack = Stack::new();
		stack.enter(|ctx| async move { parser.parse_record_id(ctx).await }).finish()
	}

	#[test]
	fn thing_normal() {
		let sql = "test:id";
		let res = thing(sql);
		let out = res.unwrap();
		assert_eq!("test:id", format!("{}", out));
		assert_eq!(
			out,
			RecordIdLit {
				table: String::from("test"),
				key: RecordIdKeyLit::String(strand!("id").to_owned()),
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
			RecordIdLit {
				table: String::from("test"),
				key: RecordIdKeyLit::Number(1),
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
			RecordIdLit {
				table: String::from("test"),
				key: RecordIdKeyLit::Number(i64::MIN),
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
			RecordIdLit {
				table: String::from("test"),
				key: RecordIdKeyLit::Number(i64::MAX),
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
			RecordIdLit {
				table: String::from("test"),
				key: RecordIdKeyLit::String(Strand::new(max_str).unwrap()),
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
			RecordIdLit {
				table: String::from("test"),
				key: RecordIdKeyLit::String(Strand::new(min_str).unwrap()),
			}
		);
	}

	#[test]
	fn thing_string() {
		let sql = "r'test:001'";
		let res = syn::expr(sql).unwrap();
		let sql::Expr::Literal(sql::Literal::RecordId(out)) = res else {
			panic!()
		};
		assert_eq!("test:1", format!("{}", out));
		assert_eq!(
			out,
			RecordIdLit {
				table: String::from("test"),
				key: RecordIdKeyLit::Number(1),
			}
		);

		let sql = "r'test:001'";
		let res = syn::expr(sql).unwrap();
		let sql::Expr::Literal(sql::Literal::RecordId(out)) = res else {
			panic!()
		};
		assert_eq!("test:1", format!("{}", out));
		assert_eq!(
			out,
			RecordIdLit {
				table: String::from("test"),
				key: RecordIdKeyLit::Number(1),
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
			RecordIdLit {
				table: String::from("test"),
				key: RecordIdKeyLit::String(strand!("id").to_owned()),
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
			RecordIdLit {
				table: String::from("test"),
				key: RecordIdKeyLit::String(strand!("id").to_owned()),
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
			RecordIdLit {
				table: String::from("test"),
				key: RecordIdKeyLit::Object(vec![
					sql::literal::ObjectEntry {
						key: "location".to_string(),
						value: sql::Expr::Literal(sql::Literal::Strand(strand!("GBR").to_owned()))
					},
					sql::literal::ObjectEntry {
						key: "year".to_string(),
						value: sql::Expr::Literal(sql::Literal::Integer(2022)),
					},
				])
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
			RecordIdLit {
				table: String::from("test"),
				key: RecordIdKeyLit::Array(vec![
					sql::Expr::Literal(sql::Literal::Strand(strand!("GBR").to_owned())),
					sql::Expr::Literal(sql::Literal::Integer(2022)),
				])
			}
		);
	}

	#[test]
	fn weird_things() {
		use crate::sql;

		fn assert_ident_parses_correctly(ident: &str) {
			let thing = format!("t:{}", ident);
			let mut parser = Parser::new_with_settings(
				thing.as_bytes(),
				ParserSettings {
					flexible_record_id: true,
					..Default::default()
				},
			);
			let mut stack = Stack::new();
			let r = stack
				.enter(|ctx| async move { parser.parse_record_id(ctx).await })
				.finish()
				.unwrap_or_else(|_| panic!("failed on {}", ident))
				.key;
			assert_eq!(r, RecordIdKeyLit::String(Strand::new(ident.to_string()).unwrap()),);

			let mut parser = Parser::new(thing.as_bytes());
			let r = stack
				.enter(|ctx| async move { parser.parse_expr_inherit(ctx).await })
				.finish()
				.unwrap_or_else(|_| panic!("failed on {}", ident));

			assert_eq!(
				r,
				Expr::Literal(Literal::RecordId(sql::RecordIdLit {
					table: "t".to_string(),
					key: RecordIdKeyLit::String(Strand::new(ident.to_string()).unwrap())
				}))
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

		assert_ident_parses_correctly("ulid");
		assert_ident_parses_correctly("uuid");
		assert_ident_parses_correctly("rand");
	}
}
