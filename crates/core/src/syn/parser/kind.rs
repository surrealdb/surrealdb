use std::collections::BTreeMap;

use reblessive::Stk;

use super::basic::NumberToken;
use super::mac::unexpected;
use super::{ParseResult, Parser};
use crate::sql::Kind;
use crate::sql::kind::{GeometryKind, KindLiteral};
use crate::syn::lexer::compound;
use crate::syn::parser::mac::expected;
use crate::syn::token::{Glued, Keyword, Span, TokenKind, t};
use crate::types::PublicDuration;

impl Parser<'_> {
	/// Parse a kind production.
	///
	/// # Parser State
	/// expects the first `<` to already be eaten
	pub(crate) async fn parse_kind(&mut self, stk: &mut Stk, delim: Span) -> ParseResult<Kind> {
		let kind = self.parse_inner_kind(stk).await?;
		self.expect_closing_delimiter(t!(">"), delim)?;
		Ok(kind)
	}

	/// Parse an inner kind, a kind without enclosing `<` `>`.
	pub(crate) async fn parse_inner_kind(&mut self, stk: &mut Stk) -> ParseResult<Kind> {
		match self.parse_inner_single_kind(stk).await? {
			Kind::Any => Ok(Kind::Any),
			first => {
				if self.peek_kind() == t!("|") {
					let mut kind = vec![first];
					while self.eat(t!("|")) {
						kind.push(stk.run(|ctx| self.parse_concrete_kind(ctx)).await?);
					}
					let kind = Kind::either(kind);
					Ok(kind)
				} else {
					Ok(first)
				}
			}
		}
	}

	/// Parse a single inner kind, a kind without enclosing `<` `>`.
	pub(super) async fn parse_inner_single_kind(&mut self, stk: &mut Stk) -> ParseResult<Kind> {
		match self.peek_kind() {
			t!("ANY") => {
				self.pop_peek();
				Ok(Kind::Any)
			}
			t!("OPTION") => {
				self.pop_peek();

				let delim = expected!(self, t!("<")).span;
				let mut kinds =
					vec![Kind::None, stk.run(|ctx| self.parse_concrete_kind(ctx)).await?];
				if self.peek_kind() == t!("|") {
					while self.eat(t!("|")) {
						kinds.push(stk.run(|ctx| self.parse_concrete_kind(ctx)).await?);
					}
				}
				self.expect_closing_delimiter(t!(">"), delim)?;
				Ok(Kind::either(kinds))
			}
			_ => stk.run(|ctx| self.parse_concrete_kind(ctx)).await,
		}
	}

	/// Parse a single kind which is not any, option, or either.
	async fn parse_concrete_kind(&mut self, stk: &mut Stk) -> ParseResult<Kind> {
		if Self::token_can_be_literal_kind(self.peek_kind()) {
			let literal = self.parse_literal_kind(stk).await?;
			return Ok(Kind::Literal(literal));
		}

		let next = self.next();
		match next.kind {
			t!("BOOL") => Ok(Kind::Bool),
			t!("NONE") => Ok(Kind::None),
			t!("NULL") => Ok(Kind::Null),
			t!("BYTES") => Ok(Kind::Bytes),
			t!("DATETIME") => Ok(Kind::Datetime),
			t!("DECIMAL") => Ok(Kind::Decimal),
			t!("DURATION") => Ok(Kind::Duration),
			t!("FLOAT") => Ok(Kind::Float),
			t!("INT") => Ok(Kind::Int),
			t!("NUMBER") => Ok(Kind::Number),
			t!("OBJECT") => Ok(Kind::Object),
			t!("POINT") => Ok(Kind::Geometry(vec![GeometryKind::Point])),
			t!("STRING") => Ok(Kind::String),
			t!("UUID") => Ok(Kind::Uuid),
			t!("RANGE") => Ok(Kind::Range),
			t!("REGEX") => Ok(Kind::Regex),
			t!("FUNCTION") => Ok(Kind::Function(Default::default(), Default::default())),
			t!("RECORD") => {
				let span = self.peek().span;
				if self.eat(t!("<")) {
					let mut tables = vec![self.parse_ident()?];
					while self.eat(t!("|")) {
						tables.push(self.parse_ident()?);
					}
					self.expect_closing_delimiter(t!(">"), span)?;
					Ok(Kind::Record(tables))
				} else {
					Ok(Kind::Record(Vec::new()))
				}
			}
			t!("TABLE") => {
				let span = self.peek().span;
				if self.eat(t!("<")) {
					let mut tables = vec![self.parse_ident()?];
					while self.eat(t!("|")) {
						tables.push(self.parse_ident()?);
					}
					self.expect_closing_delimiter(t!(">"), span)?;
					Ok(Kind::Table(tables))
				} else {
					Ok(Kind::Table(Vec::new()))
				}
			}
			t!("GEOMETRY") => {
				let span = self.peek().span;
				if self.eat(t!("<")) {
					let mut kind = vec![self.parse_geometry_kind()?];
					while self.eat(t!("|")) {
						kind.push(self.parse_geometry_kind()?);
					}
					self.expect_closing_delimiter(t!(">"), span)?;
					Ok(Kind::Geometry(kind))
				} else {
					Ok(Kind::Geometry(Vec::new()))
				}
			}
			t!("ARRAY") => {
				let span = self.peek().span;
				if self.eat(t!("<")) {
					let kind = stk.run(|ctx| self.parse_inner_kind(ctx)).await?;
					let size = self.eat(t!(",")).then(|| self.next_token_value()).transpose()?;
					self.expect_closing_delimiter(t!(">"), span)?;
					Ok(Kind::Array(Box::new(kind), size))
				} else {
					Ok(Kind::Array(Box::new(Kind::Any), None))
				}
			}
			t!("SET") => {
				let span = self.peek().span;
				if self.eat(t!("<")) {
					let kind = stk.run(|ctx| self.parse_inner_kind(ctx)).await?;
					let size = self.eat(t!(",")).then(|| self.next_token_value()).transpose()?;
					self.expect_closing_delimiter(t!(">"), span)?;
					Ok(Kind::Set(Box::new(kind), size))
				} else {
					Ok(Kind::Set(Box::new(Kind::Any), None))
				}
			}
			t!("FILE") => {
				let span = self.peek().span;
				if self.eat(t!("<")) {
					let mut buckets = vec![self.parse_ident()?];
					while self.eat(t!("|")) {
						buckets.push(self.parse_ident()?);
					}
					self.expect_closing_delimiter(t!(">"), span)?;
					Ok(Kind::File(buckets))
				} else {
					Ok(Kind::File(Vec::new()))
				}
			}
			_ => unexpected!(self, next, "a kind name"),
		}
	}

	/// Parse the kind of gemoetry
	fn parse_geometry_kind(&mut self) -> ParseResult<GeometryKind> {
		let next = self.next();
		match next.kind {
			TokenKind::Keyword(keyword) => match keyword {
				Keyword::Point => Ok(GeometryKind::Point),
				Keyword::Line => Ok(GeometryKind::Line),
				Keyword::Polygon => Ok(GeometryKind::Polygon),
				Keyword::MultiPoint => Ok(GeometryKind::MultiPoint),
				Keyword::MultiLine => Ok(GeometryKind::MultiLine),
				Keyword::MultiPolygon => Ok(GeometryKind::MultiPolygon),
				Keyword::Collection => Ok(GeometryKind::Collection),
				_ => unexpected!(self, next, "a geometry kind name"),
			},
			_ => unexpected!(self, next, "a geometry kind name"),
		}
	}

	/// Parse a literal kind
	async fn parse_literal_kind(&mut self, stk: &mut Stk) -> ParseResult<KindLiteral> {
		let peek = self.peek();
		match peek.kind {
			t!("true") => {
				self.pop_peek();
				Ok(KindLiteral::Bool(true))
			}
			t!("false") => {
				self.pop_peek();
				Ok(KindLiteral::Bool(false))
			}
			t!("'") | t!("\"") => {
				let s = self.parse_string_lit()?;
				Ok(KindLiteral::String(s))
			}
			t!("+") | t!("-") | TokenKind::Glued(Glued::Number) => {
				let kind = self.next_token_value::<NumberToken>()?;
				let kind = match kind {
					NumberToken::Float(f) => KindLiteral::Float(f),
					NumberToken::Integer(i) => KindLiteral::Integer(i),
					NumberToken::Decimal(d) => KindLiteral::Decimal(d),
				};
				Ok(kind)
			}
			TokenKind::Glued(Glued::Duration) => self.next_token_value().map(KindLiteral::Duration),
			TokenKind::Digits => {
				self.pop_peek();
				let compound = self.lexer.lex_compound(peek, compound::numeric)?;
				let v = match compound.value {
					compound::Numeric::Integer(x) => KindLiteral::Integer(x),
					compound::Numeric::Float(x) => KindLiteral::Float(x),
					compound::Numeric::Decimal(x) => KindLiteral::Decimal(x),
					compound::Numeric::Duration(x) => {
						KindLiteral::Duration(PublicDuration::from_std(x))
					}
				};
				Ok(v)
			}
			t!("{") => {
				self.pop_peek();
				let mut obj = BTreeMap::new();
				while !self.eat(t!("}")) {
					let key = self.parse_object_key()?;
					expected!(self, t!(":"));
					let kind = stk.run(|ctx| self.parse_inner_kind(ctx)).await?;
					obj.insert(key, kind);
					self.eat(t!(","));
				}
				Ok(KindLiteral::Object(obj))
			}
			t!("[") => {
				self.pop_peek();
				let mut arr = Vec::new();
				while !self.eat(t!("]")) {
					let kind = stk.run(|ctx| self.parse_inner_kind(ctx)).await?;
					arr.push(kind);
					self.eat(t!(","));
				}
				Ok(KindLiteral::Array(arr))
			}
			_ => unexpected!(self, peek, "a literal kind"),
		}
	}

	fn token_can_be_literal_kind(t: TokenKind) -> bool {
		matches!(
			t,
			t!("true")
				| t!("false")
				| t!("'") | t!("\"")
				| t!("+") | t!("-")
				| TokenKind::Glued(Glued::Duration | Glued::Number)
				| TokenKind::Digits
				| t!("{") | t!("[")
		)
	}
}

#[cfg(test)]
mod tests {
	use reblessive::Stack;
	use rstest::rstest;
use surrealdb_types::ToSql;

	use super::*;

	fn kind(i: &str) -> ParseResult<Kind> {
		let mut parser = Parser::new(i.as_bytes());
		let mut stack = Stack::new();
		stack.enter(|ctx| parser.parse_inner_kind(ctx)).finish()
	}

	#[rstest]
	#[case::any("any", "any", Kind::Any)]
	#[case::none("none", "none", Kind::None)]
	#[case::null("null", "null", Kind::Null)]
	#[case::bool("bool", "bool", Kind::Bool)]
	#[case::bytes("bytes", "bytes", Kind::Bytes)]
	#[case::datetime("datetime", "datetime", Kind::Datetime)]
	#[case::decimal("decimal", "decimal", Kind::Decimal)]
	#[case::duration("duration", "duration", Kind::Duration)]
	#[case::float("float", "float", Kind::Float)]
	#[case::number("number", "number", Kind::Number)]
	#[case::object("object", "object", Kind::Object)]
	#[case::point("point", "geometry<point>", Kind::Geometry(vec![GeometryKind::Point]))]
	#[case::string("string", "string", Kind::String)]
	#[case::uuid("uuid", "uuid", Kind::Uuid)]
	#[case::either("int | float", "int | float", Kind::Either(vec![Kind::Int, Kind::Float]))]
	#[case::record("record", "record", Kind::Record(vec![]))]
	#[case::record_one("record<person>", "record<person>", Kind::Record(vec!["person".to_owned()]))]
	#[case::record_many("record<person | animal>", "record<person | animal>", Kind::Record(vec!["person".to_owned(), "animal".to_owned()]))]
	#[case::table("table", "table", Kind::Table(vec![]))]
	#[case::table_one("table<person>", "table<person>", Kind::Table(vec!["person".to_owned()]))]
	#[case::table_many("table<person | animal>", "table<person | animal>", Kind::Table(vec!["person".to_owned(), "animal".to_owned()]))]
	#[case::geometry("geometry", "geometry", Kind::Geometry(vec![]))]
	#[case::geometry_one("geometry<point>", "geometry<point>", Kind::Geometry(vec![GeometryKind::Point]))]
	#[case::geometry_many("geometry<point | multipoint>", "geometry<point | multipoint>", Kind::Geometry(vec![GeometryKind::Point, GeometryKind::MultiPoint]))]
	#[case::option_one("option<int>", "none | int", Kind::Either(vec![Kind::None, Kind::Int]))]
	#[case::option_many("option<int | float>", "none | int | float", Kind::Either(vec![Kind::None, Kind::Int, Kind::Float]))]
	#[case::none_tuple("none | int | float", "none | int | float", Kind::Either(vec![Kind::None, Kind::Int, Kind::Float]))]
	#[case::array_any("array", "array", Kind::Array(Box::new(Kind::Any), None))]
	#[case::array_some("array<float>", "array<float>", Kind::Array(Box::new(Kind::Float), None))]
	#[case::array_some_size(
		"array<float, 10>",
		"array<float, 10>",
		Kind::Array(Box::new(Kind::Float), Some(10))
	)]
	#[case::set_any("set", "set", Kind::Set(Box::new(Kind::Any), None))]
	#[case::set_some("set<float>", "set<float>", Kind::Set(Box::new(Kind::Float), None))]
	#[case::set_some_size(
		"set<float, 10>",
		"set<float, 10>",
		Kind::Set(Box::new(Kind::Float), Some(10))
	)]
	#[case::function_any("function", "function", Kind::Function(None, None))]
	#[case::file_record_any("file", "file", Kind::File(vec![]))]
	#[case::file_record_one("file<one>", "file<one>", Kind::File(vec!["one".to_owned()]))]
	#[case::file_record_many("file<one | two>", "file<one | two>", Kind::File(vec!["one".to_string(), "two".to_string()]))]
	fn test_kind(#[case] sql: &str, #[case] expected_str: &str, #[case] expected_kind: Kind) {
		let res = kind(sql);
		let out = res.unwrap();
		assert_eq!(expected_str, out.to_sql());
		assert_eq!(expected_kind, out);
	}
}
