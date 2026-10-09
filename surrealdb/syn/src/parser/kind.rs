use std::collections::BTreeMap;

use surrealdb_sql::Kind;
use surrealdb_sql::kind::{GeometryKind, KindLiteral};

use super::mac::unexpected;
use super::{ParseResult, Parser};
use crate::lexer::compound;
use crate::parser::enter_object_recursion;
use crate::parser::mac::expected;
use crate::token::{Keyword, Span, TokenKind, t};

impl Parser<'_> {
	/// Parse a kind production.
	///
	/// # Parser State
	/// expects the first `<` to already be eaten
	pub fn parse_kind(&mut self, delim: Span) -> ParseResult<Kind> {
		let kind = self.parse_inner_kind()?;
		self.expect_closing_delimiter(t!(">"), delim)?;
		Ok(kind)
	}

	/// Parse an inner kind, a kind without enclosing `<` `>`.
	pub fn parse_inner_kind(&mut self) -> ParseResult<Kind> {
		match self.parse_inner_single_kind()? {
			Kind::Any => Ok(Kind::Any),
			first => {
				if self.peek_kind() == t!("|") {
					let mut kind = vec![first];
					while self.eat(t!("|")) {
						kind.push(self.parse_concrete_kind()?);
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
	pub(super) fn parse_inner_single_kind(&mut self) -> ParseResult<Kind> {
		match self.peek_kind() {
			t!("ANY") => {
				self.pop_peek();
				Ok(Kind::Any)
			}
			t!("OPTION") => {
				self.pop_peek();
				enter_object_recursion!(this = self => {
					let delim = expected!(this, t!("<")).span;
					let mut kinds =
						vec![Kind::None, this.parse_concrete_kind()?];
					if this.peek_kind() == t!("|") {
						while this.eat(t!("|")) {
							kinds.push(this.parse_concrete_kind()?);
						}
					}
					this.expect_closing_delimiter(t!(">"), delim)?;
					Ok(Kind::either(kinds))
				})
			}
			_ => self.parse_concrete_kind(),
		}
	}

	/// Parse a single kind which is not any, option, or either.
	fn parse_concrete_kind(&mut self) -> ParseResult<Kind> {
		enter_object_recursion!(this = self => {
		let next = this.next();
		match next.kind {
			t!("true") => Ok(Kind::Literal(KindLiteral::Bool(true))),
			t!("false") => Ok(Kind::Literal(KindLiteral::Bool(false))),
			t!("'") | t!("\"") | t!("+") | t!("-") | TokenKind::Digits | t!("{") | t!("[") => {
				this.parse_literal_kind_body(next)
			}
			t!("RECORD") | t!("TABLE") | t!("TB") | t!("GEOMETRY") | t!("FILE") => this.parse_list_kind_body(next),
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
			t!("ARRAY") => {
				let span = this.peek().span;
				if this.eat(t!("<")) {
					let kind = this.parse_inner_kind()?;
					let size = this.eat(t!(",")).then(|| this.next_token_value()).transpose()?;
					this.expect_closing_delimiter(t!(">"), span)?;
					Ok(Kind::Array(Box::new(kind), size))
				} else {
					Ok(Kind::Array(Box::new(Kind::Any), None))
				}
			}
			t!("SET") => {
				let span = this.peek().span;
				if this.eat(t!("<")) {
					let kind = this.parse_inner_kind()?;
					let size = this.eat(t!(",")).then(|| this.next_token_value()).transpose()?;
					this.expect_closing_delimiter(t!(">"), span)?;
					Ok(Kind::Set(Box::new(kind), size))
				} else {
					Ok(Kind::Set(Box::new(Kind::Any), None))
				}
			}
			_ => unexpected!(this, next, "a kind name"),
		}
		})
	}

	/// Parses a literal kind whose first token, `next`, is already consumed.
	///
	/// Kept out of [`Self::parse_concrete_kind`], whose frame every nested
	/// kind pays for.
	#[inline(never)]
	fn parse_literal_kind_body(&mut self, next: crate::token::Token) -> ParseResult<Kind> {
		match next.kind {
			t!("'") | t!("\"") => {
				let str = self.unescape_string_span(next.span)?;
				Ok(Kind::Literal(KindLiteral::String(str.into())))
			}
			TokenKind::NaN => Ok(Kind::Literal(KindLiteral::Float(f64::NAN))),
			TokenKind::Infinity => Ok(Kind::Literal(KindLiteral::Float(f64::INFINITY))),
			t!("+") | t!("-") => {
				let compound = self.lex_compound(next, compound::number)?;
				let kind = match compound.value {
					compound::Numeric::Float(f) => KindLiteral::Float(f),
					compound::Numeric::Integer(int) => {
						KindLiteral::Integer(int.into_int(compound.span)?)
					}
					compound::Numeric::Decimal(decimal) => KindLiteral::Decimal(decimal),
					compound::Numeric::Duration(_) => unreachable!(),
				};
				Ok(Kind::Literal(kind))
			}
			TokenKind::Digits => {
				let compound = self.lex_compound(next, compound::numeric)?;
				let v = match compound.value {
					compound::Numeric::Integer(x) => {
						KindLiteral::Integer(x.into_int(compound.span)?)
					}
					compound::Numeric::Float(x) => KindLiteral::Float(x),
					compound::Numeric::Decimal(x) => KindLiteral::Decimal(x),
					compound::Numeric::Duration(x) => KindLiteral::Duration(x),
				};
				Ok(Kind::Literal(v))
			}
			t!("{") => {
				let mut obj = BTreeMap::new();
				while !self.eat(t!("}")) {
					let key = self.parse_object_key()?;
					expected!(self, t!(":"));
					let kind = self.parse_inner_kind()?;
					obj.insert(key.into(), kind);
					self.eat(t!(","));
				}
				Ok(Kind::Literal(KindLiteral::Object(obj)))
			}
			t!("[") => {
				let mut arr = Vec::new();
				while !self.eat(t!("]")) {
					let kind = self.parse_inner_kind()?;
					arr.push(kind);
					self.eat(t!(","));
				}
				Ok(Kind::Literal(KindLiteral::Array(arr)))
			}
			_ => unexpected!(self, next, "a kind name"),
		}
	}

	/// Parses a kind taking an optional `<a | b>` list of names, whose keyword, `next`, is already
	/// consumed.
	///
	/// Kept out of [`Self::parse_concrete_kind`], whose frame every nested
	/// kind pays for.
	#[inline(never)]
	fn parse_list_kind_body(&mut self, next: crate::token::Token) -> ParseResult<Kind> {
		match next.kind {
			t!("RECORD") => {
				let span = self.peek().span;
				if self.eat(t!("<")) {
					let mut tables = vec![self.parse_ident_str()?.into()];
					while self.eat(t!("|")) {
						tables.push(self.parse_ident_str()?.into());
					}
					self.expect_closing_delimiter(t!(">"), span)?;
					Ok(Kind::Record(tables))
				} else {
					Ok(Kind::Record(Vec::new()))
				}
			}
			t!("TABLE") | t!("TB") => {
				let span = self.peek().span;
				if self.eat(t!("<")) {
					let mut tables = vec![self.parse_ident_str()?.into()];
					while self.eat(t!("|")) {
						tables.push(self.parse_ident_str()?.into());
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
			t!("FILE") => {
				let span = self.peek().span;
				if self.eat(t!("<")) {
					let mut buckets = vec![self.parse_ident()?.into_string()];
					while self.eat(t!("|")) {
						buckets.push(self.parse_ident()?.into_string());
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
}

#[cfg(test)]
mod tests {
	use rstest::rstest;
	use surrealdb_types::ToSql;

	use super::*;

	fn kind(i: &str) -> ParseResult<Kind> {
		let mut parser = Parser::new(i.as_bytes());
		parser.parse_inner_kind()
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
	#[case::record_one("record<person>", "record<person>", Kind::Record(vec!["person".into()]))]
	#[case::record_many("record<person | animal>", "record<person | animal>", Kind::Record(vec!["person".into(), "animal".into()]))]
	#[case::table("table", "table", Kind::Table(vec![]))]
	#[case::table_one("table<person>", "table<person>", Kind::Table(vec!["person".into()]))]
	#[case::table_many("table<person | animal>", "table<person | animal>", Kind::Table(vec!["person".into(), "animal".into()]))]
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
