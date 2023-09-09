use crate::sql::comment::mightbespace;
use crate::sql::common::verbar;
use crate::sql::common::{closeparentheses, commas, openparentheses};
use crate::sql::error::IResult;
use crate::sql::fmt::Fmt;
use crate::sql::table::{table, Table};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::char;
use nom::character::complete::u64;
use nom::combinator::{cut, map};
use nom::combinator::{opt, value};
use nom::multi::separated_list1;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

use super::util::{delimited_list1, expect_terminator};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[revisioned(revision = 1)]
pub enum Kind {
	Any,
	Null,
	Bool,
	Bytes,
	Datetime,
	Decimal,
	Duration,
	Float,
	Int,
	Number,
	Object,
	Point,
	String,
	Uuid,
	Record(Vec<Table>),
	Geometry(Vec<String>),
	Option(Box<Kind>),
	Either(Vec<Kind>),
	Set(Box<Kind>, Option<u64>),
	Array(Box<Kind>, Option<u64>),
}

impl Default for Kind {
	fn default() -> Self {
		Self::Any
	}
}

impl Kind {
	fn is_any(&self) -> bool {
		matches!(self, Kind::Any)
	}
}

impl From<&Kind> for Box<Kind> {
	#[inline]
	fn from(v: &Kind) -> Self {
		Box::new(v.clone())
	}
}

impl Display for Kind {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Kind::Any => f.write_str("any"),
			Kind::Null => f.write_str("null"),
			Kind::Bool => f.write_str("bool"),
			Kind::Bytes => f.write_str("bytes"),
			Kind::Datetime => f.write_str("datetime"),
			Kind::Decimal => f.write_str("decimal"),
			Kind::Duration => f.write_str("duration"),
			Kind::Float => f.write_str("float"),
			Kind::Int => f.write_str("int"),
			Kind::Number => f.write_str("number"),
			Kind::Object => f.write_str("object"),
			Kind::Point => f.write_str("point"),
			Kind::String => f.write_str("string"),
			Kind::Uuid => f.write_str("uuid"),
			Kind::Option(k) => write!(f, "option<{}>", k),
			Kind::Record(k) => match k {
				k if k.is_empty() => write!(f, "record"),
				k => write!(f, "record<{}>", Fmt::verbar_separated(k)),
			},
			Kind::Geometry(k) => match k {
				k if k.is_empty() => write!(f, "geometry"),
				k => write!(f, "geometry<{}>", Fmt::verbar_separated(k)),
			},
			Kind::Set(k, l) => match (k, l) {
				(k, None) if k.is_any() => write!(f, "set"),
				(k, None) => write!(f, "set<{k}>"),
				(k, Some(l)) => write!(f, "set<{k}, {l}>"),
			},
			Kind::Array(k, l) => match (k, l) {
				(k, None) if k.is_any() => write!(f, "array"),
				(k, None) => write!(f, "array<{k}>"),
				(k, Some(l)) => write!(f, "array<{k}, {l}>"),
			},
			Kind::Either(k) => write!(f, "{}", Fmt::verbar_separated(k)),
		}
	}
}

pub fn kind(i: &str) -> IResult<&str, Kind> {
	alt((any, either, option))(i)
}

pub fn any(i: &str) -> IResult<&str, Kind> {
	value(Kind::Any, tag("any"))(i)
}

pub fn simple(i: &str) -> IResult<&str, Kind> {
	alt((
		value(Kind::Bool, tag("bool")),
		value(Kind::Null, tag("null")),
		value(Kind::Bytes, tag("bytes")),
		value(Kind::Datetime, tag("datetime")),
		value(Kind::Decimal, tag("decimal")),
		value(Kind::Duration, tag("duration")),
		value(Kind::Float, tag("float")),
		value(Kind::Int, tag("int")),
		value(Kind::Number, tag("number")),
		value(Kind::Object, tag("object")),
		value(Kind::Point, tag("point")),
		value(Kind::String, tag("string")),
		value(Kind::Uuid, tag("uuid")),
	))(i)
}

fn either(i: &str) -> IResult<&str, Kind> {
	let (i, mut v) = separated_list1(verbar, alt((simple, geometry, record, array, set)))(i)?;
	match v.len() {
		1 => Ok((i, v.remove(0))),
		_ => Ok((i, Kind::Either(v))),
	}
}

fn option(i: &str) -> IResult<&str, Kind> {
	let (i, _) = tag("option")(i)?;
	let (i, _) = mightbespace(i)?;
	cut(|i| {
		let (i, s) = tag("<")(i)?;
		let (i, v) = map(alt((either, simple, geometry, record, array, set)), Box::new)(i)?;
		let (i, _) = expect_terminator(s, char('>'))(i)?;
		Ok((i, Kind::Option(v)))
	})(i)
}

fn record(i: &str) -> IResult<&str, Kind> {
	let (i, _) = tag("record")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) =
		opt(alt((delimited_list1(openparentheses, commas, cut(table), closeparentheses), |i| {
			let (i, s) = tag("<")(i)?;
			let (i, v) = separated_list1(verbar, table)(i)?;
			let (i, _) = expect_terminator(s, char('>'))(i)?;
			Ok((i, v))
		})))(i)?;
	Ok((i, Kind::Record(v.unwrap_or_default())))
}

fn geometry(i: &str) -> IResult<&str, Kind> {
	let (i, _) = tag("geometry")(i)?;
	let (i, v) =
		opt(alt((delimited_list1(openparentheses, commas, cut(geo), closeparentheses), |i| {
			let (i, _) = mightbespace(i)?;
			let (i, s) = tag("<")(i)?;
			let (i, v) = separated_list1(verbar, cut(geo))(i)?;
			let (i, _) = expect_terminator(s, char('>'))(i)?;
			Ok((i, v))
		})))(i)?;
	Ok((i, Kind::Geometry(v.unwrap_or_default())))
}

fn array(i: &str) -> IResult<&str, Kind> {
	let (i, _) = tag("array")(i)?;
	let (i, v) = opt(|i| {
		let (i, s) = tag("<")(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, k) = kind(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, l) = opt(|i| {
			let (i, _) = char(',')(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, l) = u64(i)?;
			let (i, _) = mightbespace(i)?;
			Ok((i, l))
		})(i)?;
		let (i, _) = expect_terminator(s, char('>'))(i)?;
		Ok((i, (k, l)))
	})(i)?;
	Ok((
		i,
		match v {
			Some((k, l)) => Kind::Array(Box::new(k), l),
			None => Kind::Array(Box::new(Kind::Any), None),
		},
	))
}

fn set(i: &str) -> IResult<&str, Kind> {
	let (i, _) = tag("set")(i)?;
	let (i, v) = opt(|i| {
		let (i, s) = tag("<")(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, k) = kind(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, l) = opt(|i| {
			let (i, _) = char(',')(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, l) = u64(i)?;
			let (i, _) = mightbespace(i)?;
			Ok((i, l))
		})(i)?;
		let (i, _) = expect_terminator(s, char('>'))(i)?;
		Ok((i, (k, l)))
	})(i)?;
	Ok((
		i,
		match v {
			Some((k, l)) => Kind::Set(Box::new(k), l),
			None => Kind::Set(Box::new(Kind::Any), None),
		},
	))
}

fn geo(i: &str) -> IResult<&str, String> {
	map(
		alt((
			tag("feature"),
			tag("point"),
			tag("line"),
			tag("polygon"),
			tag("multipoint"),
			tag("multiline"),
			tag("multipolygon"),
			tag("collection"),
		)),
		String::from,
	)(i)
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::table::Table;

	#[test]
	fn kind_any() {
		let sql = "any";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("any", format!("{}", out));
		assert_eq!(out, Kind::Any);
	}

	#[test]
	fn kind_null() {
		let sql = "null";
		let res = kind(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("null", format!("{}", out));
		assert_eq!(out, Kind::Null);
	}

	#[test]
	fn kind_bool() {
		let sql = "bool";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("bool", format!("{}", out));
		assert_eq!(out, Kind::Bool);
	}

	#[test]
	fn kind_bytes() {
		let sql = "bytes";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("bytes", format!("{}", out));
		assert_eq!(out, Kind::Bytes);
	}

	#[test]
	fn kind_datetime() {
		let sql = "datetime";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("datetime", format!("{}", out));
		assert_eq!(out, Kind::Datetime);
	}

	#[test]
	fn kind_decimal() {
		let sql = "decimal";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("decimal", format!("{}", out));
		assert_eq!(out, Kind::Decimal);
	}

	#[test]
	fn kind_duration() {
		let sql = "duration";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("duration", format!("{}", out));
		assert_eq!(out, Kind::Duration);
	}

	#[test]
	fn kind_float() {
		let sql = "float";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("float", format!("{}", out));
		assert_eq!(out, Kind::Float);
	}

	#[test]
	fn kind_number() {
		let sql = "number";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("number", format!("{}", out));
		assert_eq!(out, Kind::Number);
	}

	#[test]
	fn kind_object() {
		let sql = "object";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("object", format!("{}", out));
		assert_eq!(out, Kind::Object);
	}

	#[test]
	fn kind_point() {
		let sql = "point";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("point", format!("{}", out));
		assert_eq!(out, Kind::Point);
	}

	#[test]
	fn kind_string() {
		let sql = "string";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("string", format!("{}", out));
		assert_eq!(out, Kind::String);
	}

	#[test]
	fn kind_uuid() {
		let sql = "uuid";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("uuid", format!("{}", out));
		assert_eq!(out, Kind::Uuid);
	}

	#[test]
	fn kind_either() {
		let sql = "int | float";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("int | float", format!("{}", out));
		assert_eq!(out, Kind::Either(vec![Kind::Int, Kind::Float]));
	}

	#[test]
	fn kind_record_any() {
		let sql = "record";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("record", format!("{}", out));
		assert_eq!(out, Kind::Record(vec![]));
	}

	#[test]
	fn kind_record_one() {
		let sql = "record<person>";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("record<person>", format!("{}", out));
		assert_eq!(out, Kind::Record(vec![Table::from("person")]));
	}

	#[test]
	fn kind_record_many() {
		let sql = "record<person | animal>";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("record<person | animal>", format!("{}", out));
		assert_eq!(out, Kind::Record(vec![Table::from("person"), Table::from("animal")]));
	}

	#[test]
	fn kind_geometry_any() {
		let sql = "geometry";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("geometry", format!("{}", out));
		assert_eq!(out, Kind::Geometry(vec![]));
	}

	#[test]
	fn kind_geometry_one() {
		let sql = "geometry<point>";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("geometry<point>", format!("{}", out));
		assert_eq!(out, Kind::Geometry(vec![String::from("point")]));
	}

	#[test]
	fn kind_geometry_many() {
		let sql = "geometry<point | multipoint>";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("geometry<point | multipoint>", format!("{}", out));
		assert_eq!(out, Kind::Geometry(vec![String::from("point"), String::from("multipoint")]));
	}

	#[test]
	fn kind_option_one() {
		let sql = "option<int>";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("option<int>", format!("{}", out));
		assert_eq!(out, Kind::Option(Box::new(Kind::Int)));
	}

	#[test]
	fn kind_option_many() {
		let sql = "option<int | float>";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("option<int | float>", format!("{}", out));
		assert_eq!(out, Kind::Option(Box::new(Kind::Either(vec![Kind::Int, Kind::Float]))));
	}

	#[test]
	fn kind_array_any() {
		let sql = "array";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("array", format!("{}", out));
		assert_eq!(out, Kind::Array(Box::new(Kind::Any), None));
	}

	#[test]
	fn kind_array_some() {
		let sql = "array<float>";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("array<float>", format!("{}", out));
		assert_eq!(out, Kind::Array(Box::new(Kind::Float), None));
	}

	#[test]
	fn kind_array_some_size() {
		let sql = "array<float, 10>";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("array<float, 10>", format!("{}", out));
		assert_eq!(out, Kind::Array(Box::new(Kind::Float), Some(10)));
	}

	#[test]
	fn kind_set_any() {
		let sql = "set";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("set", format!("{}", out));
		assert_eq!(out, Kind::Set(Box::new(Kind::Any), None));
	}

	#[test]
	fn kind_set_some() {
		let sql = "set<float>";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("set<float>", format!("{}", out));
		assert_eq!(out, Kind::Set(Box::new(Kind::Float), None));
	}

	#[test]
	fn kind_set_some_size() {
		let sql = "set<float, 10>";
		let res = kind(sql);
		let out = res.unwrap().1;
		assert_eq!("set<float, 10>", format!("{}", out));
		assert_eq!(out, Kind::Set(Box::new(Kind::Float), Some(10)));
	}
}
