use crate::sql::error::IResult;
use crate::sql::table::{table, Table};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::char;
use nom::combinator::map;
use nom::multi::many1;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Kind {
	Any,
	Array,
	Bool,
	Datetime,
	Number,
	Object,
	String,
	Geometry(String),
	Record(Vec<Table>),
}

impl Default for Kind {
	fn default() -> Kind {
		Kind::Any
	}
}

impl fmt::Display for Kind {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Kind::Any => write!(f, "any"),
			Kind::Array => write!(f, "array"),
			Kind::Bool => write!(f, "bool"),
			Kind::Datetime => write!(f, "datetime"),
			Kind::Number => write!(f, "number"),
			Kind::Object => write!(f, "object"),
			Kind::String => write!(f, "string"),
			Kind::Geometry(v) => write!(f, "geometry({})", v),
			Kind::Record(v) => write!(
				f,
				"record({})",
				v.iter().map(|ref v| format!("{}", v)).collect::<Vec<_>>().join(", ")
			),
		}
	}
}

pub fn kind(i: &str) -> IResult<&str, Kind> {
	alt((
		map(tag("any"), |_| Kind::Any),
		map(tag("array"), |_| Kind::Array),
		map(tag("bool"), |_| Kind::Bool),
		map(tag("datetime"), |_| Kind::Datetime),
		map(tag("number"), |_| Kind::Number),
		map(tag("object"), |_| Kind::Object),
		map(tag("string"), |_| Kind::String),
		map(geometry, Kind::Geometry),
		map(record, Kind::Record),
	))(i)
}

fn geometry(i: &str) -> IResult<&str, String> {
	let (i, _) = tag("geometry")(i)?;
	let (i, _) = char('(')(i)?;
	let (i, v) = alt((
		tag("feature"),
		tag("point"),
		tag("line"),
		tag("polygon"),
		tag("multipoint"),
		tag("multiline"),
		tag("multipolygon"),
		tag("collection"),
	))(i)?;
	let (i, _) = char(')')(i)?;
	Ok((i, String::from(v)))
}

fn record(i: &str) -> IResult<&str, Vec<Table>> {
	let (i, _) = tag("record")(i)?;
	let (i, _) = char('(')(i)?;
	let (i, v) = many1(table)(i)?;
	let (i, _) = char(')')(i)?;
	Ok((i, v))
}
