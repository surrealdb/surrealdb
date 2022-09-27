use crate::sql::comment::mightbespace;
use crate::sql::common::commas;
use crate::sql::error::IResult;
use crate::sql::table::{table, Table};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::char;
use nom::combinator::map;
use nom::multi::separated_list1;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Kind {
	Any,
	Array,
	Bool,
	Datetime,
	Decimal,
	Duration,
	Float,
	Int,
	Number,
	Object,
	String,
	Record(Vec<Table>),
	Geometry(Vec<String>),
}

impl Default for Kind {
	fn default() -> Kind {
		Kind::Any
	}
}

impl fmt::Display for Kind {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Kind::Any => f.write_str("any"),
			Kind::Array => f.write_str("array"),
			Kind::Bool => f.write_str("bool"),
			Kind::Datetime => f.write_str("datetime"),
			Kind::Decimal => f.write_str("decimal"),
			Kind::Duration => f.write_str("duration"),
			Kind::Float => f.write_str("float"),
			Kind::Int => f.write_str("int"),
			Kind::Number => f.write_str("number"),
			Kind::Object => f.write_str("object"),
			Kind::String => f.write_str("string"),
			Kind::Record(v) => write!(
				f,
				"record({})",
				v.iter().map(|ref v| v.to_string()).collect::<Vec<_>>().join(", ")
			),
			Kind::Geometry(v) => write!(
				f,
				"geometry({})",
				v.iter().map(|ref v| v.to_string()).collect::<Vec<_>>().join(", ")
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
		map(tag("decimal"), |_| Kind::Decimal),
		map(tag("duration"), |_| Kind::Duration),
		map(tag("float"), |_| Kind::Float),
		map(tag("int"), |_| Kind::Int),
		map(tag("number"), |_| Kind::Number),
		map(tag("object"), |_| Kind::Object),
		map(tag("string"), |_| Kind::String),
		map(geometry, Kind::Geometry),
		map(record, Kind::Record),
	))(i)
}

fn record(i: &str) -> IResult<&str, Vec<Table>> {
	let (i, _) = tag("record")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char('(')(i)?;
	let (i, v) = separated_list1(commas, table)(i)?;
	let (i, _) = char(')')(i)?;
	Ok((i, v))
}

fn geometry(i: &str) -> IResult<&str, Vec<String>> {
	let (i, _) = tag("geometry")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char('(')(i)?;
	let (i, v) = separated_list1(
		commas,
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
		),
	)(i)?;
	let (i, _) = char(')')(i)?;
	Ok((i, v))
}
