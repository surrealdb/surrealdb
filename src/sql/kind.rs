use crate::sql::table::{table, Table};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::combinator::map;
use nom::multi::many1;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Kind {
	Any,
	Array,
	Bool,
	Circle,
	Datetime,
	Number,
	Object,
	Point,
	Polygon,
	String,
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
			Kind::Circle => write!(f, "circle"),
			Kind::Datetime => write!(f, "datetime"),
			Kind::Number => write!(f, "number"),
			Kind::Object => write!(f, "object"),
			Kind::Point => write!(f, "point"),
			Kind::Polygon => write!(f, "polygon"),
			Kind::String => write!(f, "string"),
			Kind::Record(t) => write!(
				f,
				"record({})",
				t.iter().map(|ref v| format!("{}", v)).collect::<Vec<_>>().join(", ")
			),
		}
	}
}

pub fn kind(i: &str) -> IResult<&str, Kind> {
	alt((
		map(tag("array"), |_| Kind::Array),
		map(tag("bool"), |_| Kind::Bool),
		map(tag("circle"), |_| Kind::Circle),
		map(tag("datetime"), |_| Kind::Datetime),
		map(tag("number"), |_| Kind::Number),
		map(tag("object"), |_| Kind::Object),
		map(tag("point"), |_| Kind::Point),
		map(tag("polygon"), |_| Kind::Polygon),
		map(tag("string"), |_| Kind::String),
		map(record, |v| Kind::Record(v)),
	))(i)
}

fn record(i: &str) -> IResult<&str, Vec<Table>> {
	let (i, _) = tag("record")(i)?;
	let (i, _) = tag("(")(i)?;
	let (i, v) = many1(table)(i)?;
	let (i, _) = tag(")")(i)?;
	Ok((i, v))
}
