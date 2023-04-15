use crate::sql::comment::mightbespace;
use crate::sql::common::commas;
use crate::sql::common::verbar;
use crate::sql::error::IResult;
use crate::sql::fmt::{fmt_separated_by, Fmt};
use crate::sql::table::{table, Table};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::char;
use nom::character::complete::u64;
use nom::combinator::map;
use nom::combinator::opt;
use nom::multi::{separated_list0, separated_list1};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum Kind {
	Any,
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

impl Display for Kind {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Kind::Any => f.write_str("any"),
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
				k => write!(f, "record({})", Fmt::comma_separated(k)),
			},
			Kind::Geometry(k) => match k {
				k if k.is_empty() => write!(f, "geometry"),
				k => write!(f, "geometry({})", Fmt::comma_separated(k)),
			},
			Kind::Set(k, l) => match (k, l) {
				(k, None) if k.is_any() => write!(f, "set"),
				(k, None) => write!(f, "set[{k}]"),
				(k, Some(l)) => write!(f, "set[{k}, {l}]"),
			},
			Kind::Array(k, l) => match (k, l) {
				(k, None) if k.is_any() => write!(f, "array"),
				(k, None) => write!(f, "array[{k}]"),
				(k, Some(l)) => write!(f, "array[{k}, {l}]"),
			},
			Kind::Either(k) => Display::fmt(&Fmt::new(k, fmt_separated_by(" | ")), f),
		}
	}
}

pub fn kind(i: &str) -> IResult<&str, Kind> {
	alt((
		map(tag("any"), |_| Kind::Any),
		map(tag("bool"), |_| Kind::Bool),
		map(tag("bytes"), |_| Kind::Bytes),
		map(tag("datetime"), |_| Kind::Datetime),
		map(tag("decimal"), |_| Kind::Decimal),
		map(tag("duration"), |_| Kind::Duration),
		map(tag("float"), |_| Kind::Float),
		map(tag("int"), |_| Kind::Int),
		map(tag("number"), |_| Kind::Number),
		map(tag("object"), |_| Kind::Object),
		map(tag("point"), |_| Kind::Point),
		map(tag("string"), |_| Kind::String),
		map(tag("uuid"), |_| Kind::Uuid),
		geometry,
		record,
		option,
		either,
		array,
		set,
	))(i)
}

fn set(i: &str) -> IResult<&str, Kind> {
	let (i, _) = tag("set")(i)?;
	let (i, v) = opt(|i| {
		let (i, _) = char('[')(i)?;
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
		let (i, _) = char(']')(i)?;
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

fn array(i: &str) -> IResult<&str, Kind> {
	let (i, _) = tag("array")(i)?;
	let (i, v) = opt(|i| {
		let (i, _) = char('[')(i)?;
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
		let (i, _) = char(']')(i)?;
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

fn either(i: &str) -> IResult<&str, Kind> {
	let (i, v) = separated_list1(
		verbar,
		alt((
			map(tag("bool"), |_| Kind::Bool),
			map(tag("datetime"), |_| Kind::Datetime),
			map(tag("decimal"), |_| Kind::Decimal),
			map(tag("duration"), |_| Kind::Duration),
			map(tag("float"), |_| Kind::Float),
			map(tag("int"), |_| Kind::Int),
			map(tag("number"), |_| Kind::Number),
			map(tag("object"), |_| Kind::Object),
			map(tag("point"), |_| Kind::Point),
			map(tag("string"), |_| Kind::String),
			map(tag("uuid"), |_| Kind::Uuid),
			geometry,
			record,
			array,
			set,
		)),
	)(i)?;
	Ok((i, Kind::Either(v)))
}

fn option(i: &str) -> IResult<&str, Kind> {
	let (i, _) = tag("option")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char('<')(i)?;
	let (i, v) = map(
		alt((
			map(tag("bool"), |_| Kind::Bool),
			map(tag("datetime"), |_| Kind::Datetime),
			map(tag("decimal"), |_| Kind::Decimal),
			map(tag("duration"), |_| Kind::Duration),
			map(tag("float"), |_| Kind::Float),
			map(tag("int"), |_| Kind::Int),
			map(tag("number"), |_| Kind::Number),
			map(tag("object"), |_| Kind::Object),
			map(tag("point"), |_| Kind::Point),
			map(tag("string"), |_| Kind::String),
			map(tag("uuid"), |_| Kind::Uuid),
			geometry,
			record,
			either,
			array,
			set,
		)),
		Box::new,
	)(i)?;
	let (i, _) = char('>')(i)?;
	Ok((i, Kind::Option(v)))
}

fn record(i: &str) -> IResult<&str, Kind> {
	let (i, _) = tag("record")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char('(')(i)?;
	let (i, v) = separated_list0(commas, table)(i)?;
	let (i, _) = char(')')(i)?;
	Ok((i, Kind::Record(v)))
}

fn geometry(i: &str) -> IResult<&str, Kind> {
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
	Ok((i, Kind::Geometry(v)))
}
