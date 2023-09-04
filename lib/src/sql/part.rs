use crate::sql::comment::shouldbespace;
use crate::sql::ending::ident as ending;
use crate::sql::error::IResult;
use crate::sql::fmt::Fmt;
use crate::sql::graph::{self, Graph};
use crate::sql::ident::{self, Ident};
use crate::sql::idiom::{self, Idiom};
use crate::sql::number::{number, Number};
use crate::sql::param::{self};
use crate::sql::strand::{self, no_nul_bytes};
use crate::sql::value::{self, Value};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::bytes::complete::tag_no_case;
use nom::combinator::{self, cut, map, not, peek};
use nom::sequence::{preceded, terminated};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

use super::comment::mightbespace;
use super::common::{closebracket, openbracket};
use super::error::{expected, ExplainResultExt};
use super::util::expect_delimited;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[revisioned(revision = 1)]
pub enum Part {
	All,
	Flatten,
	Last,
	First,
	Field(Ident),
	Index(Number),
	Where(Value),
	Graph(Graph),
	Value(Value),
	Start(Value),
	Method(#[serde(with = "no_nul_bytes")] String, Vec<Value>),
}

impl From<i32> for Part {
	fn from(v: i32) -> Self {
		Self::Index(v.into())
	}
}

impl From<isize> for Part {
	fn from(v: isize) -> Self {
		Self::Index(v.into())
	}
}

impl From<usize> for Part {
	fn from(v: usize) -> Self {
		Self::Index(v.into())
	}
}

impl From<String> for Part {
	fn from(v: String) -> Self {
		Self::Field(v.into())
	}
}

impl From<Number> for Part {
	fn from(v: Number) -> Self {
		Self::Index(v)
	}
}

impl From<Ident> for Part {
	fn from(v: Ident) -> Self {
		Self::Field(v)
	}
}

impl From<Graph> for Part {
	fn from(v: Graph) -> Self {
		Self::Graph(v)
	}
}

impl From<&str> for Part {
	fn from(v: &str) -> Self {
		match v.parse::<isize>() {
			Ok(v) => Self::from(v),
			_ => Self::from(v.to_owned()),
		}
	}
}

impl Part {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		match self {
			Part::Start(v) => v.writeable(),
			Part::Where(v) => v.writeable(),
			Part::Value(v) => v.writeable(),
			Part::Method(_, v) => v.iter().any(Value::writeable),
			_ => false,
		}
	}
	/// Returns a yield if an alias is specified
	pub(crate) fn alias(&self) -> Option<&Idiom> {
		match self {
			Part::Graph(v) => v.alias.as_ref(),
			_ => None,
		}
	}
}

impl fmt::Display for Part {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Part::All => f.write_str("[*]"),
			Part::Last => f.write_str("[$]"),
			Part::First => f.write_str("[0]"),
			Part::Start(v) => write!(f, "{v}"),
			Part::Field(v) => write!(f, ".{v}"),
			Part::Flatten => f.write_str("…"),
			Part::Index(v) => write!(f, "[{v}]"),
			Part::Where(v) => write!(f, "[WHERE {v}]"),
			Part::Graph(v) => write!(f, "{v}"),
			Part::Value(v) => write!(f, "[{v}]"),
			Part::Method(v, a) => write!(f, ".{v}({})", Fmt::comma_separated(a)),
		}
	}
}

// ------------------------------

pub trait Next<'a> {
	fn next(&'a self) -> &[Part];
}

impl<'a> Next<'a> for &'a [Part] {
	fn next(&'a self) -> &'a [Part] {
		match self.len() {
			0 => &[],
			_ => &self[1..],
		}
	}
}

// ------------------------------

pub fn part(i: &str) -> IResult<&str, Part> {
	alt((
		flatten,
		preceded(tag("."), cut(dot_part)),
		expect_delimited(openbracket, cut(bracketed_part), closebracket),
		graph,
	))(i)
}

pub fn graph(i: &str) -> IResult<&str, Part> {
	map(graph::graph, Part::Graph)(i)
}

pub fn flatten(i: &str) -> IResult<&str, Part> {
	combinator::value(Part::Flatten, alt((tag("..."), tag("…"))))(i)
}

pub fn local_part(i: &str) -> IResult<&str, Part> {
	// Cant cut dot part since it might be part of the flatten at the end.
	alt((
		preceded(tag("."), dot_part),
		expect_delimited(openbracket, cut(local_bracketed_part), closebracket),
		// TODO explain
	))(i)
}

pub fn basic_part(i: &str) -> IResult<&str, Part> {
	alt((
		preceded(
			tag("."),
			cut(|i| dot_part(i).explain("flattening is not allowed with a basic idiom", tag(".."))),
		),
		|s| {
			let (i, _) = openbracket(s)?;
			let (i, v) = expected(
				"$, * or a number",
				cut(terminated(basic_bracketed_part, closebracket)),
			)(i)
			.explain("basic idioms don't allow computed values", bracketed_value)
			.explain("basic idioms don't allow where selectors", bracketed_where)?;
			Ok((i, v))
		},
	))(i)
}

fn dot_part(i: &str) -> IResult<&str, Part> {
	alt((
		combinator::value(Part::All, tag("*")),
		map(terminated(ident::ident, ending), Part::Field),
	))(i)
}

fn basic_bracketed_part(i: &str) -> IResult<&str, Part> {
	alt((
		combinator::value(Part::All, tag("*")),
		combinator::value(Part::Last, tag("$")),
		map(number, Part::Index),
	))(i)
}

fn local_bracketed_part(i: &str) -> IResult<&str, Part> {
	alt((combinator::value(Part::All, tag("*")), map(number, Part::Index)))(i)
		.explain("using `[$]` in a local idiom is not allowed", tag("$"))
}

fn bracketed_part(i: &str) -> IResult<&str, Part> {
	alt((
		combinator::value(Part::All, tag("*")),
		combinator::value(Part::Last, terminated(tag("$"), peek(closebracket))),
		map(number, Part::Index),
		bracketed_where,
		bracketed_value,
	))(i)
}

pub fn first(i: &str) -> IResult<&str, Part> {
	let (i, _) = peek(not(number))(i)?;
	let (i, v) = ident::ident(i)?;
	let (i, _) = ending(i)?;
	Ok((i, Part::Field(v)))
}

pub fn bracketed_where(i: &str) -> IResult<&str, Part> {
	let (i, _) = alt((
		terminated(tag("?"), mightbespace),
		terminated(tag_no_case("WHERE"), shouldbespace),
	))(i)?;

	let (i, v) = value::value(i)?;
	Ok((i, Part::Where(v)))
}

pub fn bracketed_value(i: &str) -> IResult<&str, Part> {
	let (i, v) = alt((
		map(strand::strand, Value::Strand),
		map(param::param, Value::Param),
		map(idiom::basic, Value::Idiom),
	))(i)?;
	Ok((i, Part::Value(v)))
}

#[cfg(test)]
mod tests {

	use param::Param;

	use super::*;
	use crate::sql::expression::Expression;
	use crate::sql::test::Parse;

	#[test]
	fn part_all() {
		let sql = "[*]";
		let res = part(sql);
		let out = res.unwrap().1;
		assert_eq!("[*]", format!("{}", out));
		assert_eq!(out, Part::All);
	}

	#[test]
	fn part_last() {
		let sql = "[$]";
		let res = part(sql);
		let out = res.unwrap().1;
		assert_eq!("[$]", format!("{}", out));
		assert_eq!(out, Part::Last);
	}

	#[test]
	fn part_param() {
		let sql = "[$param]";
		let res = part(sql);
		let out = res.unwrap().1;
		assert_eq!("[$param]", format!("{}", out));
		assert_eq!(out, Part::Value(Value::Param(Param::from("param"))));
	}

	#[test]
	fn part_flatten() {
		let sql = "...";
		let res = part(sql);
		let out = res.unwrap().1;
		assert_eq!("…", format!("{}", out));
		assert_eq!(out, Part::Flatten);
	}

	#[test]
	fn part_flatten_ellipsis() {
		let sql = "…";
		let res = part(sql);
		let out = res.unwrap().1;
		assert_eq!("…", format!("{}", out));
		assert_eq!(out, Part::Flatten);
	}

	#[test]
	fn part_number() {
		let sql = "[0]";
		let res = part(sql);
		let out = res.unwrap().1;
		assert_eq!("[0]", format!("{}", out));
		assert_eq!(out, Part::Index(Number::from(0)));
	}

	#[test]
	fn part_expression_question() {
		let sql = "[?test = true]";
		let res = part(sql);
		let out = res.unwrap().1;
		assert_eq!("[WHERE test = true]", format!("{}", out));
		assert_eq!(out, Part::Where(Value::from(Expression::parse("test = true"))));
	}

	#[test]
	fn part_expression_condition() {
		let sql = "[WHERE test = true]";
		let res = part(sql);
		let out = res.unwrap().1;
		assert_eq!("[WHERE test = true]", format!("{}", out));
		assert_eq!(out, Part::Where(Value::from(Expression::parse("test = true"))));
	}
}
