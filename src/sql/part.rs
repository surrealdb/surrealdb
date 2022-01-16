use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::graph::{graph as graph_raw, Graph};
use crate::sql::ident::{ident, Ident};
use crate::sql::number::{number, Number};
use crate::sql::value::{value, Value};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::bytes::complete::tag_no_case;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Part {
	All,
	Last,
	First,
	Field(Ident),
	Index(Number),
	Where(Value),
	Graph(Graph),
}

impl From<Number> for Part {
	fn from(v: Number) -> Self {
		Part::Index(v)
	}
}

impl From<Ident> for Part {
	fn from(v: Ident) -> Self {
		Part::Field(v)
	}
}

impl From<Value> for Part {
	fn from(v: Value) -> Self {
		Part::Where(v)
	}
}

impl From<Graph> for Part {
	fn from(v: Graph) -> Self {
		Part::Graph(v)
	}
}

impl From<String> for Part {
	fn from(v: String) -> Self {
		Part::Field(Ident::from(v))
	}
}

impl From<&str> for Part {
	fn from(v: &str) -> Self {
		Part::Field(Ident::from(v.to_string()))
	}
}

impl fmt::Display for Part {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Part::All => write!(f, "[*]"),
			Part::Last => write!(f, "[$]"),
			Part::First => write!(f, "[0]"),
			Part::Field(v) => write!(f, ".{}", v),
			Part::Index(v) => write!(f, "[{}]", v),
			Part::Where(v) => write!(f, "[WHERE {}]", v),
			Part::Graph(v) => write!(f, "{}", v),
		}
	}
}

pub fn part(i: &str) -> IResult<&str, Part> {
	alt((all, last, index, field, graph, filter))(i)
}

pub fn first(i: &str) -> IResult<&str, Part> {
	let (i, v) = ident(i)?;
	Ok((i, Part::Field(v)))
}

pub fn all(i: &str) -> IResult<&str, Part> {
	let (i, _) = tag("[")(i)?;
	let (i, _) = tag("*")(i)?;
	let (i, _) = tag("]")(i)?;
	Ok((i, Part::All))
}

pub fn last(i: &str) -> IResult<&str, Part> {
	let (i, _) = tag("[")(i)?;
	let (i, _) = tag("$")(i)?;
	let (i, _) = tag("]")(i)?;
	Ok((i, Part::Last))
}

pub fn index(i: &str) -> IResult<&str, Part> {
	let (i, _) = tag("[")(i)?;
	let (i, v) = number(i)?;
	let (i, _) = tag("]")(i)?;
	Ok((i, Part::Index(v)))
}

pub fn field(i: &str) -> IResult<&str, Part> {
	let (i, _) = tag(".")(i)?;
	let (i, v) = ident(i)?;
	Ok((i, Part::Field(v)))
}

pub fn filter(i: &str) -> IResult<&str, Part> {
	let (i, _) = tag("[")(i)?;
	let (i, _) = alt((tag_no_case("WHERE"), tag("?")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = value(i)?;
	let (i, _) = tag("]")(i)?;
	Ok((i, Part::Where(v)))
}

pub fn graph(i: &str) -> IResult<&str, Part> {
	let (i, v) = graph_raw(i)?;
	Ok((i, Part::Graph(v)))
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::expression::Expression;
	use crate::sql::test::Parse;

	#[test]
	fn part_all() {
		let sql = "[*]";
		let res = part(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[*]", format!("{}", out));
		assert_eq!(out, Part::All);
	}

	#[test]
	fn part_last() {
		let sql = "[$]";
		let res = part(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[$]", format!("{}", out));
		assert_eq!(out, Part::Last);
	}

	#[test]
	fn part_number() {
		let sql = "[0]";
		let res = part(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[0]", format!("{}", out));
		assert_eq!(out, Part::Index(Number::from("0")));
	}

	#[test]
	fn part_expression_question() {
		let sql = "[? test = true]";
		let res = part(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[WHERE test = true]", format!("{}", out));
		assert_eq!(out, Part::Where(Value::from(Expression::parse("test = true"))));
	}

	#[test]
	fn part_expression_condition() {
		let sql = "[WHERE test = true]";
		let res = part(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[WHERE test = true]", format!("{}", out));
		assert_eq!(out, Part::Where(Value::from(Expression::parse("test = true"))));
	}
}
