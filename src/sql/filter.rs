use crate::sql::comment::shouldbespace;
use crate::sql::expression::{expression, Expression};
use crate::sql::number::{number, Number};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::bytes::complete::tag_no_case;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Filter {
	All,
	Last,
	Number(Number),
	Expression(Expression),
}

impl From<Number> for Filter {
	fn from(v: Number) -> Self {
		Filter::Number(v)
	}
}

impl From<Expression> for Filter {
	fn from(v: Expression) -> Self {
		Filter::Expression(v)
	}
}

impl<'a> From<&'a str> for Filter {
	fn from(s: &str) -> Self {
		filter(s).unwrap().1
	}
}

impl fmt::Display for Filter {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Filter::All => write!(f, "*"),
			Filter::Last => write!(f, "$"),
			Filter::Number(v) => write!(f, "{}", v),
			Filter::Expression(v) => write!(f, "WHERE {}", v),
		}
	}
}

pub fn filter(i: &str) -> IResult<&str, Filter> {
	alt((filter_all, filter_last, filter_number, filter_expression))(i)
}

fn filter_all(i: &str) -> IResult<&str, Filter> {
	let (i, _) = tag("*")(i)?;
	Ok((i, Filter::All))
}

fn filter_last(i: &str) -> IResult<&str, Filter> {
	let (i, _) = tag("$")(i)?;
	Ok((i, Filter::Last))
}

fn filter_number(i: &str) -> IResult<&str, Filter> {
	let (i, v) = number(i)?;
	Ok((i, Filter::Number(v)))
}

fn filter_expression(i: &str) -> IResult<&str, Filter> {
	let (i, _) = alt((tag_no_case("WHERE"), tag("?")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = expression(i)?;
	Ok((i, Filter::Expression(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn filter_all() {
		let sql = "*";
		let res = filter(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("*", format!("{}", out));
		assert_eq!(out, Filter::All);
	}

	#[test]
	fn filter_last() {
		let sql = "$";
		let res = filter(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("$", format!("{}", out));
		assert_eq!(out, Filter::Last);
	}

	#[test]
	fn filter_number() {
		let sql = "0";
		let res = filter(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("0", format!("{}", out));
		assert_eq!(out, Filter::Number(Number::from("0")));
	}

	#[test]
	fn filter_expression_question() {
		let sql = "? test = true";
		let res = filter(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("WHERE test = true", format!("{}", out));
		assert_eq!(out, Filter::Expression(Expression::from("test = true")));
	}

	#[test]
	fn filter_expression_condition() {
		let sql = "WHERE test = true";
		let res = filter(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("WHERE test = true", format!("{}", out));
		assert_eq!(out, Filter::Expression(Expression::from("test = true")));
	}
}
