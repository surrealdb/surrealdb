use crate::sql::array::{array, Array};
use crate::sql::comment::mightbespace;
use crate::sql::comment::shouldbespace;
use crate::sql::common::commas;
use crate::sql::expression::{expression, Expression};
use crate::sql::idiom::{idiom, Idiom};
use crate::sql::object::{object, Object};
use crate::sql::operator::{assigner, Operator};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::multi::separated_list1;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Data {
	SetExpression(Vec<(Idiom, Operator, Expression)>),
	DiffExpression(Array),
	MergeExpression(Object),
	ContentExpression(Object),
}

impl fmt::Display for Data {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Data::SetExpression(v) => write!(
				f,
				"SET {}",
				v.iter()
					.map(|(l, o, r)| format!("{} {} {}", l, o, r))
					.collect::<Vec<_>>()
					.join(", ")
			),
			Data::DiffExpression(v) => write!(f, "DIFF {}", v),
			Data::MergeExpression(v) => write!(f, "MERGE {}", v),
			Data::ContentExpression(v) => write!(f, "CONTENT {}", v),
		}
	}
}

pub fn data(i: &str) -> IResult<&str, Data> {
	alt((set, diff, merge, content))(i)
}

fn set(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("SET")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = separated_list1(commas, |i| {
		let (i, l) = idiom(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, o) = assigner(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, r) = expression(i)?;
		Ok((i, (l, o, r)))
	})(i)?;
	Ok((i, Data::SetExpression(v)))
}

fn diff(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("DIFF")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = array(i)?;
	Ok((i, Data::DiffExpression(v)))
}

fn merge(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("MERGE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = object(i)?;
	Ok((i, Data::MergeExpression(v)))
}

fn content(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("CONTENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = object(i)?;
	Ok((i, Data::ContentExpression(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn set_statement() {
		let sql = "SET field = true";
		let res = data(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("SET field = true", format!("{}", out));
	}

	#[test]
	fn set_statement_multiple() {
		let sql = "SET field = true, other.field = false";
		let res = data(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("SET field = true, other.field = false", format!("{}", out));
	}

	#[test]
	fn diff_statement() {
		let sql = "DIFF [{ field: true }]";
		let res = data(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("DIFF [{ field: true }]", format!("{}", out));
	}

	#[test]
	fn merge_statement() {
		let sql = "MERGE { field: true }";
		let res = data(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("MERGE { field: true }", format!("{}", out));
	}

	#[test]
	fn content_statement() {
		let sql = "CONTENT { field: true }";
		let res = data(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("CONTENT { field: true }", format!("{}", out));
	}
}
