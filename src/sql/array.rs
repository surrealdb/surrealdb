use crate::sql::comment::mightbespace;
use crate::sql::common::commas;
use crate::sql::expression::{expression, Expression};
use nom::bytes::complete::tag;
use nom::combinator::opt;
use nom::multi::separated_list;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Array(Vec<Expression>);

impl fmt::Display for Array {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"[{}]",
			self.0
				.iter()
				.map(|ref v| format!("{}", v))
				.collect::<Vec<_>>()
				.join(", ")
		)
	}
}

pub fn array(i: &str) -> IResult<&str, Array> {
	let (i, _) = tag("[")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = separated_list(commas, expression)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = opt(tag(","))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag("]")(i)?;
	Ok((i, Array(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn array_normal() {
		let sql = "[1,2,3]";
		let res = array(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[1, 2, 3]", format!("{}", out));
		assert_eq!(out.0.len(), 3);
	}

	#[test]
	fn array_commas() {
		let sql = "[1,2,3,]";
		let res = array(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[1, 2, 3]", format!("{}", out));
		assert_eq!(out.0.len(), 3);
	}

	#[test]
	fn array_expression() {
		let sql = "[1,2,3+1]";
		let res = array(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[1, 2, 3 + 1]", format!("{}", out));
		assert_eq!(out.0.len(), 3);
	}
}
