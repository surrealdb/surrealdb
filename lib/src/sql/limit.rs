use crate::sql::comment::shouldbespace;
use crate::sql::common::take_usize;
use crate::sql::error::IResult;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::tuple;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Limit(pub usize);

impl fmt::Display for Limit {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "LIMIT {}", self.0)
	}
}

pub fn limit(i: &str) -> IResult<&str, Limit> {
	let (i, _) = tag_no_case("LIMIT")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("BY"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = take_usize(i)?;
	Ok((i, Limit(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn limit_statement() {
		let sql = "LIMIT 100";
		let res = limit(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, Limit(100));
		assert_eq!("LIMIT 100", format!("{}", out));
	}

	#[test]
	fn limit_statement_by() {
		let sql = "LIMIT BY 100";
		let res = limit(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, Limit(100));
		assert_eq!("LIMIT 100", format!("{}", out));
	}
}
