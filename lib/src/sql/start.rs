use crate::sql::comment::shouldbespace;
use crate::sql::common::take_usize;
use crate::sql::error::IResult;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::tuple;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Start(pub usize);

impl fmt::Display for Start {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "START {}", self.0)
	}
}

pub fn start(i: &str) -> IResult<&str, Start> {
	let (i, _) = tag_no_case("START")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("AT"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = take_usize(i)?;
	Ok((i, Start(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn start_statement() {
		let sql = "START 100";
		let res = start(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, Start(100));
		assert_eq!("START 100", format!("{}", out));
	}

	#[test]
	fn start_statement_at() {
		let sql = "START AT 100";
		let res = start(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, Start(100));
		assert_eq!("START 100", format!("{}", out));
	}
}
