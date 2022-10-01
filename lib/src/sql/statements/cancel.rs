use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::tuple;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store)]
pub struct CancelStatement;

impl fmt::Display for CancelStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("CANCEL TRANSACTION")
	}
}

pub fn cancel(i: &str) -> IResult<&str, CancelStatement> {
	alt((cancel_query, cancel_basic))(i)
}

fn cancel_basic(i: &str) -> IResult<&str, CancelStatement> {
	let (i, _) = tag_no_case("CANCEL")(i)?;
	Ok((i, CancelStatement))
}

fn cancel_query(i: &str) -> IResult<&str, CancelStatement> {
	let (i, _) = tag_no_case("CANCEL")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("TRANSACTION"))))(i)?;
	Ok((i, CancelStatement))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn cancel_basic() {
		let sql = "CANCEL";
		let res = cancel(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("CANCEL TRANSACTION", format!("{}", out))
	}

	#[test]
	fn cancel_query() {
		let sql = "CANCEL TRANSACTION";
		let res = cancel(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("CANCEL TRANSACTION", format!("{}", out))
	}
}
