use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use derive::Store;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::tuple;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct BeginStatement;

impl fmt::Display for BeginStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("BEGIN TRANSACTION")
	}
}

pub fn begin(i: &str) -> IResult<&str, BeginStatement> {
	let (i, _) = tag_no_case("BEGIN")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("TRANSACTION"))))(i)?;
	Ok((i, BeginStatement))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn begin_basic() {
		let sql = "BEGIN";
		let res = begin(sql);
		let out = res.unwrap().1;
		assert_eq!("BEGIN TRANSACTION", format!("{}", out))
	}

	#[test]
	fn begin_query() {
		let sql = "BEGIN TRANSACTION";
		let res = begin(sql);
		let out = res.unwrap().1;
		assert_eq!("BEGIN TRANSACTION", format!("{}", out))
	}
}
