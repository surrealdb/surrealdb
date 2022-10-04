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
pub struct CommitStatement;

impl fmt::Display for CommitStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("COMMIT TRANSACTION")
	}
}

pub fn commit(i: &str) -> IResult<&str, CommitStatement> {
	alt((commit_query, commit_basic))(i)
}

fn commit_basic(i: &str) -> IResult<&str, CommitStatement> {
	let (i, _) = tag_no_case("COMMIT")(i)?;
	Ok((i, CommitStatement))
}

fn commit_query(i: &str) -> IResult<&str, CommitStatement> {
	let (i, _) = tag_no_case("COMMIT")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("TRANSACTION"))))(i)?;
	Ok((i, CommitStatement))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn commit_basic() {
		let sql = "COMMIT";
		let res = commit(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("COMMIT TRANSACTION", format!("{}", out))
	}

	#[test]
	fn commit_query() {
		let sql = "COMMIT TRANSACTION";
		let res = commit(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("COMMIT TRANSACTION", format!("{}", out))
	}
}
