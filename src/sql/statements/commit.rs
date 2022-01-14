use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
use crate::sql::value::Value;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::tuple;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommitStatement;

impl CommitStatement {
	pub async fn compute(
		&self,
		_ctx: &Runtime,
		_opt: &Options<'_>,
		_exe: &mut Executor,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		Ok(Value::None)
	}
}

impl fmt::Display for CommitStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "COMMIT TRANSACTION")
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
	let (i, _) = shouldbespace(i)?;
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
