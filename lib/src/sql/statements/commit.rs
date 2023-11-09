use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct CommitStatement;

impl fmt::Display for CommitStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("COMMIT TRANSACTION")
	}
}

pub fn commit(i: &str) -> IResult<&str, CommitStatement> {
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
		let out = res.unwrap().1;
		assert_eq!("COMMIT TRANSACTION", format!("{}", out))
	}

	#[test]
	fn commit_query() {
		let sql = "COMMIT TRANSACTION";
		let res = commit(sql);
		let out = res.unwrap().1;
		assert_eq!("COMMIT TRANSACTION", format!("{}", out))
	}
}
