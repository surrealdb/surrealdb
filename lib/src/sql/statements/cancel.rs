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
#[revisioned(revision = 2)]
pub struct CancelStatement {
	#[revision(start = 2)]
	pub dryrun: bool,
}

impl fmt::Display for CancelStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "CANCEL TRANSACTION")?;
		if self.dryrun {
			write!(f, " AS DRYRUN")?
		};
		Ok(())
	}
}

pub fn cancel(i: &str) -> IResult<&str, CancelStatement> {
	let (i, _) = tag_no_case("CANCEL")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("TRANSACTION"))))(i)?;
	let (i, dryrun) = opt(dryrun)(i)?;
	Ok((
		i,
		CancelStatement {
			dryrun: dryrun.is_some_and(|d| d),
		},
	))
}

pub fn dryrun(i: &str) -> IResult<&str, bool> {
	let (i, _) =
		tuple((shouldbespace, tag_no_case("AS"), shouldbespace, tag_no_case("DRYRUN")))(i)?;
	Ok((i, true))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn cancel_basic() {
		let sql = "CANCEL";
		let res = cancel(sql);
		let out = res.unwrap().1;
		assert_eq!("CANCEL TRANSACTION", format!("{}", out))
	}

	#[test]
	fn cancel_query() {
		let sql = "CANCEL TRANSACTION";
		let res = cancel(sql);
		let out = res.unwrap().1;
		assert_eq!("CANCEL TRANSACTION", format!("{}", out))
	}

	#[test]
	fn cancel_dryrun_basic() {
		let sql = "CANCEL AS DRYRUN";
		let res = cancel(sql);
		let out = res.unwrap().1;
		assert_eq!("CANCEL TRANSACTION AS DRYRYN", format!("{}", out))
	}

	#[test]
	fn cancel_dryrun_query() {
		let sql = "CANCEL TRANSACTION AS DRYRUN";
		let res = cancel(sql);
		let out = res.unwrap().1;
		assert_eq!("CANCEL TRANSACTION AS DRYRYN", format!("{}", out))
	}
}
