use crate::sql::comment::shouldbespace;
use crate::sql::duration::{duration, Duration};
use crate::sql::error::IResult;
use nom::bytes::complete::tag_no_case;
use nom::combinator::cut;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[revisioned(revision = 1)]
pub struct Timeout(pub Duration);

impl Deref for Timeout {
	type Target = Duration;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl fmt::Display for Timeout {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "TIMEOUT {}", self.0)
	}
}

pub fn timeout(i: &str) -> IResult<&str, Timeout> {
	let (i, _) = tag_no_case("TIMEOUT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(duration)(i)?;
	Ok((i, Timeout(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn timeout_statement() {
		let sql = "TIMEOUT 5s";
		let res = timeout(sql);
		let out = res.unwrap().1;
		assert_eq!("TIMEOUT 5s", format!("{}", out));
		assert_eq!(out, Timeout(Duration::try_from("5s").unwrap()));
	}
}
