use crate::sql::comment::shouldbespace;
use crate::sql::datetime::{datetime, Datetime};
use crate::sql::error::IResult;
use nom::bytes::complete::tag_no_case;
use nom::combinator::cut;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[revisioned(revision = 1)]
pub struct Version(pub Datetime);

impl fmt::Display for Version {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "VERSION {}", self.0)
	}
}

pub fn version(i: &str) -> IResult<&str, Version> {
	let (i, _) = tag_no_case("VERSION")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(datetime)(i)?;
	Ok((i, Version(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn version_statement() {
		let sql = "VERSION '2020-01-01T00:00:00Z'";
		let res = version(sql);
		let out = res.unwrap().1;
		assert_eq!(out, Version(Datetime::try_from("2020-01-01T00:00:00Z").unwrap()));
		assert_eq!("VERSION '2020-01-01T00:00:00Z'", format!("{}", out));
	}
}
