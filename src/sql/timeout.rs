use crate::sql::comment::shouldbespace;
use crate::sql::duration::{duration, Duration};
use nom::bytes::complete::tag_no_case;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Timeout {
	pub expr: Duration,
}

impl fmt::Display for Timeout {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "TIMEOUT {}", self.expr)
	}
}

pub fn timeout(i: &str) -> IResult<&str, Timeout> {
	let (i, _) = tag_no_case("TIMEOUT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = duration(i)?;
	Ok((
		i,
		Timeout {
			expr: v,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn timeout_statement() {
		let sql = "TIMEOUT 5s";
		let res = timeout(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Timeout {
				expr: Duration::from("5s")
			}
		);
		assert_eq!("TIMEOUT 5s", format!("{}", out));
	}
}
