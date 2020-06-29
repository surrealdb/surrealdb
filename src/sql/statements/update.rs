use crate::sql::comment::shouldbespace;
use crate::sql::cond::{cond, Cond};
use crate::sql::data::{data, Data};
use crate::sql::output::{output, Output};
use crate::sql::timeout::{timeout, Timeout};
use crate::sql::what::{whats, Whats};
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::preceded;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct UpdateStatement {
	pub what: Whats,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub data: Option<Data>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub cond: Option<Cond>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub output: Option<Output>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub timeout: Option<Timeout>,
}

impl fmt::Display for UpdateStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "UPDATE {}", self.what)?;
		if let Some(ref v) = self.data {
			write!(f, " {}", v)?
		}
		if let Some(ref v) = self.cond {
			write!(f, " {}", v)?
		}
		if let Some(ref v) = self.output {
			write!(f, " {}", v)?
		}
		if let Some(ref v) = self.timeout {
			write!(f, " {}", v)?
		}
		Ok(())
	}
}

pub fn update(i: &str) -> IResult<&str, UpdateStatement> {
	let (i, _) = tag_no_case("UPDATE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = whats(i)?;
	let (i, data) = opt(preceded(shouldbespace, data))(i)?;
	let (i, cond) = opt(preceded(shouldbespace, cond))(i)?;
	let (i, output) = opt(preceded(shouldbespace, output))(i)?;
	let (i, timeout) = opt(preceded(shouldbespace, timeout))(i)?;
	Ok((
		i,
		UpdateStatement {
			what,
			data,
			cond,
			output,
			timeout,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn update_statement() {
		let sql = "UPDATE test";
		let res = update(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("UPDATE test", format!("{}", out))
	}
}
