use crate::sql::comment::shouldbespace;
use crate::sql::expression::{expression, Expression};
use crate::sql::output::{output, Output};
use crate::sql::table::{table, Table};
use crate::sql::timeout::{timeout, Timeout};
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::preceded;
use nom::sequence::tuple;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct InsertStatement {
	pub data: Expression,
	pub into: Table,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub output: Option<Output>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub timeout: Option<Timeout>,
}

impl fmt::Display for InsertStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "INSERT {} INTO {}", self.data, self.into)?;
		if let Some(ref v) = self.output {
			write!(f, " {}", v)?
		}
		if let Some(ref v) = self.timeout {
			write!(f, " {}", v)?
		}
		Ok(())
	}
}

pub fn insert(i: &str) -> IResult<&str, InsertStatement> {
	let (i, _) = tag_no_case("INSERT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, data) = expression(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("INTO")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("TABLE"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, into) = table(i)?;
	let (i, output) = opt(preceded(shouldbespace, output))(i)?;
	let (i, timeout) = opt(preceded(shouldbespace, timeout))(i)?;
	Ok((
		i,
		InsertStatement {
			data,
			into,
			output,
			timeout,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn insert_statement() {
		let sql = "INSERT [1,2,3] INTO test";
		let res = insert(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("INSERT [1, 2, 3] INTO test", format!("{}", out))
	}
}
