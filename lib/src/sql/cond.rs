use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::value::{value, Value};
use nom::bytes::complete::tag_no_case;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub struct Cond(pub Value);

impl Deref for Cond {
	type Target = Value;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl fmt::Display for Cond {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "WHERE {}", self.0)
	}
}

pub fn cond(i: &str) -> IResult<&str, Cond> {
	let (i, _) = tag_no_case("WHERE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = value(i)?;
	Ok((i, Cond(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn cond_statement() {
		let sql = "WHERE field = true";
		let res = cond(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("WHERE field = true", format!("{}", out));
	}

	#[test]
	fn cond_statement_multiple() {
		let sql = "WHERE field = true AND other.field = false";
		let res = cond(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("WHERE field = true AND other.field = false", format!("{}", out));
	}
}
