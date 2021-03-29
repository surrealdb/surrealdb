use crate::sql::comment::shouldbespace;
use crate::sql::expression::{expression, Expression};
use nom::bytes::complete::tag_no_case;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Cond {
	pub expr: Expression,
}

impl fmt::Display for Cond {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "WHERE {}", self.expr)
	}
}

pub fn cond(i: &str) -> IResult<&str, Cond> {
	let (i, _) = tag_no_case("WHERE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = expression(i)?;
	Ok((
		i,
		Cond {
			expr: v,
		},
	))
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
