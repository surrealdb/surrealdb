use crate::sql::comment::shouldbespace;
use crate::sql::expression::{expression, Expression};
use nom::bytes::complete::tag_no_case;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct OutputStatement {
	pub what: Expression,
}

impl fmt::Display for OutputStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "RETURN {}", self.what)
	}
}

pub fn output(i: &str) -> IResult<&str, OutputStatement> {
	let (i, _) = tag_no_case("RETURN")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = expression(i)?;
	Ok((i, OutputStatement { what: v }))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn output_statement() {
		let sql = "RETURN $param";
		let res = output(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("RETURN $param", format!("{}", out));
	}
}
