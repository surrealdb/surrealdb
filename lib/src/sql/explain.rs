use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::tuple;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub struct Explain(pub bool);

impl fmt::Display for Explain {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("EXPLAIN")?;
		if self.0 {
			f.write_str(" FULL")?;
		}
		Ok(())
	}
}

pub fn explain(i: &str) -> IResult<&str, Explain> {
	let (i, _) = tag_no_case("EXPLAIN")(i)?;
	let (i, full) = opt(tuple((shouldbespace, tag_no_case("FULL"))))(i)?;
	Ok((i, Explain(full.is_some())))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn explain_statement() {
		let sql = "EXPLAIN";
		let res = explain(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, Explain(false));
		assert_eq!("EXPLAIN", format!("{}", out));
	}

	#[test]
	fn explain_full_statement() {
		let sql = "EXPLAIN FULL";
		let res = explain(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, Explain(true));
		assert_eq!("EXPLAIN FULL", format!("{}", out));
	}
}
