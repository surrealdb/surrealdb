use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::field::{fields, Fields};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Output {
	None,
	Null,
	Diff,
	After,
	Before,
	Fields(Fields),
}

impl Default for Output {
	fn default() -> Output {
		Output::None
	}
}

impl fmt::Display for Output {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("RETURN ")?;
		match self {
			Output::None => f.write_str("NONE"),
			Output::Null => f.write_str("NULL"),
			Output::Diff => f.write_str("DIFF"),
			Output::After => f.write_str("AFTER"),
			Output::Before => f.write_str("BEFORE"),
			Output::Fields(v) => write!(f, "{}", v),
		}
	}
}

pub fn output(i: &str) -> IResult<&str, Output> {
	let (i, _) = tag_no_case("RETURN")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = alt((
		map(tag_no_case("NONE"), |_| Output::None),
		map(tag_no_case("NULL"), |_| Output::Null),
		map(tag_no_case("DIFF"), |_| Output::Diff),
		map(tag_no_case("AFTER"), |_| Output::After),
		map(tag_no_case("BEFORE"), |_| Output::Before),
		map(fields, Output::Fields),
	))(i)?;
	Ok((i, v))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn output_statement() {
		let sql = "RETURN field, other.field";
		let res = output(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("RETURN field, other.field", format!("{}", out));
	}
}
