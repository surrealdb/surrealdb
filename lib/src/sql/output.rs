use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::field::{fields, Fields};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::{cut, map, value};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[revisioned(revision = 1)]
pub enum Output {
	None,
	Null,
	Diff,
	After,
	Before,
	Fields(Fields),
}

impl Default for Output {
	fn default() -> Self {
		Self::None
	}
}

impl Display for Output {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("RETURN ")?;
		match self {
			Self::None => f.write_str("NONE"),
			Self::Null => f.write_str("NULL"),
			Self::Diff => f.write_str("DIFF"),
			Self::After => f.write_str("AFTER"),
			Self::Before => f.write_str("BEFORE"),
			Self::Fields(v) => Display::fmt(v, f),
		}
	}
}

pub fn output(i: &str) -> IResult<&str, Output> {
	let (i, _) = tag_no_case("RETURN")(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(|i| {
		let (i, v) = alt((
			value(Output::None, tag_no_case("NONE")),
			value(Output::Null, tag_no_case("NULL")),
			value(Output::Diff, tag_no_case("DIFF")),
			value(Output::After, tag_no_case("AFTER")),
			value(Output::Before, tag_no_case("BEFORE")),
			map(fields, Output::Fields),
		))(i)?;
		Ok((i, v))
	})(i)
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn output_statement() {
		let sql = "RETURN field, other.field";
		let res = output(sql);
		let out = res.unwrap().1;
		assert_eq!("RETURN field, other.field", format!("{}", out));
	}
}
