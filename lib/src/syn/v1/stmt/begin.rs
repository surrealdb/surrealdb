use super::super::{
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, commas, commasorspace, openparentheses},
	error::{expect_tag_no_case, expected},
	idiom::{basic, plain},
	literal::{datetime, duration, ident, scoring, table, tables},
	operator::{assigner, dir},
	thing::thing,
	// TODO: go through and check every import for alias.
	value::value,
	IResult,
};
use crate::sql::statements::BeginStatement;
use nom::{
	branch::alt,
	bytes::complete::{escaped, escaped_transform, is_not, tag, tag_no_case, take, take_while_m_n},
	character::complete::{anychar, char, u16, u32},
	combinator::{cut, into, map, map_res, opt, recognize, value as map_value},
	multi::separated_list1,
	number::complete::recognize_float,
	sequence::{delimited, preceded, terminated, tuple},
	Err,
};

pub fn begin(i: &str) -> IResult<&str, BeginStatement> {
	let (i, _) = tag_no_case("BEGIN")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("TRANSACTION"))))(i)?;
	Ok((i, BeginStatement))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn begin_basic() {
		let sql = "BEGIN";
		let res = begin(sql);
		let out = res.unwrap().1;
		assert_eq!("BEGIN TRANSACTION", format!("{}", out))
	}

	#[test]
	fn begin_query() {
		let sql = "BEGIN TRANSACTION";
		let res = begin(sql);
		let out = res.unwrap().1;
		assert_eq!("BEGIN TRANSACTION", format!("{}", out))
	}
}
