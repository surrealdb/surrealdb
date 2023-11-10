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
use crate::sql::statements::CancelStatement;
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

pub fn cancel(i: &str) -> IResult<&str, CancelStatement> {
	let (i, _) = tag_no_case("CANCEL")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("TRANSACTION"))))(i)?;
	Ok((i, CancelStatement))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn cancel_basic() {
		let sql = "CANCEL";
		let res = cancel(sql);
		let out = res.unwrap().1;
		assert_eq!("CANCEL TRANSACTION", format!("{}", out))
	}

	#[test]
	fn cancel_query() {
		let sql = "CANCEL TRANSACTION";
		let res = cancel(sql);
		let out = res.unwrap().1;
		assert_eq!("CANCEL TRANSACTION", format!("{}", out))
	}
}
