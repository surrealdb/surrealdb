use super::super::{
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, commas, commasorspace, openparentheses},
	error::{expect_tag_no_case, expected},
	idiom::{basic, plain},
	literal::{datetime, duration, ident, scoring, table, tables, timeout},
	operator::{assigner, dir},
	part::{cond, data, output},
	thing::thing,
	value::{value, whats},
	IResult,
};
use crate::sql::statements::{BreakStatement, ContinueStatement};
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

pub fn r#break(i: &str) -> IResult<&str, BreakStatement> {
	let (i, _) = tag_no_case("BREAK")(i)?;
	Ok((i, BreakStatement))
}

pub fn r#continue(i: &str) -> IResult<&str, ContinueStatement> {
	let (i, _) = tag_no_case("CONTINUE")(i)?;
	Ok((i, ContinueStatement))
}
#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn break_basic() {
		let sql = "BREAK";
		let res = r#break(sql);
		let out = res.unwrap().1;
		assert_eq!("BREAK", format!("{}", out))
	}

	#[test]
	fn continue_basic() {
		let sql = "CONTINUE";
		let res = r#continue(sql);
		let out = res.unwrap().1;
		assert_eq!("CONTINUE", format!("{}", out))
	}
}
