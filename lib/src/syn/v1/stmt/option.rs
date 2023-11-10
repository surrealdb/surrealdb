use super::super::{
	block::block,
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, commas, commasorspace, openparentheses},
	error::{expect_tag_no_case, expected, ExplainResultExt},
	idiom::{basic, plain},
	literal::{datetime, duration, ident, param, scoring, table, tables, timeout},
	operator::{assigner, dir},
	part::{
		cond, data,
		data::{single, update},
		output,
	},
	thing::thing,
	value::{value, values, whats},
	IResult,
};
use crate::sql::{statements::OptionStatement, Value};
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

pub fn option(i: &str) -> IResult<&str, OptionStatement> {
	let (i, _) = tag_no_case("OPTION")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, n) = ident(i)?;
	let (i, v) = expected(
		"'=' followed by a value for the option",
		cut(opt(alt((
			value(true, tuple((mightbespace, char('='), mightbespace, tag_no_case("TRUE")))),
			value(false, tuple((mightbespace, char('='), mightbespace, tag_no_case("FALSE")))),
		)))),
	)(i)?;
	Ok((
		i,
		OptionStatement {
			name: n,
			what: v.unwrap_or(true),
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn option_statement() {
		let sql = "OPTION IMPORT";
		let res = option(sql);
		let out = res.unwrap().1;
		assert_eq!("OPTION IMPORT", format!("{}", out));
	}

	#[test]
	fn option_statement_true() {
		let sql = "OPTION IMPORT = TRUE";
		let res = option(sql);
		let out = res.unwrap().1;
		assert_eq!("OPTION IMPORT", format!("{}", out));
	}

	#[test]
	fn option_statement_false() {
		let sql = "OPTION IMPORT = FALSE";
		let res = option(sql);
		let out = res.unwrap().1;
		assert_eq!("OPTION IMPORT = FALSE", format!("{}", out));
	}
}
