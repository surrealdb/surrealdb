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
		fetch, fields, output,
	},
	thing::thing,
	value::{value, values, whats},
	IResult,
};
use crate::sql::{statements::LiveStatement, Fields, Value};
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

pub fn live(i: &str) -> IResult<&str, LiveStatement> {
	let (i, _) = tag_no_case("LIVE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("SELECT")(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(|i| {
		let (i, expr) = alt((map(tag_no_case("DIFF"), |_| Fields::default()), fields))(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, _) = expect_tag_no_case("FROM")(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, what) = alt((into(param), into(table)))(i)?;
		let (i, cond) = opt(preceded(shouldbespace, cond))(i)?;
		let (i, fetch) = opt(preceded(shouldbespace, fetch))(i)?;
		Ok((i, LiveStatement::from_source_parts(expr, what, cond, fetch)))
	})(i)
}
