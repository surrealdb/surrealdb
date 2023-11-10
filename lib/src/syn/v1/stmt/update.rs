use super::super::{
	block::block,
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, commas, commasorspace, openparentheses},
	error::{expect_tag_no_case, expected, ExplainResultExt},
	idiom::{basic, plain},
	literal::{datetime, duration, ident, param, scoring, table, tables, timeout},
	operator::{assigner, dir},
	part::{cond, data, data::single, fetch, fields, output},
	thing::thing,
	value::{value, values, whats},
	IResult,
};
use crate::sql::{statements::UpdateStatement, Fields, Value};
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

pub fn update(i: &str) -> IResult<&str, UpdateStatement> {
	let (i, _) = tag_no_case("UPDATE")(i)?;
	let (i, only) = opt(preceded(shouldbespace, tag_no_case("ONLY")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = whats(i)?;
	let (i, data) = opt(preceded(shouldbespace, data))(i)?;
	let (i, cond) = opt(preceded(shouldbespace, cond))(i)?;
	let (i, output) = opt(preceded(shouldbespace, output))(i)?;
	let (i, timeout) = opt(preceded(shouldbespace, timeout))(i)?;
	let (i, parallel) = opt(preceded(shouldbespace, tag_no_case("PARALLEL")))(i)?;
	Ok((
		i,
		UpdateStatement {
			only: only.is_some(),
			what,
			data,
			cond,
			output,
			timeout,
			parallel: parallel.is_some(),
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn update_statement() {
		let sql = "UPDATE test";
		let res = update(sql);
		let out = res.unwrap().1;
		assert_eq!("UPDATE test", format!("{}", out))
	}
}
