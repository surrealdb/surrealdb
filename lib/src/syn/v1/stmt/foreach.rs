use super::super::{
	block::block,
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, commas, commasorspace, openparentheses},
	error::{expect_tag_no_case, expected},
	idiom::{basic, plain},
	literal::{datetime, duration, ident, param, scoring, table, tables, timeout},
	operator::{assigner, dir},
	part::{cond, data, output},
	thing::thing,
	value::{value, whats},
	IResult,
};
use crate::sql::statements::ForeachStatement;
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

pub fn foreach(i: &str) -> IResult<&str, ForeachStatement> {
	let (i, _) = tag_no_case("FOR")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, param) = param(i)?;
	let (i, (range, block)) = cut(|i| {
		let (i, _) = shouldbespace(i)?;
		let (i, _) = expect_tag_no_case("IN")(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, range) = value(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, block) = block(i)?;
		Ok((i, (range, block)))
	})(i)?;
	Ok((
		i,
		ForeachStatement {
			param,
			range,
			block,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn foreach_statement_first() {
		let sql = "FOR $test IN [1, 2, 3, 4, 5] { UPDATE person:test SET scores += $test; }";
		let res = foreach(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}
}
