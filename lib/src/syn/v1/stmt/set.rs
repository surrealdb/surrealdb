use super::super::{
	block::block,
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, commas, commasorspace, openparentheses},
	error::{expect_tag_no_case, expected, ExplainResultExt},
	idiom::{basic, plain},
	literal::{datetime, duration, ident, ident_raw, param, scoring, table, tables, timeout},
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
use crate::sql::{statements::SetStatement, Fields, Value};
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

pub fn set(i: &str) -> IResult<&str, SetStatement> {
	let (i, _) = opt(terminated(tag_no_case("LET"), shouldbespace))(i)?;
	let (i, n) = preceded(char('$'), cut(ident_raw))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char('=')(i)?;
	let (i, w) = cut(|i| {
		let (i, _) = mightbespace(i)?;
		value(i)
	})(i)?;
	Ok((
		i,
		SetStatement {
			name: n,
			what: w,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn let_statement() {
		let sql = "LET $name = NULL";
		let res = set(sql);
		let out = res.unwrap().1;
		assert_eq!("LET $name = NULL", format!("{}", out));
	}
}
