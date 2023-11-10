use super::{
	super::{
		comment::shouldbespace,
		common::{closeparentheses, commas, expect_delimited, openparentheses},
		error::expected,
		thing::id,
		IResult, ParseError,
	},
	duration::duration,
	ident_raw,
};
use crate::sql::Timeout;
use nom::{
	branch::alt,
	bytes::complete::{escaped, escaped_transform, is_not, tag, tag_no_case, take, take_while_m_n},
	character::complete::{anychar, char},
	combinator::{cut, map, map_res, opt, value},
	number::complete::recognize_float,
	sequence::{preceded, terminated},
	Err,
};

pub fn timeout(i: &str) -> IResult<&str, Timeout> {
	let (i, _) = tag_no_case("TIMEOUT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(duration)(i)?;
	Ok((i, Timeout(v)))
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::time::Duration;

	#[test]
	fn timeout_statement() {
		let sql = "TIMEOUT 5s";
		let res = timeout(sql);
		let out = res.unwrap().1;
		assert_eq!("TIMEOUT 5s", format!("{}", out));
		assert_eq!(out, Timeout(Duration::try_from("5s").unwrap()));
	}
}
