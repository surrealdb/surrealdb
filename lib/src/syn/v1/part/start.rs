use super::super::{
	comment::{mightbespace, shouldbespace},
	common::commas,
	literal::{ident, scoring},
	value::value,
	IResult,
};
use crate::sql::Start;
use nom::{
	branch::alt,
	bytes::complete::{escaped, escaped_transform, is_not, tag, tag_no_case, take, take_while_m_n},
	character::complete::{anychar, char, u16, u32},
	combinator::{cut, map, map_res, opt, recognize},
	multi::separated_list1,
	number::complete::recognize_float,
	sequence::{delimited, preceded, terminated, tuple},
	Err,
};

pub fn start(i: &str) -> IResult<&str, Start> {
	let (i, _) = tag_no_case("START")(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(|i| {
		let (i, _) = opt(terminated(tag_no_case("AT"), shouldbespace))(i)?;
		let (i, v) = value(i)?;
		Ok((i, Start(v)))
	})(i)
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn start_statement() {
		let sql = "START 100";
		let res = start(sql);
		let out = res.unwrap().1;
		assert_eq!(out, Start(Value::from(100)));
		assert_eq!("START 100", format!("{}", out));
	}

	#[test]
	fn start_statement_at() {
		let sql = "START AT 100";
		let res = start(sql);
		let out = res.unwrap().1;
		assert_eq!(out, Start(Value::from(100)));
		assert_eq!("START 100", format!("{}", out));
	}
}
