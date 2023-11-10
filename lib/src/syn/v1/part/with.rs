use super::super::{
	comment::{mightbespace, shouldbespace},
	common::{commas, commasorspace},
	error::{expect_tag_no_case, expected},
	idiom::plain,
	literal::{duration, ident_raw, scoring, tables},
	operator::{assigner, dir},
	thing::thing,
	// TODO: go through and check every import for alias.
	value::value,
	IResult,
};
use crate::sql::With;
use nom::{
	branch::alt,
	bytes::complete::{escaped, escaped_transform, is_not, tag, tag_no_case, take, take_while_m_n},
	character::complete::{anychar, char, u16, u32},
	combinator::{cut, map, map_res, opt, recognize, value as map_value},
	multi::separated_list1,
	number::complete::recognize_float,
	sequence::{delimited, preceded, terminated, tuple},
	Err,
};

fn no_index(i: &str) -> IResult<&str, With> {
	let (i, _) = tag_no_case("NOINDEX")(i)?;
	Ok((i, With::NoIndex))
}

fn index(i: &str) -> IResult<&str, With> {
	let (i, _) = tag_no_case("INDEX")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(separated_list1(commas, ident_raw))(i)?;
	Ok((i, With::Index(v)))
}

pub fn with(i: &str) -> IResult<&str, With> {
	let (i, _) = tag_no_case("WITH")(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(alt((no_index, index)))(i)
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn with_no_index() {
		let sql = "WITH NOINDEX";
		let res = with(sql);
		let out = res.unwrap().1;
		assert_eq!(out, With::NoIndex);
		assert_eq!("WITH NOINDEX", format!("{}", out));
	}

	#[test]
	fn with_index() {
		let sql = "WITH INDEX idx,uniq";
		let res = with(sql);
		let out = res.unwrap().1;
		assert_eq!(out, With::Index(vec!["idx".to_string(), "uniq".to_string()]));
		assert_eq!("WITH INDEX idx,uniq", format!("{}", out));
	}
}
