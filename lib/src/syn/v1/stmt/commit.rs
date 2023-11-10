use super::super::{
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, commas, commasorspace, openparentheses},
	error::{expect_tag_no_case, expected},
	idiom::{basic, plain},
	literal::{datetime, duration, ident, scoring, table, tables},
	operator::{assigner, dir},
	thing::thing,
	value::value,
	IResult,
};
use crate::sql::statements::CommitStatement;
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

pub fn commit(i: &str) -> IResult<&str, CommitStatement> {
	let (i, _) = tag_no_case("COMMIT")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("TRANSACTION"))))(i)?;
	Ok((i, CommitStatement))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn commit_basic() {
		let sql = "COMMIT";
		let res = commit(sql);
		let out = res.unwrap().1;
		assert_eq!("COMMIT TRANSACTION", format!("{}", out))
	}

	#[test]
	fn commit_query() {
		let sql = "COMMIT TRANSACTION";
		let res = commit(sql);
		let out = res.unwrap().1;
		assert_eq!("COMMIT TRANSACTION", format!("{}", out))
	}
}
