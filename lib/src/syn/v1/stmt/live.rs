use super::super::{
	comment::shouldbespace,
	error::expect_tag_no_case,
	literal::{param, table},
	part::{cond, fetch, fields},
	IResult,
};
use crate::sql::{statements::LiveStatement, Fields};
use nom::{
	branch::alt,
	bytes::complete::tag_no_case,
	combinator::{cut, into, map, opt},
	sequence::preceded,
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
