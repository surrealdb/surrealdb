use super::super::{
	block::block,
	comment::{mightbespace, shouldbespace},
	error::expect_tag_no_case,
	literal::param,
	value::value,
	IResult,
};
use crate::sql::statements::ForeachStatement;
use nom::{bytes::complete::tag_no_case, combinator::cut};

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
