use super::super::{
	comment::shouldbespace,
	part::{cond, output, timeout},
	value::whats,
	IResult,
};
use crate::sql::statements::DeleteStatement;
use nom::{bytes::complete::tag_no_case, combinator::opt, sequence::preceded};

pub fn delete(i: &str) -> IResult<&str, DeleteStatement> {
	let (i, _) = tag_no_case("DELETE")(i)?;
	let (i, _) = opt(preceded(shouldbespace, tag_no_case("FROM")))(i)?;
	let (i, only) = opt(preceded(shouldbespace, tag_no_case("ONLY")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = whats(i)?;
	let (i, cond) = opt(preceded(shouldbespace, cond))(i)?;
	let (i, output) = opt(preceded(shouldbespace, output))(i)?;
	let (i, timeout) = opt(preceded(shouldbespace, timeout))(i)?;
	let (i, parallel) = opt(preceded(shouldbespace, tag_no_case("PARALLEL")))(i)?;
	Ok((
		i,
		DeleteStatement {
			only: only.is_some(),
			what,
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
	fn delete_statement() {
		let sql = "DELETE test";
		let res = delete(sql);
		let out = res.unwrap().1;
		assert_eq!("DELETE test", format!("{}", out))
	}
}
