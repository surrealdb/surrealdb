use super::super::{comment::shouldbespace, IResult};
use crate::sql::statements::BeginStatement;
use nom::{bytes::complete::tag_no_case, combinator::opt, sequence::tuple};

pub fn begin(i: &str) -> IResult<&str, BeginStatement> {
	let (i, _) = tag_no_case("BEGIN")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("TRANSACTION"))))(i)?;
	Ok((i, BeginStatement))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn begin_basic() {
		let sql = "BEGIN";
		let res = begin(sql);
		let out = res.unwrap().1;
		assert_eq!("BEGIN TRANSACTION", format!("{}", out))
	}

	#[test]
	fn begin_query() {
		let sql = "BEGIN TRANSACTION";
		let res = begin(sql);
		let out = res.unwrap().1;
		assert_eq!("BEGIN TRANSACTION", format!("{}", out))
	}
}
