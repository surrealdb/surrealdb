use super::super::IResult;
use crate::sql::statements::{BreakStatement, ContinueStatement};
use nom::bytes::complete::tag_no_case;

pub fn r#break(i: &str) -> IResult<&str, BreakStatement> {
	let (i, _) = tag_no_case("BREAK")(i)?;
	Ok((i, BreakStatement))
}

pub fn r#continue(i: &str) -> IResult<&str, ContinueStatement> {
	let (i, _) = tag_no_case("CONTINUE")(i)?;
	Ok((i, ContinueStatement))
}
#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn break_basic() {
		let sql = "BREAK";
		let res = r#break(sql);
		let out = res.unwrap().1;
		assert_eq!("BREAK", format!("{}", out))
	}

	#[test]
	fn continue_basic() {
		let sql = "CONTINUE";
		let res = r#continue(sql);
		let out = res.unwrap().1;
		assert_eq!("CONTINUE", format!("{}", out))
	}
}
