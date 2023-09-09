use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::idiom::{locals as idioms, Idioms};
use nom::bytes::complete::tag_no_case;
use nom::combinator::cut;

pub fn omit(i: &str) -> IResult<&str, Idioms> {
	let (i, _) = tag_no_case("OMIT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(idioms)(i)?;
	Ok((i, v))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn omit_statement() {
		let sql = "OMIT field";
		let res = omit(sql);
		let out = res.unwrap().1;
		assert_eq!("field", format!("{}", out));
	}

	#[test]
	fn omit_statement_multiple() {
		let sql = "OMIT field, other.field";
		let res = omit(sql);
		let out = res.unwrap().1;
		assert_eq!("field, other.field", format!("{}", out));
	}
}
