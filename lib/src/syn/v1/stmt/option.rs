use super::super::{
	comment::{mightbespace, shouldbespace},
	error::expected,
	literal::ident,
	IResult,
};
use crate::sql::statements::OptionStatement;
use nom::{
	branch::alt,
	bytes::complete::tag_no_case,
	character::complete::char,
	combinator::{cut, opt, value},
	sequence::tuple,
};

pub fn option(i: &str) -> IResult<&str, OptionStatement> {
	let (i, _) = tag_no_case("OPTION")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, n) = ident(i)?;
	let (i, v) = expected(
		"'=' followed by a value for the option",
		cut(opt(alt((
			value(true, tuple((mightbespace, char('='), mightbespace, tag_no_case("TRUE")))),
			value(false, tuple((mightbespace, char('='), mightbespace, tag_no_case("FALSE")))),
		)))),
	)(i)?;
	Ok((
		i,
		OptionStatement {
			name: n,
			what: v.unwrap_or(true),
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn option_statement() {
		let sql = "OPTION IMPORT";
		let res = option(sql);
		let out = res.unwrap().1;
		assert_eq!("OPTION IMPORT", format!("{}", out));
	}

	#[test]
	fn option_statement_true() {
		let sql = "OPTION IMPORT = TRUE";
		let res = option(sql);
		let out = res.unwrap().1;
		assert_eq!("OPTION IMPORT", format!("{}", out));
	}

	#[test]
	fn option_statement_false() {
		let sql = "OPTION IMPORT = FALSE";
		let res = option(sql);
		let out = res.unwrap().1;
		assert_eq!("OPTION IMPORT = FALSE", format!("{}", out));
	}
}
