use super::super::{
	comment::{mightbespace, shouldbespace},
	literal::ident_raw,
	value::value,
	IResult,
};
use crate::sql::statements::SetStatement;
use nom::{
	bytes::complete::tag_no_case,
	character::complete::char,
	combinator::{cut, opt},
	sequence::{preceded, terminated},
};

pub fn set(i: &str) -> IResult<&str, SetStatement> {
	let (i, _) = opt(terminated(tag_no_case("LET"), shouldbespace))(i)?;
	let (i, n) = preceded(char('$'), cut(ident_raw))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char('=')(i)?;
	let (i, w) = cut(|i| {
		let (i, _) = mightbespace(i)?;
		value(i)
	})(i)?;
	Ok((
		i,
		SetStatement {
			name: n,
			what: w,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn let_statement() {
		let sql = "LET $name = NULL";
		let res = set(sql);
		let out = res.unwrap().1;
		assert_eq!("LET $name = NULL", format!("{}", out));
	}
}
