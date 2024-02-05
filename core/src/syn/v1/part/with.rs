use super::super::{comment::shouldbespace, common::commas, literal::ident_raw, IResult};
use crate::sql::With;
use nom::{branch::alt, bytes::complete::tag_no_case, combinator::cut, multi::separated_list1};

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
