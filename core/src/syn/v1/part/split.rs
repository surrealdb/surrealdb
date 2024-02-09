use super::super::{comment::shouldbespace, common::commas, idiom::basic, IResult};
use crate::sql::{Split, Splits};
use nom::{
	bytes::complete::tag_no_case,
	combinator::{cut, opt},
	multi::separated_list1,
	sequence::terminated,
};

pub fn split(i: &str) -> IResult<&str, Splits> {
	let (i, _) = tag_no_case("SPLIT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = opt(terminated(tag_no_case("ON"), shouldbespace))(i)?;
	let (i, v) = cut(separated_list1(commas, split_raw))(i)?;
	Ok((i, Splits(v)))
}

fn split_raw(i: &str) -> IResult<&str, Split> {
	let (i, v) = basic(i)?;
	Ok((i, Split(v)))
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::{sql::Idiom, syn::Parse};

	#[test]
	fn split_statement() {
		let sql = "SPLIT field";
		let res = split(sql);
		let out = res.unwrap().1;
		assert_eq!(out, Splits(vec![Split(Idiom::parse("field"))]),);
		assert_eq!("SPLIT ON field", format!("{}", out));
	}

	#[test]
	fn split_statement_on() {
		let sql = "SPLIT ON field";
		let res = split(sql);
		let out = res.unwrap().1;
		assert_eq!(out, Splits(vec![Split(Idiom::parse("field"))]),);
		assert_eq!("SPLIT ON field", format!("{}", out));
	}

	#[test]
	fn split_statement_multiple() {
		let sql = "SPLIT field, other.field";
		let res = split(sql);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Splits(vec![Split(Idiom::parse("field")), Split(Idiom::parse("other.field")),])
		);
		assert_eq!("SPLIT ON field, other.field", format!("{}", out));
	}
}
