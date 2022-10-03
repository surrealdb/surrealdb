use crate::sql::comment::shouldbespace;
use crate::sql::common::commas;
use crate::sql::error::IResult;
use crate::sql::fmt::Fmt;
use crate::sql::idiom::{basic, Idiom};
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::multi::separated_list1;
use nom::sequence::tuple;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Splits(pub Vec<Split>);

impl Deref for Splits {
	type Target = Vec<Split>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl IntoIterator for Splits {
	type Item = Split;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl fmt::Display for Splits {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "SPLIT ON {}", Fmt::comma_separated(&self.0))
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Split(pub Idiom);

impl Deref for Split {
	type Target = Idiom;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Display for Split {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

pub fn split(i: &str) -> IResult<&str, Splits> {
	let (i, _) = tag_no_case("SPLIT")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("ON"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = separated_list1(commas, split_raw)(i)?;
	Ok((i, Splits(v)))
}

fn split_raw(i: &str) -> IResult<&str, Split> {
	let (i, v) = basic(i)?;
	Ok((i, Split(v)))
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::test::Parse;

	#[test]
	fn split_statement() {
		let sql = "SPLIT field";
		let res = split(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, Splits(vec![Split(Idiom::parse("field"))]),);
		assert_eq!("SPLIT ON field", format!("{}", out));
	}

	#[test]
	fn split_statement_on() {
		let sql = "SPLIT ON field";
		let res = split(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, Splits(vec![Split(Idiom::parse("field"))]),);
		assert_eq!("SPLIT ON field", format!("{}", out));
	}

	#[test]
	fn split_statement_multiple() {
		let sql = "SPLIT field, other.field";
		let res = split(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Splits(vec![Split(Idiom::parse("field")), Split(Idiom::parse("other.field")),])
		);
		assert_eq!("SPLIT ON field, other.field", format!("{}", out));
	}
}
