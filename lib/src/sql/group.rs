use crate::sql::comment::shouldbespace;
use crate::sql::common::commas;
use crate::sql::error::IResult;
use crate::sql::fmt::Fmt;
use crate::sql::idiom::{basic, Idiom};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::multi::separated_list1;
use nom::sequence::tuple;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub struct Groups(pub Vec<Group>);

impl Deref for Groups {
	type Target = Vec<Group>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl IntoIterator for Groups {
	type Item = Group;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl Display for Groups {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		if self.0.is_empty() {
			write!(f, "GROUP ALL")
		} else {
			write!(f, "GROUP BY {}", Fmt::comma_separated(&self.0))
		}
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub struct Group(pub Idiom);

impl Deref for Group {
	type Target = Idiom;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Display for Group {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

pub fn group(i: &str) -> IResult<&str, Groups> {
	alt((group_all, group_any))(i)
}

fn group_all(i: &str) -> IResult<&str, Groups> {
	let (i, _) = tag_no_case("GROUP")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ALL")(i)?;
	Ok((i, Groups(vec![])))
}

fn group_any(i: &str) -> IResult<&str, Groups> {
	let (i, _) = tag_no_case("GROUP")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("BY"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = separated_list1(commas, group_raw)(i)?;
	Ok((i, Groups(v)))
}

fn group_raw(i: &str) -> IResult<&str, Group> {
	let (i, v) = basic(i)?;
	Ok((i, Group(v)))
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::test::Parse;

	#[test]
	fn group_statement() {
		let sql = "GROUP field";
		let res = group(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, Groups(vec![Group(Idiom::parse("field"))]));
		assert_eq!("GROUP BY field", format!("{}", out));
	}

	#[test]
	fn group_statement_by() {
		let sql = "GROUP BY field";
		let res = group(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, Groups(vec![Group(Idiom::parse("field"))]));
		assert_eq!("GROUP BY field", format!("{}", out));
	}

	#[test]
	fn group_statement_multiple() {
		let sql = "GROUP field, other.field";
		let res = group(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Groups(vec![Group(Idiom::parse("field")), Group(Idiom::parse("other.field"))])
		);
		assert_eq!("GROUP BY field, other.field", format!("{}", out));
	}

	#[test]
	fn group_statement_all() {
		let sql = "GROUP ALL";
		let out = group(sql).unwrap().1;
		assert_eq!(out, Groups(Vec::new()));
		assert_eq!(sql, out.to_string());
	}
}
