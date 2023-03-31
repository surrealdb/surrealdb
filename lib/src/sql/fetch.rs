use crate::sql::comment::shouldbespace;
use crate::sql::common::commas;
use crate::sql::error::IResult;
use crate::sql::fmt::Fmt;
use crate::sql::idiom::{plain as idiom, Idiom};
use nom::bytes::complete::tag_no_case;
use nom::multi::separated_list1;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct Fetchs(pub Vec<Fetch>);

impl Deref for Fetchs {
	type Target = Vec<Fetch>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl IntoIterator for Fetchs {
	type Item = Fetch;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl fmt::Display for Fetchs {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "FETCH {}", Fmt::comma_separated(&self.0))
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct Fetch(pub Idiom);

impl Deref for Fetch {
	type Target = Idiom;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Display for Fetch {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

pub fn fetch(i: &str) -> IResult<&str, Fetchs> {
	let (i, _) = tag_no_case("FETCH")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = separated_list1(commas, fetch_raw)(i)?;
	Ok((i, Fetchs(v)))
}

fn fetch_raw(i: &str) -> IResult<&str, Fetch> {
	let (i, v) = idiom(i)?;
	Ok((i, Fetch(v)))
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::test::Parse;

	#[test]
	fn fetch_statement() {
		let sql = "FETCH field";
		let res = fetch(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, Fetchs(vec![Fetch(Idiom::parse("field"))]));
		assert_eq!("FETCH field", format!("{}", out));
	}

	#[test]
	fn fetch_statement_multiple() {
		let sql = "FETCH field, other.field";
		let res = fetch(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Fetchs(vec![Fetch(Idiom::parse("field")), Fetch(Idiom::parse("other.field")),])
		);
		assert_eq!("FETCH field, other.field", format!("{}", out));
	}
}
