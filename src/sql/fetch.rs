use crate::sql::comment::shouldbespace;
use crate::sql::common::commas;
use crate::sql::idiom::{idiom, Idiom};
use nom::bytes::complete::tag_no_case;
use nom::multi::separated_list1;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Fetchs(pub Vec<Fetch>);

impl fmt::Display for Fetchs {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"FETCH {}",
			self.0.iter().map(|ref v| format!("{}", v)).collect::<Vec<_>>().join(", ")
		)
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Fetch {
	pub fetch: Idiom,
}

impl fmt::Display for Fetch {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.fetch)
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
	Ok((
		i,
		Fetch {
			fetch: v,
		},
	))
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
		assert_eq!(
			out,
			Fetchs(vec![Fetch {
				fetch: Idiom::parse("field")
			}])
		);
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
			Fetchs(vec![
				Fetch {
					fetch: Idiom::parse("field")
				},
				Fetch {
					fetch: Idiom::parse("other.field")
				},
			])
		);
		assert_eq!("FETCH field, other.field", format!("{}", out));
	}
}
