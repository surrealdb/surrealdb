use crate::sql::comment::shouldbespace;
use crate::sql::common::commas;
use crate::sql::idiom::{idiom, Idiom};
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::multi::separated_nonempty_list;
use nom::sequence::tuple;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Splits(Vec<Split>);

impl fmt::Display for Splits {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"SPLIT ON {}",
			self.0
				.iter()
				.map(|ref v| format!("{}", v))
				.collect::<Vec<_>>()
				.join(", ")
		)
	}
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Split {
	pub split: Idiom,
}

impl fmt::Display for Split {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.split)
	}
}

pub fn split(i: &str) -> IResult<&str, Splits> {
	let (i, _) = tag_no_case("SPLIT")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("ON"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = separated_nonempty_list(commas, split_raw)(i)?;
	Ok((i, Splits(v)))
}

fn split_raw(i: &str) -> IResult<&str, Split> {
	let (i, v) = idiom(i)?;
	Ok((i, Split { split: v }))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn split_statement() {
		let sql = "SPLIT field";
		let res = split(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Splits(vec![Split {
				split: Idiom::from("field")
			}])
		);
		assert_eq!("SPLIT ON field", format!("{}", out));
	}

	#[test]
	fn split_statement_on() {
		let sql = "SPLIT ON field";
		let res = split(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Splits(vec![Split {
				split: Idiom::from("field")
			}])
		);
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
			Splits(vec![
				Split {
					split: Idiom::from("field")
				},
				Split {
					split: Idiom::from("other.field")
				},
			])
		);
		assert_eq!("SPLIT ON field, other.field", format!("{}", out));
	}
}
