use crate::sql::comment::shouldbespace;
use crate::sql::common::commas;
use crate::sql::error::IResult;
use crate::sql::ident::ident_raw;
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::multi::separated_list1;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Store, Hash)]
pub enum With {
	NoIndex,
	Index(Vec<String>),
}

impl Display for With {
	fn fmt(&self, f: &mut Formatter) -> Result {
		f.write_str("WITH")?;
		match self {
			With::NoIndex => f.write_str(" NOINDEX"),
			With::Index(i) => {
				f.write_str(" INDEX ")?;
				f.write_str(&i.join(","))
			}
		}
	}
}

fn no_index(i: &str) -> IResult<&str, With> {
	let (i, _) = tag_no_case("NOINDEX")(i)?;
	Ok((i, With::NoIndex))
}

fn index(i: &str) -> IResult<&str, With> {
	let (i, _) = tag_no_case("INDEX")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = separated_list1(commas, ident_raw)(i)?;
	Ok((i, With::Index(v)))
}

pub fn with(i: &str) -> IResult<&str, With> {
	let (i, _) = tag_no_case("WITH")(i)?;
	let (i, _) = shouldbespace(i)?;
	alt((no_index, index))(i)
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn with_no_index() {
		let sql = "WITH NOINDEX";
		let res = with(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, With::NoIndex);
		assert_eq!("WITH NOINDEX", format!("{}", out));
	}

	#[test]
	fn with_index() {
		let sql = "WITH INDEX idx,uniq";
		let res = with(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, With::Index(vec!["idx".to_string(), "uniq".to_string()]));
		assert_eq!("WITH INDEX idx,uniq", format!("{}", out));
	}
}
