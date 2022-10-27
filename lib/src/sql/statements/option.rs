use crate::sql::comment::mightbespace;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::ident::{ident, Ident};
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::char;
use nom::combinator::{map, opt};
use nom::sequence::tuple;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store, Hash)]
pub struct OptionStatement {
	pub name: Ident,
	pub what: bool,
}

impl fmt::Display for OptionStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		if self.what {
			write!(f, "OPTION {}", self.name)
		} else {
			write!(f, "OPTION {} = FALSE", self.name)
		}
	}
}

pub fn option(i: &str) -> IResult<&str, OptionStatement> {
	let (i, _) = tag_no_case("OPTION")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, n) = ident(i)?;
	let (i, v) = opt(alt((
		map(tuple((mightbespace, char('='), mightbespace, tag_no_case("TRUE"))), |_| true),
		map(tuple((mightbespace, char('='), mightbespace, tag_no_case("FALSE"))), |_| false),
	)))(i)?;
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
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("OPTION IMPORT", format!("{}", out));
	}

	#[test]
	fn option_statement_true() {
		let sql = "OPTION IMPORT = TRUE";
		let res = option(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("OPTION IMPORT", format!("{}", out));
	}

	#[test]
	fn option_statement_false() {
		let sql = "OPTION IMPORT = FALSE";
		let res = option(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("OPTION IMPORT = FALSE", format!("{}", out));
	}
}
