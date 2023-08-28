use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::ident::ident_raw;
use derive::Store;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::{preceded, tuple};
use nom::{
	branch::alt,
	combinator::{cut, map},
};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct UseStatement {
	pub ns: Option<String>,
	pub db: Option<String>,
}

impl fmt::Display for UseStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("USE")?;
		if let Some(ref ns) = self.ns {
			write!(f, " NS {ns}")?;
		}
		if let Some(ref db) = self.db {
			write!(f, " DB {db}")?;
		}
		Ok(())
	}
}

pub fn r#use(i: &str) -> IResult<&str, UseStatement> {
	let (i, _) = tag_no_case("USE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, (ns, db)) = alt((
		map(tuple((namespace, opt(preceded(shouldbespace, database)))), |x| (Some(x.0), x.1)),
		map(database, |x| (None, Some(x))),
	))(i)?;
	Ok((
		i,
		UseStatement {
			ns,
			db,
		},
	))
}

fn namespace(i: &str) -> IResult<&str, String> {
	let (i, _) = alt((tag_no_case("NAMESPACE"), tag_no_case("NS")))(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(ident_raw)(i)
}

fn database(i: &str) -> IResult<&str, String> {
	let (i, _) = alt((tag_no_case("DATABASE"), tag_no_case("DB")))(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(ident_raw)(i)
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn use_query_ns() {
		let sql = "USE NS test";
		let res = r#use(sql);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			UseStatement {
				ns: Some(String::from("test")),
				db: None,
			}
		);
		assert_eq!("USE NS test", format!("{}", out));
	}

	#[test]
	fn use_query_db() {
		let sql = "USE DB test";
		let res = r#use(sql);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			UseStatement {
				ns: None,
				db: Some(String::from("test")),
			}
		);
		assert_eq!("USE DB test", format!("{}", out));
	}

	#[test]
	fn use_query_both() {
		let sql = "USE NS test DB test";
		let res = r#use(sql);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			UseStatement {
				ns: Some(String::from("test")),
				db: Some(String::from("test")),
			}
		);
		assert_eq!("USE NS test DB test", format!("{}", out));
	}
}
