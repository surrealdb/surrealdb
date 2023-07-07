use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
use crate::sql::common::take_u64;
use crate::sql::error::IResult;
use crate::sql::table::{table, Table};
use crate::sql::value::Value;
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::u32;
use nom::combinator::map;
use nom::combinator::opt;
use nom::sequence::preceded;
use serde::{Deserialize, Serialize};
use std::fmt;

// ShowStatement is used to show changes in a table or database via
// the SHOW CHANGES statement.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store, Hash)]
pub struct ShowStatement {
	pub table: Option<Table>,
	pub since: Option<u64>,
	pub limit: Option<u32>,
}

impl ShowStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		_opt: &Options,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		Err(Error::FeatureNotYetImplemented {
			feature: "change feed",
		})
	}
}

impl fmt::Display for ShowStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "SHOW CHANGES FOR")?;
		match self.table {
			Some(ref v) => write!(f, " TABLE {}", v)?,
			None => write!(f, " DATABASE")?,
		}
		if let Some(ref v) = self.since {
			write!(f, " SINCE {}", v)?
		}
		if let Some(ref v) = self.limit {
			write!(f, " LIMIT {}", v)?
		}
		Ok(())
	}
}

pub fn table_or_database(i: &str) -> IResult<&str, Option<Table>> {
	let (i, v) = alt((
		map(preceded(tag_no_case("table"), preceded(shouldbespace, table)), Some),
		map(tag_no_case("database"), |_| None),
	))(i)?;
	Ok((i, v))
}

pub fn since(i: &str) -> IResult<&str, u64> {
	let (i, _) = tag_no_case("SINCE")(i)?;
	let (i, _) = shouldbespace(i)?;

	take_u64(i)
}

pub fn limit(i: &str) -> IResult<&str, u32> {
	let (i, _) = tag_no_case("LIMIT")(i)?;
	let (i, _) = shouldbespace(i)?;

	u32(i)
}

pub fn show(i: &str) -> IResult<&str, ShowStatement> {
	let (i, _) = tag_no_case("SHOW CHANGES")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("FOR")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, table) = table_or_database(i)?;
	let (i, since) = opt(preceded(shouldbespace, since))(i)?;
	let (i, limit) = opt(preceded(shouldbespace, limit))(i)?;
	Ok((
		i,
		ShowStatement {
			table,
			since,
			limit,
		},
	))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn tb() {
		let sql = "TABLE person";
		let res = table_or_database(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1.unwrap();
		assert_eq!("person", format!("{}", out))
	}

	#[test]
	fn db() {
		let sql = "DATABASE";
		let res = table_or_database(sql);
		assert!(res.is_ok());
		assert!(res.unwrap().1.is_none())
	}

	#[test]
	fn show_table_changes() {
		let sql = "SHOW CHANGES FOR TABLE person";
		let res = show(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn show_table_changes_since() {
		let sql = "SHOW CHANGES FOR TABLE person SINCE 0";
		let res = show(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn show_table_changes_limit() {
		let sql = "SHOW CHANGES FOR TABLE person LIMIT 10";
		let res = show(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn show_table_changes_since_limit() {
		let sql = "SHOW CHANGES FOR TABLE person SINCE 0 LIMIT 10";
		let res = show(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn show_database_changes() {
		let sql = "SHOW CHANGES FOR DATABASE";
		let res = show(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn show_database_changes_since() {
		let sql = "SHOW CHANGES FOR DATABASE SINCE 0";
		let res = show(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn show_database_changes_limit() {
		let sql = "SHOW CHANGES FOR DATABASE LIMIT 10";
		let res = show(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn show_database_changes_since_limit() {
		let sql = "SHOW CHANGES FOR DATABASE SINCE 0 LIMIT 10";
		let res = show(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}
}
