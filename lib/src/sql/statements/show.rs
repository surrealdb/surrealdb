use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::Action;
use crate::iam::ResourceKind;
use crate::sql::comment::shouldbespace;
use crate::sql::common::take_u64;
use crate::sql::datetime::datetime;
use crate::sql::error::expect_tag_no_case;
use crate::sql::error::expected;
use crate::sql::error::IResult;
use crate::sql::table::{table, Table};
use crate::sql::value::Value;
use crate::sql::Base;
use crate::sql::Datetime;
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::u32;
use nom::combinator::cut;
use nom::combinator::map;
use nom::combinator::opt;
use nom::combinator::value;
use nom::sequence::preceded;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[revisioned(revision = 1)]
pub enum ShowSince {
	Timestamp(Datetime),
	Versionstamp(u64),
}

// ShowStatement is used to show changes in a table or database via
// the SHOW CHANGES statement.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct ShowStatement {
	pub table: Option<Table>,
	pub since: ShowSince,
	pub limit: Option<u32>,
}

impl ShowStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Selected DB?
		opt.is_allowed(Action::View, ResourceKind::Table, &Base::Db)?;
		// Clone transaction
		let txn = txn.clone();
		// Claim transaction
		let mut run = txn.lock().await;
		// Process the show query
		let tb = self.table.as_deref();
		let r = crate::cf::read(
			&mut run,
			opt.ns(),
			opt.db(),
			tb.map(|x| x.as_str()),
			self.since.clone(),
			self.limit,
		)
		.await?;
		// Return the changes
		let mut a = Vec::<Value>::new();
		for r in r.iter() {
			let v: Value = r.clone().into_value();
			a.push(v);
		}
		let v: Value = Value::Array(crate::sql::array::Array(a));
		Ok(v)
	}
}

impl fmt::Display for ShowStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "SHOW CHANGES FOR")?;
		match self.table {
			Some(ref v) => write!(f, " TABLE {}", v)?,
			None => write!(f, " DATABASE")?,
		}
		match self.since {
			ShowSince::Timestamp(ref v) => write!(f, " SINCE {}", v)?,
			ShowSince::Versionstamp(ref v) => write!(f, " SINCE {}", v)?,
		}
		if let Some(ref v) = self.limit {
			write!(f, " LIMIT {}", v)?
		}
		Ok(())
	}
}

pub fn table_or_database(i: &str) -> IResult<&str, Option<Table>> {
	let (i, v) = expected(
		"one of TABLE, DATABASE",
		alt((
			map(preceded(tag_no_case("TABLE"), preceded(shouldbespace, cut(table))), Some),
			value(None, tag_no_case("DATABASE")),
		)),
	)(i)?;
	Ok((i, v))
}

pub fn since(i: &str) -> IResult<&str, ShowSince> {
	let (i, _) = expect_tag_no_case("SINCE")(i)?;
	let (i, _) = shouldbespace(i)?;

	cut(alt((map(take_u64, ShowSince::Versionstamp), map(datetime, ShowSince::Timestamp))))(i)
}

pub fn limit(i: &str) -> IResult<&str, u32> {
	let (i, _) = tag_no_case("LIMIT")(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(u32)(i)
}

pub fn show(i: &str) -> IResult<&str, ShowStatement> {
	let (i, _) = tag_no_case("SHOW")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("CHANGES")(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(|i| {
		let (i, _) = tag_no_case("FOR")(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, table) = table_or_database(i)?;
		let (i, since) = preceded(shouldbespace, since)(i)?;
		let (i, limit) = opt(preceded(shouldbespace, limit))(i)?;
		Ok((
			i,
			ShowStatement {
				table,
				since,
				limit,
			},
		))
	})(i)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn tb() {
		let sql = "TABLE person";
		let res = table_or_database(sql);
		let out = res.unwrap().1.unwrap();
		assert_eq!("person", format!("{}", out))
	}

	#[test]
	fn db() {
		let sql = "DATABASE";
		let res = table_or_database(sql);
		assert!(res.unwrap().1.is_none())
	}

	#[test]
	fn show_table_changes() {
		let sql = "SHOW CHANGES FOR TABLE person";
		show(sql).unwrap_err();
	}

	#[test]
	fn show_table_changes_since() {
		let sql = "SHOW CHANGES FOR TABLE person SINCE 0";
		let res = show(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn show_table_changes_since_ts() {
		let sql = "SHOW CHANGES FOR TABLE person SINCE '2022-07-03T07:18:52Z'";
		let res = show(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn show_table_changes_limit() {
		let sql = "SHOW CHANGES FOR TABLE person LIMIT 10";
		show(sql).unwrap_err();
	}

	#[test]
	fn show_table_changes_since_limit() {
		let sql = "SHOW CHANGES FOR TABLE person SINCE 0 LIMIT 10";
		let res = show(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn show_database_changes() {
		let sql = "SHOW CHANGES FOR DATABASE";
		show(sql).unwrap_err();
	}

	#[test]
	fn show_database_changes_since() {
		let sql = "SHOW CHANGES FOR DATABASE SINCE 0";
		let res = show(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn show_database_changes_since_ts() {
		let sql = "SHOW CHANGES FOR DATABASE SINCE '2022-07-03T07:18:52Z'";
		let res = show(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn show_database_changes_limit() {
		let sql = "SHOW CHANGES FOR DATABASE LIMIT 10";
		show(sql).unwrap_err();
	}

	#[test]
	fn show_database_changes_since_limit() {
		let sql = "SHOW CHANGES FOR DATABASE SINCE 0 LIMIT 10";
		let res = show(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}
}
