use crate::sql::comment::shouldbespace;
use crate::sql::cond::{cond, Cond};
use crate::sql::fetch::{fetch, Fetchs};
use crate::sql::field::{fields, Fields};
use crate::sql::group::{group, Groups};
use crate::sql::limit::{limit, Limit};
use crate::sql::literal::{literals, Literals};
use crate::sql::order::{order, Orders};
use crate::sql::split::{split, Splits};
use crate::sql::start::{start, Start};
use crate::sql::timeout::{timeout, Timeout};
use crate::sql::version::{version, Version};
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::preceded;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct SelectStatement {
	pub expr: Fields,
	pub what: Literals,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub cond: Option<Cond>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub split: Option<Splits>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub group: Option<Groups>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub order: Option<Orders>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub limit: Option<Limit>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub start: Option<Start>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub fetch: Option<Fetchs>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub version: Option<Version>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub timeout: Option<Timeout>,
}

impl fmt::Display for SelectStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "SELECT {} FROM {}", self.expr, self.what)?;
		if let Some(ref v) = self.cond {
			write!(f, " {}", v)?
		}
		if let Some(ref v) = self.split {
			write!(f, " {}", v)?
		}
		if let Some(ref v) = self.group {
			write!(f, " {}", v)?
		}
		if let Some(ref v) = self.order {
			write!(f, " {}", v)?
		}
		if let Some(ref v) = self.limit {
			write!(f, " {}", v)?
		}
		if let Some(ref v) = self.start {
			write!(f, " {}", v)?
		}
		if let Some(ref v) = self.fetch {
			write!(f, " {}", v)?
		}
		if let Some(ref v) = self.version {
			write!(f, " {}", v)?
		}
		if let Some(ref v) = self.timeout {
			write!(f, " {}", v)?
		}
		Ok(())
	}
}

pub fn select(i: &str) -> IResult<&str, SelectStatement> {
	let (i, _) = tag_no_case("SELECT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, expr) = fields(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("FROM")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = literals(i)?;
	let (i, cond) = opt(preceded(shouldbespace, cond))(i)?;
	let (i, split) = opt(preceded(shouldbespace, split))(i)?;
	let (i, group) = opt(preceded(shouldbespace, group))(i)?;
	let (i, order) = opt(preceded(shouldbespace, order))(i)?;
	let (i, limit) = opt(preceded(shouldbespace, limit))(i)?;
	let (i, start) = opt(preceded(shouldbespace, start))(i)?;
	let (i, fetch) = opt(preceded(shouldbespace, fetch))(i)?;
	let (i, version) = opt(preceded(shouldbespace, version))(i)?;
	let (i, timeout) = opt(preceded(shouldbespace, timeout))(i)?;
	Ok((
		i,
		SelectStatement {
			expr,
			what,
			cond,
			split,
			group,
			order,
			limit,
			start,
			fetch,
			version,
			timeout,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn select_statement_param() {
		let sql = "SELECT * FROM $test";
		let res = select(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn select_statement_table() {
		let sql = "SELECT * FROM test";
		let res = select(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out));
	}

	#[test]
	fn select_statement_thing() {
		let sql = "SELECT * FROM test:thingy ORDER BY name";
		let res = select(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn select_statement_clash() {
		let sql = "SELECT * FROM order ORDER BY order";
		let res = select(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn select_statement_table_thing() {
		let sql = "SELECT *, ((1 + 3) / 4), 1.3999 AS tester FROM test, test:thingy";
		let res = select(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}
}
