use crate::ctx::Context;
use crate::dbs::Iterable;
use crate::dbs::Iterator;
use crate::dbs::Level;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
use crate::sql::cond::{cond, Cond};
use crate::sql::error::IResult;
use crate::sql::fetch::{fetch, Fetchs};
use crate::sql::field::{fields, Field, Fields};
use crate::sql::group::{group, Groups};
use crate::sql::limit::{limit, Limit};
use crate::sql::order::{order, Orders};
use crate::sql::special::check_group_by_fields;
use crate::sql::special::check_order_by_fields;
use crate::sql::special::check_split_on_fields;
use crate::sql::split::{split, Splits};
use crate::sql::start::{start, Start};
use crate::sql::timeout::{timeout, Timeout};
use crate::sql::value::{selects, Value, Values};
use crate::sql::version::{version, Version};
use derive::Store;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::preceded;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store, Hash)]
pub struct SelectStatement {
	pub expr: Fields,
	pub what: Values,
	pub cond: Option<Cond>,
	pub split: Option<Splits>,
	pub group: Option<Groups>,
	pub order: Option<Orders>,
	pub limit: Option<Limit>,
	pub start: Option<Start>,
	pub fetch: Option<Fetchs>,
	pub version: Option<Version>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
}

impl SelectStatement {
	pub(crate) fn writeable(&self) -> bool {
		if self.expr.iter().any(|v| match v {
			Field::All => false,
			Field::Alone(v) => v.writeable(),
			Field::Alias(v, _) => v.writeable(),
		}) {
			return true;
		}
		if self.what.iter().any(|v| v.writeable()) {
			return true;
		}
		self.cond.as_ref().map_or(false, |v| v.writeable())
	}

	pub(crate) fn single(&self) -> bool {
		match self.what.len() {
			1 if self.what[0].is_object() => true,
			1 if self.what[0].is_thing() => true,
			_ => false,
		}
	}

	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Selected DB?
		opt.needs(Level::Db)?;
		// Allowed to run?
		opt.check(Level::No)?;
		// Create a new iterator
		let mut i = Iterator::new();
		// Ensure futures are stored
		let opt = &opt.futures(false);
		// Loop over the select targets
		for w in self.what.0.iter() {
			let v = w.compute(ctx, opt, txn, doc).await?;
			match v {
				Value::Table(v) => i.ingest(Iterable::Table(v)),
				Value::Thing(v) => i.ingest(Iterable::Thing(v)),
				Value::Range(v) => i.ingest(Iterable::Range(*v)),
				Value::Edges(v) => i.ingest(Iterable::Edges(*v)),
				Value::Model(v) => {
					for v in v {
						i.ingest(Iterable::Thing(v));
					}
				}
				Value::Array(v) => {
					for v in v {
						match v {
							Value::Table(v) => i.ingest(Iterable::Table(v)),
							Value::Thing(v) => i.ingest(Iterable::Thing(v)),
							Value::Edges(v) => i.ingest(Iterable::Edges(*v)),
							Value::Model(v) => {
								for v in v {
									i.ingest(Iterable::Thing(v));
								}
							}
							_ => i.ingest(Iterable::Value(v)),
						}
					}
				}
				v => i.ingest(Iterable::Value(v)),
			};
		}
		// Assign the statement
		let stm = Statement::from(self);
		// Output the results
		i.output(ctx, opt, txn, &stm).await
	}
}

impl fmt::Display for SelectStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "SELECT {} FROM {}", self.expr, self.what)?;
		if let Some(ref v) = self.cond {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.split {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.group {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.order {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.limit {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.start {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.fetch {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.version {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.timeout {
			write!(f, " {v}")?
		}
		if self.parallel {
			f.write_str(" PARALLEL")?
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
	let (i, what) = selects(i)?;
	let (i, cond) = opt(preceded(shouldbespace, cond))(i)?;
	let (i, split) = opt(preceded(shouldbespace, split))(i)?;
	check_split_on_fields(i, &expr, &split)?;
	let (i, group) = opt(preceded(shouldbespace, group))(i)?;
	check_group_by_fields(i, &expr, &group)?;
	let (i, order) = opt(preceded(shouldbespace, order))(i)?;
	check_order_by_fields(i, &expr, &order)?;
	let (i, limit) = opt(preceded(shouldbespace, limit))(i)?;
	let (i, start) = opt(preceded(shouldbespace, start))(i)?;
	let (i, fetch) = opt(preceded(shouldbespace, fetch))(i)?;
	let (i, version) = opt(preceded(shouldbespace, version))(i)?;
	let (i, timeout) = opt(preceded(shouldbespace, timeout))(i)?;
	let (i, parallel) = opt(preceded(shouldbespace, tag_no_case("PARALLEL")))(i)?;
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
			parallel: parallel.is_some(),
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
