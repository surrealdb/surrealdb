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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store)]
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
	/// Return the statement limit number or 0 if not set
	pub fn limit(&self) -> usize {
		match self.limit {
			Some(Limit(v)) => v,
			None => 0,
		}
	}

	/// Return the statement start number or 0 if not set
	pub fn start(&self) -> usize {
		match self.start {
			Some(Start(v)) => v,
			None => 0,
		}
	}

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
		// Ensure futures are processed
		let opt = &opt.futures(true);
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

	fn idiom_in_fields(idom: &crate::sql::Idiom, expr: &Fields) -> bool {
		log::info!("group idom {:?}", idom.0);

		if idom.0.iter().any(|x| x == &crate::sql::Part::None) {
			return false;
		}

		let contains = expr.0.iter().any(|x| match x {
			crate::sql::Field::All => true,
			crate::sql::Field::Alone(x) => match x {
				crate::sql::Value::Idiom(i) => i == idom,
				_ => false,
			},
			crate::sql::Field::Alias(x, _) => match x {
				crate::sql::Value::Idiom(i) => i == idom,
				_ => false,
			},
		});

		!contains
	}

	///Validate Query is Integral
	fn check(mut self) -> IResult<&'static str, Self> {
		// this is not a group query, all is good
		if self.group.is_none() {
			return Ok(("", self));
		}

		let group = self.group.clone().unwrap();
		let exprs = &self.expr.clone();

		// get vector of Fields that need to be added to query because they did not exist in the original query
		let mut to_add = vec![];

		let groups_to_add: Vec<Field> = group
			.iter()
			.map(|b| &b.0)
			.filter(|idom| Self::idiom_in_fields(idom, exprs))
			.map(|i| Field::Alone(Value::Idiom(i.clone())))
			.collect();

		to_add.extend(groups_to_add);

		let order_fields_to_add = match &self.order {
			Some(o) => o
				.iter()
				.map(|x| crate::sql::Idiom(x.0.clone()))
				.filter(|idom| Self::idiom_in_fields(idom, exprs))
				.map(|i| Field::Alone(Value::Idiom(i.clone())))
				.collect(),
			None => vec![],
		};

		to_add.extend(order_fields_to_add);

		let split_fields_to_add = match &self.split {
			Some(o) => o
				.iter()
				.map(|x| &x.0)
				.filter(|idom| Self::idiom_in_fields(idom, exprs))
				.map(|i| Field::Alone(Value::Idiom(i.clone())))
				.collect(),
			None => vec![],
		};

		to_add.extend(split_fields_to_add);

		let fetch_fields_to_add = match &self.fetch {
			Some(o) => o
				.iter()
				.map(|x| &x.0)
				.filter(|idom| Self::idiom_in_fields(idom, exprs))
				.map(|i| Field::Alone(Value::Idiom(i.clone())))
				.collect(),
			None => vec![],
		};

		to_add.extend(fetch_fields_to_add);

		// deduplicate the collected required adds
		to_add.dedup();
		// TODO should propbably also DROP columns that were added before emitted

		// add vector of needed groupby fields to expression
		self.expr.0.extend(to_add);

		// make sure all expressions only use aggregateable values
		// based loosely from : https://github.com/surrealdb/surrealdb/blob/golang/sql/check.go
		let non_aggregate_non_self_in_query = self.expr.0.iter().any(|field| {
			match field {
				Field::All => true, // cannot use * in group clause
				Field::Alone(v) => !v.can_aggregate(&group),
				Field::Alias(v, _) => !v.can_aggregate(&group),
			}
		});

		// Throw an error if something was incorrectly used
		// want to use crate::err:Error enum for graceful error but couldn't figure how
		if non_aggregate_non_self_in_query {
			return Err(nom::Err::Error(crate::sql::Error::ParserError("failing function names")));
		}

		Ok(("", self))
	}
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
		if self.parallel {
			write!(f, " PARALLEL")?
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
	let (i, group) = opt(preceded(shouldbespace, group))(i)?;
	let (i, order) = opt(preceded(shouldbespace, order))(i)?;
	let (i, limit) = opt(preceded(shouldbespace, limit))(i)?;
	let (i, start) = opt(preceded(shouldbespace, start))(i)?;
	let (i, fetch) = opt(preceded(shouldbespace, fetch))(i)?;
	let (i, version) = opt(preceded(shouldbespace, version))(i)?;
	let (i, timeout) = opt(preceded(shouldbespace, timeout))(i)?;
	let (i, parallel) = opt(preceded(shouldbespace, tag_no_case("PARALLEL")))(i)?;

	let select = SelectStatement {
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
	}
	.check()?
	.1;

	Ok((i, select))
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
