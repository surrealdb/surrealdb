use crate::ctx::Context;
use crate::dbs::Iterator;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::{Iterable, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::idx::planner::QueryPlanner;
use crate::sql::comment::shouldbespace;
use crate::sql::cond::{cond, Cond};
use crate::sql::ending;
use crate::sql::error::expect_tag_no_case;
use crate::sql::error::expected;
use crate::sql::error::IResult;
use crate::sql::explain::{explain, Explain};
use crate::sql::fetch::{fetch, Fetchs};
use crate::sql::field::{fields, Field, Fields};
use crate::sql::group::{group, Groups};
use crate::sql::idiom::Idioms;
use crate::sql::limit::{limit, Limit};
use crate::sql::omit::omit;
use crate::sql::order::{order, Orders};
use crate::sql::special::check_group_by_fields;
use crate::sql::special::check_order_by_fields;
use crate::sql::special::check_split_on_fields;
use crate::sql::split::{split, Splits};
use crate::sql::start::{start, Start};
use crate::sql::timeout::{timeout, Timeout};
use crate::sql::value::{selects, Value, Values};
use crate::sql::version::{version, Version};
use crate::sql::with::{with, With};
use derive::Store;
use nom::bytes::complete::tag_no_case;
use nom::combinator::cut;
use nom::combinator::opt;
use nom::combinator::peek;
use nom::sequence::preceded;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 2)]
pub struct SelectStatement {
	pub expr: Fields,
	pub omit: Option<Idioms>,
	#[revision(start = 2)]
	pub only: bool,
	pub what: Values,
	pub with: Option<With>,
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
	pub explain: Option<Explain>,
}

impl SelectStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		if self.expr.iter().any(|v| match v {
			Field::All => false,
			Field::Single {
				expr,
				..
			} => expr.writeable(),
		}) {
			return true;
		}
		if self.what.iter().any(|v| v.writeable()) {
			return true;
		}
		self.cond.as_ref().map_or(false, |v| v.writeable())
	}

	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Valid options?
		opt.valid_for_db()?;
		// Create a new iterator
		let mut i = Iterator::new();
		// Ensure futures are stored
		let opt = &opt.new_with_futures(false).with_projections(true);
		// Get a query planner
		let mut planner = QueryPlanner::new(opt, &self.with, &self.cond);
		// Loop over the select targets
		for w in self.what.0.iter() {
			let v = w.compute(ctx, opt, txn, doc).await?;
			match v {
				Value::Table(t) => {
					planner.add_iterables(ctx, txn, t, &mut i).await?;
				}
				Value::Thing(v) => i.ingest(Iterable::Thing(v)),
				Value::Range(v) => i.ingest(Iterable::Range(*v)),
				Value::Edges(v) => i.ingest(Iterable::Edges(*v)),
				Value::Mock(v) => {
					for v in v {
						i.ingest(Iterable::Thing(v));
					}
				}
				Value::Array(v) => {
					for v in v {
						match v {
							Value::Table(t) => {
								planner.add_iterables(ctx, txn, t, &mut i).await?;
							}
							Value::Thing(v) => i.ingest(Iterable::Thing(v)),
							Value::Edges(v) => i.ingest(Iterable::Edges(*v)),
							Value::Mock(v) => {
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
		// Create a new context
		let mut ctx = Context::new(ctx);
		// Assign the statement
		let stm = Statement::from(self);
		// Add query executors if any
		if planner.has_executors() {
			ctx.set_query_planner(&planner);
		}
		// Output the results
		match i.output(&ctx, opt, txn, &stm).await? {
			// This is a single record result
			Value::Array(mut a) if self.only => match a.len() {
				// There was exactly one result
				1 => Ok(a.remove(0)),
				// There were no results
				_ => Err(Error::SingleOnlyOutput),
			},
			// This is standard query result
			v => Ok(v),
		}
	}
}

impl fmt::Display for SelectStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "SELECT {}", self.expr)?;
		if let Some(ref v) = self.omit {
			write!(f, " OMIT {v}")?
		}
		write!(f, " FROM")?;
		if self.only {
			f.write_str(" ONLY")?
		}
		write!(f, " {}", self.what)?;
		if let Some(ref v) = self.with {
			write!(f, " {v}")?
		}
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
		if let Some(ref v) = self.explain {
			write!(f, " {v}")?
		}
		Ok(())
	}
}

pub fn select(i: &str) -> IResult<&str, SelectStatement> {
	let (i, _) = tag_no_case("SELECT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, expr) = fields(i)?;
	let (i, omit) = opt(preceded(shouldbespace, omit))(i)?;
	let (i, _) = cut(shouldbespace)(i)?;
	let (i, _) = expect_tag_no_case("FROM")(i)?;
	let (i, only) = opt(preceded(shouldbespace, tag_no_case("ONLY")))(i)?;
	let (i, _) = cut(shouldbespace)(i)?;
	let (i, what) = cut(selects)(i)?;
	let (i, with) = opt(preceded(shouldbespace, with))(i)?;
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
	let (i, explain) = opt(preceded(shouldbespace, explain))(i)?;
	let (i, _) = expected(
		"one of WITH, WHERE, SPLIT, GROUP, ORDER, LIMIT, START, FETCH, VERSION, TIMEOUT, PARELLEL, or EXPLAIN",
		cut(peek(ending::query))
	)(i)?;

	Ok((
		i,
		SelectStatement {
			expr,
			omit,
			only: only.is_some(),
			what,
			with,
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
			explain,
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
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn select_statement_table() {
		let sql = "SELECT * FROM test";
		let res = select(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out));
	}

	#[test]
	fn select_statement_omit() {
		let sql = "SELECT * OMIT password FROM test";
		let res = select(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out));
	}

	#[test]
	fn select_statement_thing() {
		let sql = "SELECT * FROM test:thingy ORDER BY name";
		let res = select(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn select_statement_clash() {
		let sql = "SELECT * FROM order ORDER BY order";
		let res = select(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn select_statement_table_thing() {
		let sql = "SELECT *, ((1 + 3) / 4), 1.3999f AS tester FROM test, test:thingy";
		let res = select(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn select_with_function() {}
}
