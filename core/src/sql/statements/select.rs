use crate::ctx::Context;
use crate::dbs::{Iterable, Iterator, Options, Statement, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::idx::planner::QueryPlanner;
use crate::sql::{
	Cond, Explain, Fetchs, Field, Fields, Groups, Idioms, Limit, Orders, Splits, Start, Timeout,
	Value, Values, Version, With,
};
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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
	#[revision(start = 3)]
	pub tempfiles: bool,
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
		stk: &mut Stk,
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
		// Used for ONLY: is the limit 1?
		let limit_is_one_or_zero = match &self.limit {
			Some(l) => l.process(stk, ctx, opt, txn, doc).await? <= 1,
			_ => false,
		};
		// Fail for multiple targets without a limit
		if self.only && !limit_is_one_or_zero && self.what.0.len() > 1 {
			return Err(Error::SingleOnlyOutput);
		}
		// Loop over the select targets
		for w in self.what.0.iter() {
			let v = w.compute(stk, ctx, opt, txn, doc).await?;
			match v {
				Value::Table(t) => {
					if self.only && !limit_is_one_or_zero {
						return Err(Error::SingleOnlyOutput);
					}

					planner.add_iterables(stk, ctx, txn, t, &mut i).await?;
				}
				Value::Thing(v) => i.ingest(Iterable::Thing(v)),
				Value::Range(v) => {
					if self.only && !limit_is_one_or_zero {
						return Err(Error::SingleOnlyOutput);
					}

					i.ingest(Iterable::Range(*v))
				}
				Value::Edges(v) => {
					if self.only && !limit_is_one_or_zero {
						return Err(Error::SingleOnlyOutput);
					}

					i.ingest(Iterable::Edges(*v))
				}
				Value::Mock(v) => {
					if self.only && !limit_is_one_or_zero {
						return Err(Error::SingleOnlyOutput);
					}

					for v in v {
						i.ingest(Iterable::Thing(v));
					}
				}
				Value::Array(v) => {
					if self.only && !limit_is_one_or_zero {
						return Err(Error::SingleOnlyOutput);
					}

					for v in v {
						match v {
							Value::Table(t) => {
								planner.add_iterables(stk, ctx, txn, t, &mut i).await?;
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
		match i.output(stk, &ctx, opt, txn, &stm).await? {
			// This is a single record result
			Value::Array(mut a) if self.only => match a.len() {
				// There were no results
				0 => Ok(Value::None),
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
