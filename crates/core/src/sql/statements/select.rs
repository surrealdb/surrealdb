use crate::ctx::{Context, MutableContext};
use crate::dbs::{Iterable, Iterator, Options, Statement};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::idx::planner::{QueryPlanner, StatementContext};
use crate::sql::{
	order::{OldOrders, Order, OrderList, Ordering},
	Cond, Explain, Fetchs, Field, Fields, Groups, Idioms, Limit, Splits, Start, Timeout, Value,
	Values, Version, With,
};
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

#[revisioned(revision = 4)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct SelectStatement {
	/// The foo,bar part in SELECT foo,bar FROM baz.
	pub expr: Fields,
	pub omit: Option<Idioms>,
	#[revision(start = 2)]
	pub only: bool,
	/// The baz part in SELECT foo,bar FROM baz.
	pub what: Values,
	pub with: Option<With>,
	pub cond: Option<Cond>,
	pub split: Option<Splits>,
	pub group: Option<Groups>,
	#[revision(end = 4, convert_fn = "convert_old_orders")]
	pub old_order: Option<OldOrders>,
	#[revision(start = 4)]
	pub order: Option<Ordering>,
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
	fn convert_old_orders(
		&mut self,
		_rev: u16,
		old_value: Option<OldOrders>,
	) -> Result<(), revision::Error> {
		let Some(x) = old_value else {
			// nothing to do.
			return Ok(());
		};

		if x.0.iter().any(|x| x.random) {
			self.order = Some(Ordering::Random);
			return Ok(());
		}

		let new_ord =
			x.0.into_iter()
				.map(|x| Order {
					value: x.order,
					collate: x.collate,
					numeric: x.numeric,
					direction: x.direction,
				})
				.collect();

		self.order = Some(Ordering::Order(OrderList(new_ord)));

		Ok(())
	}

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
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		// Valid options?
		opt.valid_for_db()?;
		// Assign the statement
		let stm = Statement::from(self);
		// Create a new iterator
		let mut i = Iterator::new();
		// Ensure futures are stored and the version is set if specified
		let version = match &self.version {
			Some(v) => Some(v.compute(stk, ctx, opt, doc).await?),
			_ => None,
		};
		let opt = Arc::new(opt.new_with_futures(false).with_version(version));
		// Extract the limit
		let limit = i.setup_limit(stk, ctx, &opt, &stm).await?;
		// Used for ONLY: is the limit 1?
		let limit_is_one_or_zero = match limit {
			Some(l) => l <= 1,
			_ => false,
		};
		// Fail for multiple targets without a limit
		if self.only && !limit_is_one_or_zero && self.what.0.len() > 1 {
			return Err(Error::SingleOnlyOutput);
		}
		// Check if there is a timeout
		let ctx = match self.timeout.as_ref() {
			Some(timeout) => {
				let mut ctx = MutableContext::new(ctx);
				ctx.add_timeout(*timeout.0)?;
				ctx.freeze()
			}
			None => ctx.clone(),
		};
		// Get a query planner
		let mut planner = QueryPlanner::new();
		let stm_ctx = StatementContext::new(&ctx, &opt, &stm)?;
		// Loop over the select targets
		for w in self.what.0.iter() {
			let v = w.compute(stk, &ctx, &opt, doc).await?;
			match v {
				Value::Thing(v) => match v.is_range() {
					true => {
						// Evaluate if we can only scan keys (rather than keys AND values)
						let keys_only = stm_ctx.is_keys_only(false, &v.tb).await?;
						i.prepare_range(&stm, v, keys_only)?
					}
					false => i.prepare_thing(&stm, v)?,
				},
				Value::Edges(v) => {
					if self.only && !limit_is_one_or_zero {
						return Err(Error::SingleOnlyOutput);
					}
					i.prepare_edges(&stm, *v)?;
				}
				Value::Mock(v) => {
					if self.only && !limit_is_one_or_zero {
						return Err(Error::SingleOnlyOutput);
					}
					i.prepare_mock(&stm, v)?;
				}
				Value::Table(t) => {
					if self.only && !limit_is_one_or_zero {
						return Err(Error::SingleOnlyOutput);
					}
					planner.add_iterables(stk, &stm_ctx, t, &mut i).await?;
				}
				Value::Array(v) => {
					if self.only && !limit_is_one_or_zero {
						return Err(Error::SingleOnlyOutput);
					}
					for v in v {
						match v {
							Value::Table(t) => {
								planner.add_iterables(stk, &stm_ctx, t, &mut i).await?;
							}
							Value::Mock(v) => i.prepare_mock(&stm, v)?,
							Value::Edges(v) => i.prepare_edges(&stm, *v)?,
							Value::Thing(v) => match v.is_range() {
								true => {
									// Evaluate if we can only scan keys (rather than keys AND values)
									let keys_only = stm_ctx.is_keys_only(false, &v.tb).await?;
									i.prepare_range(&stm, v, keys_only)?
								}
								false => i.prepare_thing(&stm, v)?,
							},
							_ => i.ingest(Iterable::Value(v)),
						}
					}
				}
				v => i.ingest(Iterable::Value(v)),
			};
		}
		// Create a new context
		let mut ctx = MutableContext::new(&ctx);
		// Add query executors if any
		if planner.has_executors() {
			ctx.set_query_planner(planner);
		}
		let ctx = ctx.freeze();
		// Process the statement
		let res = i.output(stk, &ctx, &opt, &stm).await?;
		// Catch statement timeout
		if ctx.is_timedout() {
			return Err(Error::QueryTimedout);
		}
		// Output the results
		match res {
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
