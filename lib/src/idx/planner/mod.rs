pub(crate) mod executor;
pub(crate) mod iterators;
pub(crate) mod plan;
mod tree;

use crate::ctx::Context;
use crate::dbs::{Iterable, Iterator, Options, Transaction};
use crate::err::Error;
use crate::idx::planner::executor::QueryExecutor;
use crate::idx::planner::plan::{Plan, PlanBuilder};
use crate::idx::planner::tree::Tree;
use crate::sql::{Cond, Table};
use std::collections::HashMap;

pub(crate) struct QueryPlanner<'a> {
	opt: &'a Options,
	cond: &'a Option<Cond>,
	/// There is one executor per table
	executors: HashMap<String, QueryExecutor>,
}

impl<'a> QueryPlanner<'a> {
	pub(crate) fn new(opt: &'a Options, cond: &'a Option<Cond>) -> Self {
		Self {
			opt,
			cond,
			executors: HashMap::default(),
		}
	}

	pub(crate) async fn add_iterables(
		&mut self,
		ctx: &Context<'_>,
		txn: &Transaction,
		t: Table,
		it: &mut Iterator,
	) -> Result<(), Error> {
		let res = Tree::build(ctx, self.opt, txn, &t, self.cond).await?;
		if let Some((node, im)) = res {
			let exe = QueryExecutor::new(self.opt, txn, &t, im).await?;
			self.executors.insert(t.0.clone(), exe);
			match PlanBuilder::build(node) {
				Ok(plan) => match plan {
					Plan::SingleIndex(exp, io) => {
						it.ingest(Iterable::Index(t, exp, io));
						return Ok(());
					}
					Plan::MultiIndex(v) => {
						for (exp, io) in v {
							it.ingest(Iterable::Index(t.clone(), exp, io));
						}
						return Ok(());
					}
				},
				Err(Error::BypassQueryPlanner) => {}
				Err(e) => return Err(e),
			}
		}
		it.ingest(Iterable::Table(t));
		Ok(())
	}

	pub(crate) fn finish(self) -> Option<HashMap<String, QueryExecutor>> {
		if self.executors.is_empty() {
			None
		} else {
			Some(self.executors)
		}
	}
}
