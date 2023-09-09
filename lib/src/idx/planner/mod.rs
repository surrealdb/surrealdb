pub(crate) mod executor;
pub(crate) mod iterators;
pub(crate) mod plan;
mod tree;

use crate::ctx::Context;
use crate::dbs::{Iterable, Iterator, Options, Transaction};
use crate::err::Error;
use crate::idx::planner::executor::QueryExecutor;
use crate::idx::planner::plan::{Plan, PlanBuilder};
use crate::idx::planner::tree::{IndexMap, Tree};
use crate::sql::with::With;
use crate::sql::{Cond, Table};
use std::collections::HashMap;

pub(crate) struct QueryPlanner<'a> {
	opt: &'a Options,
	with: &'a Option<With>,
	cond: &'a Option<Cond>,
	/// There is one executor per table
	executors: HashMap<String, QueryExecutor>,
	requires_distinct: bool,
	fallbacks: Vec<String>,
}

impl<'a> QueryPlanner<'a> {
	pub(crate) fn new(opt: &'a Options, with: &'a Option<With>, cond: &'a Option<Cond>) -> Self {
		Self {
			opt,
			with,
			cond,
			executors: HashMap::default(),
			requires_distinct: false,
			fallbacks: vec![],
		}
	}

	pub(crate) async fn add_iterables(
		&mut self,
		ctx: &Context<'_>,
		txn: &Transaction,
		t: Table,
		it: &mut Iterator,
	) -> Result<(), Error> {
		match Tree::build(ctx, self.opt, txn, &t, self.cond).await? {
			Some((node, im)) => {
				Self::detect_range_queries(&im);
				let mut exe = QueryExecutor::new(self.opt, txn, &t, im).await?;
				match PlanBuilder::build(node, self.with)? {
					Plan::SingleIndex(exp, io) => {
						let ir = exe.add_iterator(exp);
						it.ingest(Iterable::Index(t.clone(), ir, io));
						self.executors.insert(t.0.clone(), exe);
					}
					Plan::MultiIndex(v) => {
						for (exp, io) in v {
							let ir = exe.add_iterator(exp);
							it.ingest(Iterable::Index(t.clone(), ir, io));
							self.requires_distinct = true;
						}
						self.executors.insert(t.0.clone(), exe);
					}
					Plan::TableIterator(fallback) => {
						if let Some(fallback) = fallback {
							self.fallbacks.push(fallback);
						}
						self.executors.insert(t.0.clone(), exe);
						it.ingest(Iterable::Table(t));
					}
				}
			}
			None => {
				it.ingest(Iterable::Table(t));
			}
		}
		Ok(())
	}

	fn detect_range_queries(im: &IndexMap) {
		for (_, ios) in im.groups() {
			for (_, io) in ios {}
		}
		todo!()
	}

	pub(crate) fn has_executors(&self) -> bool {
		!self.executors.is_empty()
	}

	pub(crate) fn get_query_executor(&self, tb: &str) -> Option<&QueryExecutor> {
		self.executors.get(tb)
	}

	pub(crate) fn requires_distinct(&self) -> bool {
		self.requires_distinct
	}

	pub(crate) fn fallbacks(&self) -> &Vec<String> {
		&self.fallbacks
	}
}
