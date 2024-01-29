pub(crate) mod executor;
pub(crate) mod iterators;
pub(crate) mod plan;
mod tree;

use crate::ctx::Context;
use crate::dbs::{Iterable, Iterator, Options, Transaction};
use crate::err::Error;
use crate::idx::planner::executor::{IteratorEntry, QueryExecutor};
use crate::idx::planner::plan::{Plan, PlanBuilder};
use crate::idx::planner::tree::Tree;
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
		match Tree::build(ctx, self.opt, txn, &t, self.cond, self.with).await? {
			Some((node, im, with_indexes)) => {
				let mut exe = QueryExecutor::new(ctx, self.opt, txn, &t, im).await?;
				match PlanBuilder::build(node, self.with, with_indexes)? {
					Plan::SingleIndex(exp, io) => {
						if io.require_distinct() {
							self.requires_distinct = true;
						}
						let ir = exe.add_iterator(IteratorEntry::Single(exp, io));
						it.ingest(Iterable::Index(t.clone(), ir));
						self.executors.insert(t.0.clone(), exe);
					}
					Plan::MultiIndex(v) => {
						for (exp, io) in v {
							let ir = exe.add_iterator(IteratorEntry::Single(exp, io));
							it.ingest(Iterable::Index(t.clone(), ir));
							self.requires_distinct = true;
						}
						self.executors.insert(t.0.clone(), exe);
					}
					Plan::SingleIndexMultiExpression(ixn, rq) => {
						let ir =
							exe.add_iterator(IteratorEntry::Range(rq.exps, ixn, rq.from, rq.to));
						it.ingest(Iterable::Index(t.clone(), ir));
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
