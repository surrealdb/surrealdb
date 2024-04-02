pub(crate) mod executor;
pub(crate) mod iterators;
pub(in crate::idx) mod knn;
pub(crate) mod plan;
mod tree;

use crate::ctx::Context;
use crate::dbs::{Iterable, Iterator, Options, Transaction};
use crate::err::Error;
use crate::idx::planner::executor::{
	InnerQueryExecutor, IteratorEntry, IteratorRef, QueryExecutor,
};
use crate::idx::planner::plan::{Plan, PlanBuilder};
use crate::idx::planner::tree::Tree;
use crate::sql::with::With;
use crate::sql::{Cond, Expression, Table, Thing};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

pub(crate) struct QueryPlanner<'a> {
	opt: &'a Options,
	with: &'a Option<With>,
	cond: &'a Option<Cond>,
	/// There is one executor per table
	executors: HashMap<String, QueryExecutor>,
	requires_distinct: bool,
	fallbacks: Vec<String>,
	iteration_workflow: Vec<IterationStage>,
	iteration_index: AtomicU8,
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
			iteration_workflow: Vec::default(),
			iteration_index: AtomicU8::new(0),
		}
	}

	pub(crate) async fn add_iterables(
		&mut self,
		ctx: &Context<'_>,
		txn: &Transaction,
		t: Table,
		it: &mut Iterator,
	) -> Result<(), Error> {
		let mut is_table_iterator = false;
		let mut is_knn = false;
		let t = Arc::new(t);
		match Tree::build(ctx, self.opt, txn, &t, self.cond, self.with).await? {
			Some(tree) => {
				is_knn = is_knn || !tree.knn_expressions.is_empty();
				let mut exe = InnerQueryExecutor::new(
					ctx,
					self.opt,
					txn,
					&t,
					tree.index_map,
					tree.knn_expressions,
				)
				.await?;
				match PlanBuilder::build(tree.root, self.with, tree.with_indexes)? {
					Plan::SingleIndex(exp, io) => {
						if io.require_distinct() {
							self.requires_distinct = true;
						}
						let ir = exe.add_iterator(IteratorEntry::Single(exp, io));
						self.add(t.clone(), Some(ir), exe, it);
					}
					Plan::MultiIndex(non_range_indexes, ranges_indexes) => {
						for (exp, io) in non_range_indexes {
							let ie = IteratorEntry::Single(exp, io);
							let ir = exe.add_iterator(ie);
							it.ingest(Iterable::Index(t.clone(), ir));
						}
						for (ixn, rq) in ranges_indexes {
							let ie = IteratorEntry::Range(rq.exps, ixn, rq.from, rq.to);
							let ir = exe.add_iterator(ie);
							it.ingest(Iterable::Index(t.clone(), ir));
						}
						self.requires_distinct = true;
						self.add(t.clone(), None, exe, it);
					}
					Plan::SingleIndexRange(ixn, rq) => {
						let ir =
							exe.add_iterator(IteratorEntry::Range(rq.exps, ixn, rq.from, rq.to));
						self.add(t.clone(), Some(ir), exe, it);
					}
					Plan::TableIterator(fallback) => {
						if let Some(fallback) = fallback {
							self.fallbacks.push(fallback);
						}
						self.add(t.clone(), None, exe, it);
						it.ingest(Iterable::Table(t));
						is_table_iterator = true;
					}
				}
			}
			None => {
				it.ingest(Iterable::Table(t));
			}
		}
		if is_knn && is_table_iterator {
			self.iteration_workflow = vec![IterationStage::CollectKnn, IterationStage::BuildKnn];
		} else {
			self.iteration_workflow = vec![IterationStage::Iterate(None)];
		}
		Ok(())
	}

	fn add(
		&mut self,
		tb: Arc<Table>,
		irf: Option<IteratorRef>,
		exe: InnerQueryExecutor,
		it: &mut Iterator,
	) {
		self.executors.insert(tb.0.clone(), exe.into());
		if let Some(irf) = irf {
			it.ingest(Iterable::Index(tb, irf));
		}
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

	pub(crate) async fn next_iteration_stage(&self) -> Option<IterationStage> {
		let pos = self.iteration_index.fetch_add(1, Ordering::Relaxed);
		match self.iteration_workflow.get(pos as usize) {
			Some(IterationStage::BuildKnn) => {
				Some(IterationStage::Iterate(Some(self.build_knn_sets().await)))
			}
			is => is.cloned(),
		}
	}

	async fn build_knn_sets(&self) -> KnnSets {
		let mut results = HashMap::with_capacity(self.executors.len());
		for (tb, exe) in &self.executors {
			results.insert(tb.clone(), exe.build_knn_set().await);
		}
		Arc::new(results)
	}
}

pub(crate) type KnnSet = HashMap<Arc<Expression>, HashSet<Arc<Thing>>>;
pub(crate) type KnnSets = Arc<HashMap<String, KnnSet>>;

#[derive(Clone)]
pub(crate) enum IterationStage {
	Iterate(Option<KnnSets>),
	CollectKnn,
	BuildKnn,
}
