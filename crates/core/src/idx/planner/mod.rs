pub mod checker;
pub(crate) mod executor;
pub(crate) mod iterators;
pub(in crate::idx) mod knn;
pub(crate) mod plan;
pub(in crate::idx) mod rewriter;
pub(in crate::idx) mod tree;

use crate::ctx::Context;
use crate::dbs::{Iterable, Iterator, Options, Statement};
use crate::err::Error;
use crate::idx::planner::executor::{InnerQueryExecutor, IteratorEntry, QueryExecutor};
use crate::idx::planner::iterators::IteratorRef;
use crate::idx::planner::knn::KnnBruteForceResults;
use crate::idx::planner::plan::{Plan, PlanBuilder};
use crate::idx::planner::tree::Tree;
use crate::sql::with::With;
use crate::sql::{order::Ordering, Cond, Fields, Groups, Table};
use reblessive::tree::Stk;
use std::collections::HashMap;
use std::sync::atomic::{self, AtomicU8};

/// The goal of this structure is to cache parameters so they can be easily passed
/// from one function to the other, so we don't pass too many arguments.
/// It also caches evaluated fields (like is_keys_only)
pub(crate) struct StatementContext<'a> {
	pub(crate) ctx: &'a Context,
	pub(crate) opt: &'a Options,
	pub(crate) ns: &'a str,
	pub(crate) db: &'a str,
	pub(crate) stm: &'a Statement<'a>,
	pub(crate) fields: Option<&'a Fields>,
	pub(crate) with: Option<&'a With>,
	pub(crate) order: Option<&'a Ordering>,
	pub(crate) cond: Option<&'a Cond>,
	pub(crate) group: Option<&'a Groups>,
}

impl<'a> StatementContext<'a> {
	pub(crate) fn new(
		ctx: &'a Context,
		opt: &'a Options,
		stm: &'a Statement<'a>,
	) -> Result<Self, Error> {
		Ok(Self {
			ctx,
			opt,
			stm,
			ns: opt.ns()?,
			db: opt.db()?,
			fields: stm.expr(),
			with: stm.with(),
			order: stm.order(),
			cond: stm.cond(),
			group: stm.group(),
		})
	}

	pub(crate) async fn is_keys_only(&self, tb: &str) -> Result<bool, Error> {
		// If there is a WHERE clause then
		// we need to fetch and process
		// record content values too.
		if self.cond.is_some() {
			return Ok(false);
		}
		// If there is a GROUP BY clause,
		// and it is not GROUP ALL, then we
		// need to process record values.
		if let Some(g) = self.group {
			if !g.is_empty() {
				return Ok(false);
			}
		}
		// If there is a ORDER BY clause,
		// with specific fields, then we
		// need to process record values.
		if let Some(p) = self.order {
			match p {
				Ordering::Random => {}
				Ordering::Order(x) => {
					if !x.0.is_empty() {
						return Ok(false);
					}
				}
			}
		}
		// If there are any field expressions
		// defined which are not count() then
		// we need to process record values.
		if let Some(fields) = self.fields {
			if !fields.is_count_all_only() {
				return Ok(false);
			}
		}
		// If there are specific permissions
		// defined on the table, then we need
		// to process record values.
		if self.opt.perms {
			// Get the table for this planner
			match self.ctx.tx().get_tb(self.ns, self.db, tb).await {
				Ok(table) => {
					let perms = self.stm.permissions(&table, false);
					if perms.is_specific() {
						return Ok(false);
					}
				}
				Err(Error::TbNotFound {
					..
				}) => {
					// We can safely ignore this error,
					// as it just means that there is no
					// table and no permissions defined.
				}
				Err(e) => return Err(e),
			}
		}
		// Otherwise we can iterate over keys
		Ok(true)
	}
}

pub(crate) struct QueryPlanner {
	/// There is one executor per table
	executors: HashMap<String, QueryExecutor>,
	requires_distinct: bool,
	fallbacks: Vec<String>,
	iteration_workflow: Vec<IterationStage>,
	iteration_index: AtomicU8,
	orders: Vec<IteratorRef>,
}

impl QueryPlanner {
	pub(crate) fn new() -> Self {
		Self {
			executors: HashMap::default(),
			requires_distinct: false,
			fallbacks: vec![],
			iteration_workflow: Vec::default(),
			iteration_index: AtomicU8::new(0),
			orders: vec![],
		}
	}

	pub(crate) async fn add_iterables(
		&mut self,
		stk: &mut Stk,
		ctx: &StatementContext<'_>,
		t: Table,
		it: &mut Iterator,
	) -> Result<(), Error> {
		let mut is_table_iterator = false;

		let tree = Tree::build(stk, ctx, &t).await?;

		let is_knn = !tree.knn_expressions.is_empty();
		let mut exe = InnerQueryExecutor::new(
			stk,
			ctx.ctx,
			ctx.opt,
			&t,
			tree.index_map.options,
			tree.knn_expressions,
			tree.knn_brute_force_expressions,
			tree.knn_condition,
		)
		.await?;
		match PlanBuilder::build(
			&t,
			tree.root,
			ctx,
			tree.with_indexes,
			tree.index_map.compound_indexes,
			tree.index_map.order_limit,
			tree.all_and_groups,
			tree.all_and,
			tree.all_expressions_with_index,
		)
		.await?
		{
			Plan::SingleIndex(exp, io) => {
				if io.require_distinct() {
					self.requires_distinct = true;
				}
				let is_order = exp.is_none();
				let ir = exe.add_iterator(IteratorEntry::Single(exp, io));
				self.add(t.clone(), Some(ir), exe, it);
				if is_order {
					self.orders.push(ir);
				}
			}
			Plan::MultiIndex(non_range_indexes, ranges_indexes) => {
				for (exp, io) in non_range_indexes {
					let ie = IteratorEntry::Single(Some(exp), io);
					let ir = exe.add_iterator(ie);
					it.ingest(Iterable::Index(t.clone(), ir));
				}
				for (ixr, rq) in ranges_indexes {
					let ie = IteratorEntry::Range(rq.exps, ixr, rq.from, rq.to);
					let ir = exe.add_iterator(ie);
					it.ingest(Iterable::Index(t.clone(), ir));
				}
				self.requires_distinct = true;
				self.add(t.clone(), None, exe, it);
			}
			Plan::SingleIndexRange(ixn, rq) => {
				let ir = exe.add_iterator(IteratorEntry::Range(rq.exps, ixn, rq.from, rq.to));
				self.add(t.clone(), Some(ir), exe, it);
			}
			Plan::TableIterator(reason, keys_only) => {
				if let Some(reason) = reason {
					self.fallbacks.push(reason);
				}
				self.add(t.clone(), None, exe, it);
				it.ingest(Iterable::Table(t, keys_only));
				is_table_iterator = true;
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
		tb: Table,
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

	#[cfg(not(target_arch = "wasm32"))]
	pub(crate) fn is_order(&self, irf: &IteratorRef) -> bool {
		self.orders.contains(irf)
	}

	pub(crate) async fn next_iteration_stage(&self) -> Option<IterationStage> {
		let pos = self.iteration_index.fetch_add(1, atomic::Ordering::Relaxed);
		match self.iteration_workflow.get(pos as usize) {
			Some(IterationStage::BuildKnn) => {
				Some(IterationStage::Iterate(Some(self.build_bruteforce_knn_results().await)))
			}
			is => is.cloned(),
		}
	}

	async fn build_bruteforce_knn_results(&self) -> KnnBruteForceResults {
		let mut results = HashMap::with_capacity(self.executors.len());
		for (tb, exe) in &self.executors {
			results.insert(tb.clone(), exe.build_bruteforce_knn_result().await);
		}
		results.into()
	}
}

#[derive(Clone)]
pub(crate) enum IterationStage {
	Iterate(Option<KnnBruteForceResults>),
	CollectKnn,
	BuildKnn,
}
