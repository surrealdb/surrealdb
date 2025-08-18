pub mod checker;
pub(crate) mod executor;
pub(crate) mod iterators;
pub(in crate::idx) mod knn;
pub(crate) mod plan;
pub(in crate::idx) mod rewriter;
pub(in crate::idx) mod tree;

use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{self, AtomicU8};

use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::DatabaseDefinition;
use crate::ctx::Context;
use crate::dbs::{Iterable, Iterator, Options, Statement};
use crate::expr::order::Ordering;
use crate::expr::with::With;
use crate::expr::{Cond, Fields, Groups, Ident};
use crate::idx::planner::executor::{InnerQueryExecutor, IteratorEntry, QueryExecutor};
use crate::idx::planner::iterators::IteratorRef;
use crate::idx::planner::knn::KnnBruteForceResults;
use crate::idx::planner::plan::{Plan, PlanBuilder, PlanBuilderParameters};
use crate::idx::planner::tree::Tree;

/// The goal of this structure is to cache parameters so they can be easily
/// passed from one function to the other, so we don't pass too many arguments.
/// It also caches evaluated fields (like is_keys_only)
pub(crate) struct StatementContext<'a> {
	pub(crate) ctx: &'a Context,
	pub(crate) opt: &'a Options,
	pub(crate) stm: &'a Statement<'a>,
	pub(crate) fields: Option<&'a Fields>,
	pub(crate) with: Option<&'a With>,
	pub(crate) order: Option<&'a Ordering>,
	pub(crate) cond: Option<&'a Cond>,
	pub(crate) group: Option<&'a Groups>,
	is_perm: bool,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum RecordStrategy {
	Count,
	KeysOnly,
	KeysAndValues,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum ScanDirection {
	Forward,
	#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
	Backward,
}

impl Display for ScanDirection {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			ScanDirection::Forward => f.write_str("forward"),
			#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
			ScanDirection::Backward => f.write_str("backward"),
		}
	}
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum GrantedPermission {
	None,
	Full,
	Specific,
}

impl<'a> StatementContext<'a> {
	pub(crate) fn new(ctx: &'a Context, opt: &'a Options, stm: &'a Statement<'a>) -> Result<Self> {
		let is_perm = opt.check_perms(stm.into())?;
		Ok(Self {
			ctx,
			opt,
			stm,
			fields: stm.expr(),
			with: stm.with(),
			order: stm.order(),
			cond: stm.cond(),
			group: stm.group(),
			is_perm,
		})
	}

	pub(crate) async fn check_table_permission(&self, tb: &str) -> Result<GrantedPermission> {
		if !self.is_perm {
			return Ok(GrantedPermission::Full);
		}
		let (ns, db) = self.ctx.get_ns_db_ids(self.opt).await?;
		// Get the table for this planner
		match self.ctx.tx().get_tb(ns, db, tb).await? {
			Some(table) => {
				// TODO(tobiemh): we should really
				// not even get here if the table
				// permissions are NONE, because
				// there is no point in processing
				// a table which we can't access.
				let perms = self.stm.permissions(&table, self.stm.is_create());
				// If permissions are specific, we
				// need to fetch the record content.
				if perms.is_specific() {
					return Ok(GrantedPermission::Specific);
				}
				// If permissions are NONE, we also
				// need to fetch the record content.
				if perms.is_none() {
					return Ok(GrantedPermission::None);
				}
			}
			None => {
				// Fall through to full permissions.
			}
		}
		Ok(GrantedPermission::Full)
	}

	/// Decide whether to fetch just record keys, keys and values, or only a
	/// COUNT.
	///
	/// This function evaluates the statement shape (UPDATE/DELETE/etc.),
	/// WHERE/GROUP/ORDER clauses, selected fields, and table permissions to
	/// select the most efficient record retrieval strategy:
	/// - KeysAndValues: required when values must be read (e.g., UPDATE/DELETE; WHERE not fully
	///   covered by indexes; GROUP BY with fields; ORDER BY with fields; non-count projections; or
	///   when table permissions are Specific).
	/// - Count: when we only need COUNT(*) and GROUP ALL.
	/// - KeysOnly: when none of the above apply, allowing index-only iteration.
	pub(crate) fn check_record_strategy(
		&self,
		all_expressions_with_index: bool,
		granted_permission: GrantedPermission,
	) -> Result<RecordStrategy> {
		// Update / Upsert / Delete need to retrieve the values:
		// 1. So they can be removed from any existing index
		// 2. To hydrate live queries
		if matches!(self.stm, Statement::Update(_) | Statement::Upsert(_) | Statement::Delete(_)) {
			return Ok(RecordStrategy::KeysAndValues);
		}
		// If there is an index backs a WHERE clause but not all expressions,
		// then we need to fetch and process
		// record content values too.
		if !all_expressions_with_index && self.cond.is_some() {
			return Ok(RecordStrategy::KeysAndValues);
		}

		// If there is a GROUP BY clause,
		// and it is not GROUP ALL, then we
		// need to process record values.
		let is_group_all = if let Some(g) = self.group {
			if !g.is_empty() {
				return Ok(RecordStrategy::KeysAndValues);
			}
			true
		} else {
			false
		};

		// If there is an ORDER BY clause,
		// with specific fields, then we
		// need to process record values.
		if let Some(p) = self.order {
			match p {
				Ordering::Random => {}
				Ordering::Order(x) => {
					if !x.is_empty() {
						return Ok(RecordStrategy::KeysAndValues);
					}
				}
			}
		}

		// If there are any field expressions
		// defined which are not count() then
		// we need to process record values.
		let is_count_all = if let Some(fields) = self.fields {
			if !fields.is_count_all_only() {
				return Ok(RecordStrategy::KeysAndValues);
			}
			true
		} else {
			false
		};

		// If there are specific permissions
		// defined on the table, then we need
		// to process record values.
		if matches!(granted_permission, GrantedPermission::Specific) {
			return Ok(RecordStrategy::KeysAndValues);
		}

		// We just want to count
		if is_count_all && is_group_all {
			return Ok(RecordStrategy::Count);
		}
		// Otherwise we can iterate over keys only
		Ok(RecordStrategy::KeysOnly)
	}

	/// Determines the scan direction.
	/// This is used for Table and Range iterators.
	/// The direction is reversed if the first element of order is ID
	/// descending. Typically: `ORDER BY id DESC`
	/// Determine forward/backward scan direction for table/range iterators.
	///
	/// On backends that support reverse scans (e.g., RocksDB/TiKV), we reverse
	/// the direction when the first ORDER BY is `id DESC`. Otherwise, we
	/// default to forward.
	pub(crate) fn check_scan_direction(&self) -> ScanDirection {
		#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
		if let Some(Ordering::Order(o)) = self.order {
			if let Some(o) = o.first() {
				if !o.direction && o.value.is_id() {
					return ScanDirection::Backward;
				}
			}
		}
		ScanDirection::Forward
	}
}

pub(crate) struct QueryPlanner {
	/// There is one executor per table
	executors: HashMap<String, QueryExecutor>,
	requires_distinct: bool,
	fallbacks: Vec<String>,
	iteration_workflow: Vec<IterationStage>,
	iteration_index: AtomicU8,
	ordering_indexes: Vec<IteratorRef>,
	granted_permissions: HashMap<String, GrantedPermission>,
	any_specific_permission: bool,
}

impl QueryPlanner {
	pub(crate) fn new() -> Self {
		Self {
			executors: HashMap::default(),
			requires_distinct: false,
			fallbacks: vec![],
			iteration_workflow: Vec::default(),
			iteration_index: AtomicU8::new(0),
			ordering_indexes: vec![],
			granted_permissions: HashMap::default(),
			any_specific_permission: false,
		}
	}

	/// Check the table permissions and cache the result.
	/// Keep track of any specific permission.
	pub(crate) async fn check_table_permission(
		&mut self,
		ctx: &StatementContext<'_>,
		tb: &str,
	) -> Result<GrantedPermission> {
		if ctx.is_perm {
			if let Some(p) = self.granted_permissions.get(tb) {
				return Ok(*p);
			}
			let p = ctx.check_table_permission(tb).await?;
			self.granted_permissions.insert(tb.to_string(), p);
			if matches!(p, GrantedPermission::Specific) {
				self.any_specific_permission = true;
			}
			return Ok(p);
		}
		Ok(GrantedPermission::Full)
	}

	pub(crate) async fn add_iterables(
		&mut self,
		db: &DatabaseDefinition,
		stk: &mut Stk,
		ctx: &StatementContext<'_>,
		t: Ident,
		gp: GrantedPermission,
		it: &mut Iterator,
	) -> Result<()> {
		let mut is_table_iterator = false;

		let tree = Tree::build(stk, ctx, &t).await?;

		let is_knn = !tree.knn_expressions.is_empty();
		let mut exe = InnerQueryExecutor::new(
			db,
			stk,
			ctx.ctx,
			ctx.opt,
			&t,
			tree.index_map.options,
			tree.knn_brute_force_expressions,
			tree.knn_condition,
		)
		.await?;
		let p = PlanBuilderParameters {
			root: tree.root,
			gp,
			compound_indexes: tree.index_map.compound_indexes,
			order_limit: tree.index_map.order_limit,
			with_indexes: tree.with_indexes,
			all_and: tree.all_and,
			all_expressions_with_index: tree.all_expressions_with_index,
			all_and_groups: tree.all_and_groups,
			reverse_scan: ctx.ctx.tx().reverse_scan(),
		};
		match PlanBuilder::build(ctx, p).await? {
			Plan::SingleIndex(exp, io, rs) => {
				if io.require_distinct() {
					self.requires_distinct = true;
				}
				let is_order = io.is_order();
				let ir = exe.add_iterator(IteratorEntry::Single(exp, io));
				self.add(t.clone(), Some(ir), exe, it, rs);
				if is_order {
					self.ordering_indexes.push(ir);
				}
			}
			Plan::MultiIndex(non_range_indexes, ranges_indexes, rs) => {
				for (exp, io) in non_range_indexes {
					let ie = IteratorEntry::Single(Some(exp), io);
					let ir = exe.add_iterator(ie);
					it.ingest(Iterable::Index(t.clone(), ir, rs));
				}
				for (ixr, rq) in ranges_indexes {
					let ie =
						IteratorEntry::Range(rq.exps, ixr, rq.from, rq.to, ScanDirection::Forward);
					let ir = exe.add_iterator(ie);
					it.ingest(Iterable::Index(t.clone(), ir, rs));
				}
				self.requires_distinct = true;
				self.add(t.clone(), None, exe, it, rs);
			}
			Plan::SingleIndexRange(ixn, rq, keys_only, sc, is_order) => {
				let ir = exe.add_iterator(IteratorEntry::Range(rq.exps, ixn, rq.from, rq.to, sc));
				if is_order {
					self.ordering_indexes.push(ir);
				}
				self.add(t.clone(), Some(ir), exe, it, keys_only);
			}
			Plan::TableIterator(reason, rs, sc) => {
				if let Some(reason) = reason {
					self.fallbacks.push(reason);
				}
				self.add(t.clone(), None, exe, it, rs);
				it.ingest(Iterable::Table(t, rs, sc));
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
		tb: Ident,
		irf: Option<IteratorRef>,
		exe: InnerQueryExecutor,
		it: &mut Iterator,
		rs: RecordStrategy,
	) {
		self.executors.insert(tb.clone().into_string(), exe.into());
		if let Some(irf) = irf {
			it.ingest(Iterable::Index(tb, irf, rs));
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

	pub(crate) fn is_order(&self, irf: &IteratorRef) -> bool {
		self.ordering_indexes.contains(irf)
	}

	pub(crate) fn is_any_specific_permission(&self) -> bool {
		self.any_specific_permission
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
