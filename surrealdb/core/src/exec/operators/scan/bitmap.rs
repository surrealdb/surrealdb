//! Bitmap candidate scan operators (roaring bitmap index fusion).
//!
//! These operators make [`roaring::RoaringTreemap`]s over a table's shared
//! doc-ID space the planner's candidate currency for AND/OR/AND-NOT
//! composition of index-backed predicates (issue #547):
//!
//! - [`BitmapNode`] — one node of the bitmap expression tree. Leaves drain a b-tree index range
//!   (reading the doc-ID appended to each entry value) or expose a full-text query's merged posting
//!   bitmap; inner nodes apply set algebra (intersection smallest-first, union, difference).
//! - [`BitmapResolve`] — the physical operator that owns the tree: it evaluates the root bitmap,
//!   batch-resolves surviving doc-IDs to record IDs through [`TableDocIds::get_record_ids_batch`],
//!   and feeds the standard fetch pipeline (table permissions per record, computed fields,
//!   field-level permissions). Rows are emitted in doc-ID order, which is unspecified relative to
//!   any field — the planner only chooses this plan when no index-covered ORDER BY is available, so
//!   any ORDER BY is enforced by a downstream Sort.
//!
//! [`BitmapNode`]s implement [`ExecOperator`] so EXPLAIN renders the bitmap
//! expression as a plan subtree (`BitmapAnd` / `BitmapOr` / `BitmapAndNot` /
//! `BitmapIndexScan` / `BitmapFullTextScan`), but they are not directly
//! executable: only the owning [`BitmapResolve`] evaluates them, inside its
//! own execution, via [`BitmapNode::build_bitmap`].
//!
//! Snapshot consistency is inherited from the query's [`Transaction`]: every
//! branch drains index entries through it, so all branches observe one
//! snapshot. Row-level permissions are never bitmap-evaluated — they apply
//! per record at the resolve/fetch stage, exactly as for other index scans.

use std::collections::HashSet;
use std::sync::Arc;

use common::future::stream::{self, Yielder};
use reblessive::TreeStack;
use roaring::RoaringTreemap;

use super::common::fetch_and_filter_records_batch;
use super::pipeline::{ScanPipeline, build_field_state};
use super::resolved::ResolvedTableContext;
use crate::catalog::{DatabaseId, Index, NamespaceId};
use crate::err::Error;
use crate::exec::index::access_path::{BTreeAccess, IndexRef};
use crate::exec::index::iterator::btree::{
	INDEX_BATCH_SIZE, bitmap_scan_range, decode_entry_doc_ids,
};
use crate::exec::permission::{
	PhysicalPermission, convert_permission_to_physical_runtime, should_check_perms,
	validate_record_user_access,
};
use crate::exec::{
	AccessMode, BoxFut, ContextLevel, ExecOperator, ExecutionContext, FlowResult, OperatorMetrics,
	ValueBatch, ValueBatchStream, monitor_stream,
};
use crate::expr::operator::MatchesOperator;
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::iam::Action;
use crate::idx::IndexKeyBase;
use crate::idx::docids::TableDocIds;
use crate::idx::ft::fulltext::FullTextIndex;
use crate::kvs::util::scan;
use crate::kvs::{CachePolicy, Transaction};
use crate::val::{RecordId, TableName};

/// Number of doc-IDs resolved and fetched per output batch.
const RESOLVE_BATCH_SIZE: usize = 1000;

/// Reject execution under a version (time-travel) context.
///
/// Doc-ID mappings and index entry bitmaps are not versioned, so a bitmap
/// plan can only describe current state. The planner gates plan selection on
/// both the statement's VERSION clause and the enclosing version context;
/// this is the defense-in-depth backstop should a versioned execution reach
/// a bitmap operator anyway.
fn reject_versioned_execution(ctx: &ExecutionContext) -> std::result::Result<(), ControlFlow> {
	if ctx.version_stamp().is_some() {
		return Err(ControlFlow::Err(anyhow::anyhow!(
			"Bitmap index plans do not support VERSION queries"
		)));
	}
	Ok(())
}

/// One node of a bitmap candidate expression tree.
///
/// Constructed by the planner (see `AccessPath::BitmapFusion`) and evaluated
/// by the owning [`BitmapResolve`]. Implements [`ExecOperator`] purely so the
/// tree appears in EXPLAIN output — `execute()` is unreachable in a
/// well-formed plan and returns an error.
#[derive(Debug)]
pub(crate) struct BitmapNode {
	kind: BitmapNodeKind,
	/// The same child node Arcs coerced to `dyn ExecOperator`, so
	/// [`ExecOperator::children`] can hand out references for EXPLAIN.
	children_dyn: Vec<Arc<dyn ExecOperator>>,
	/// Node cardinality (bitmap length) recorded during evaluation for
	/// EXPLAIN ANALYZE.
	metrics: Arc<OperatorMetrics>,
}

#[derive(Debug)]
pub(crate) enum BitmapNodeKind {
	/// Drain a b-tree index range, collecting the doc-ID appended to each
	/// entry value (see [`crate::key::index::IndexEntryValue`]).
	BTree {
		index_ref: IndexRef,
		access: BTreeAccess,
	},
	/// A full-text query's merged posting bitmap (no scoring — BM25 runs
	/// later, over surviving documents only, via `search::score()`).
	FullText {
		index_ref: IndexRef,
		query: String,
		operator: MatchesOperator,
	},
	/// Intersection of all children (applied smallest-first).
	And {
		children: Vec<Arc<BitmapNode>>,
	},
	/// Union of all children.
	Or {
		children: Vec<Arc<BitmapNode>>,
	},
	/// `base AND NOT subtract` — the only supported form of negation.
	AndNot {
		base: Arc<BitmapNode>,
		subtract: Arc<BitmapNode>,
	},
}

/// Result of evaluating one branch.
///
/// `Overflow` means the branch abandoned its drain because it exceeded the
/// per-branch entry budget ([`crate::cnf::BITMAP_BRANCH_BUDGET`]). An AND
/// simply drops such a branch — the full WHERE clause is kept as a residual
/// filter above [`BitmapResolve`], so dropping a conjunct only widens the
/// candidate set. A branch that cannot be dropped (a union member, an AND-NOT
/// base, or the plan's anchor) is evaluated without a budget and never
/// overflows.
enum BranchBitmap {
	Ready(RoaringTreemap),
	Overflow,
}

impl BitmapNode {
	pub(crate) fn btree(index_ref: IndexRef, access: BTreeAccess) -> Arc<Self> {
		Arc::new(Self {
			kind: BitmapNodeKind::BTree {
				index_ref,
				access,
			},
			children_dyn: Vec::new(),
			metrics: Arc::new(OperatorMetrics::new()),
		})
	}

	pub(crate) fn fulltext(
		index_ref: IndexRef,
		query: String,
		operator: MatchesOperator,
	) -> Arc<Self> {
		Arc::new(Self {
			kind: BitmapNodeKind::FullText {
				index_ref,
				query,
				operator,
			},
			children_dyn: Vec::new(),
			metrics: Arc::new(OperatorMetrics::new()),
		})
	}

	pub(crate) fn and(children: Vec<Arc<BitmapNode>>) -> Arc<Self> {
		let children_dyn =
			children.iter().map(|c| Arc::clone(c) as Arc<dyn ExecOperator>).collect();
		Arc::new(Self {
			kind: BitmapNodeKind::And {
				children,
			},
			children_dyn,
			metrics: Arc::new(OperatorMetrics::new()),
		})
	}

	pub(crate) fn or(children: Vec<Arc<BitmapNode>>) -> Arc<Self> {
		let children_dyn =
			children.iter().map(|c| Arc::clone(c) as Arc<dyn ExecOperator>).collect();
		Arc::new(Self {
			kind: BitmapNodeKind::Or {
				children,
			},
			children_dyn,
			metrics: Arc::new(OperatorMetrics::new()),
		})
	}

	pub(crate) fn and_not(base: Arc<BitmapNode>, subtract: Arc<BitmapNode>) -> Arc<Self> {
		let children_dyn = vec![
			Arc::clone(&base) as Arc<dyn ExecOperator>,
			Arc::clone(&subtract) as Arc<dyn ExecOperator>,
		];
		Arc::new(Self {
			kind: BitmapNodeKind::AndNot {
				base,
				subtract,
			},
			children_dyn,
			metrics: Arc::new(OperatorMetrics::new()),
		})
	}

	/// Evaluate this node into a candidate bitmap.
	///
	/// `anchored` marks a node whose bitmap is required for the plan to
	/// produce rows at all: the budget is disabled for it (and for every
	/// descendant that is itself non-droppable), so an anchored node never
	/// returns [`BranchBitmap::Overflow`].
	fn build_bitmap<'a>(
		&'a self,
		bctx: &'a BitmapBuildContext<'a>,
		anchored: bool,
	) -> BoxFut<'a, std::result::Result<BranchBitmap, ControlFlow>> {
		Box::pin(async move {
			let result = match &self.kind {
				BitmapNodeKind::BTree {
					index_ref,
					access,
				} => self.build_btree_bitmap(bctx, index_ref, access, anchored).await?,
				BitmapNodeKind::FullText {
					index_ref,
					query,
					operator,
				} => {
					// Posting lists are already materialized bitmaps; no budget.
					BranchBitmap::Ready(
						self.build_fulltext_bitmap(bctx, index_ref, query, operator).await?,
					)
				}
				BitmapNodeKind::And {
					children,
				} => {
					// The first child anchors the intersection: it is drained
					// without a budget so the AND always has a base. Later
					// children that overflow are dropped — the residual WHERE
					// filter enforces their predicate per surviving row.
					let mut acc: Option<RoaringTreemap> = None;
					for (i, child) in children.iter().enumerate() {
						match child.build_bitmap(bctx, anchored && i == 0).await? {
							BranchBitmap::Ready(bitmap) => {
								acc = Some(match acc {
									None => bitmap,
									Some(mut acc) => {
										// Intersect smallest-first.
										if bitmap.len() < acc.len() {
											let mut bitmap = bitmap;
											bitmap &= &acc;
											bitmap
										} else {
											acc &= &bitmap;
											acc
										}
									}
								});
								// Early exit: an empty intersection stays empty.
								if acc.as_ref().is_some_and(|a| a.is_empty()) {
									break;
								}
							}
							BranchBitmap::Overflow => continue,
						}
					}
					match acc {
						Some(acc) => BranchBitmap::Ready(acc),
						// Every child overflowed; only possible unanchored.
						None => BranchBitmap::Overflow,
					}
				}
				BitmapNodeKind::Or {
					children,
				} => {
					// A union missing a member would silently drop rows, so
					// every child inherits this node's anchoring; any child
					// overflow makes the whole union overflow.
					let mut acc = RoaringTreemap::new();
					let mut overflow = false;
					for child in children {
						match child.build_bitmap(bctx, anchored).await? {
							BranchBitmap::Ready(bitmap) => acc |= bitmap,
							BranchBitmap::Overflow => {
								overflow = true;
								break;
							}
						}
					}
					if overflow {
						BranchBitmap::Overflow
					} else {
						BranchBitmap::Ready(acc)
					}
				}
				BitmapNodeKind::AndNot {
					base,
					subtract,
				} => {
					match base.build_bitmap(bctx, anchored).await? {
						BranchBitmap::Ready(mut acc) => {
							// A dropped subtraction only widens the candidate
							// set (the residual `NOT ...` filter still
							// applies), so the subtract side is never anchored.
							if !acc.is_empty()
								&& let BranchBitmap::Ready(sub) =
									subtract.build_bitmap(bctx, false).await?
							{
								acc -= sub;
							}
							BranchBitmap::Ready(acc)
						}
						BranchBitmap::Overflow => BranchBitmap::Overflow,
					}
				}
			};
			if let BranchBitmap::Ready(bitmap) = &result {
				// Candidate cardinality for EXPLAIN ANALYZE.
				self.metrics.add_output_rows(bitmap.len());
			}
			Ok(result)
		})
	}

	/// Evaluate the tree to its exact cardinality, for index-only COUNT
	/// plans (`IndexCountScan`).
	///
	/// No branch budget applies: the planner admitted this tree only because
	/// every branch bitmap exactly equals its predicate's truth set, so no
	/// branch may be dropped and the cardinality is the count — with zero
	/// record fetches.
	pub(crate) async fn build_exact_cardinality(
		&self,
		ctx: &ExecutionContext,
		table: &TableName,
	) -> std::result::Result<u64, ControlFlow> {
		// The planner never emits a bitmap count for versioned queries; if a
		// version context reaches this operator anyway, fail loudly rather
		// than report a current-state cardinality for a historical query.
		reject_versioned_execution(ctx)?;
		let db_ctx = ctx.database().context("Bitmap cardinality requires database context")?;
		let ns = db_ctx.ns_ctx.ns.namespace_id;
		let db = db_ctx.db.database_id;
		let txn = ctx.txn();
		let doc_ids = TableDocIds::new(ns, db, table.clone());
		let bctx = BitmapBuildContext {
			ctx,
			txn: txn.as_ref(),
			ns,
			db,
			table,
			doc_ids: &doc_ids,
			// `0` disables the drained-entry budget: exact mode.
			budget: 0,
		};
		match self.build_bitmap(&bctx, true).await? {
			BranchBitmap::Ready(bitmap) => Ok(bitmap.len()),
			// Unreachable with the budget disabled.
			BranchBitmap::Overflow => Err(ControlFlow::Err(anyhow::anyhow!(
				"An exact bitmap plan overflowed its branch budget"
			))),
		}
	}

	/// Drain a b-tree index range into a doc-ID bitmap.
	async fn build_btree_bitmap(
		&self,
		bctx: &BitmapBuildContext<'_>,
		index_ref: &IndexRef,
		access: &BTreeAccess,
		anchored: bool,
	) -> std::result::Result<BranchBitmap, ControlFlow> {
		let ix = index_ref.definition();
		let mut range = bitmap_scan_range(bctx.ns, bctx.db, ix, access)
			.context("Failed to compute bitmap scan range")?;
		let mut docs = RoaringTreemap::new();
		let mut missing: Vec<RecordId> = Vec::new();
		let mut drained = 0usize;
		loop {
			if bctx.ctx.cancellation().is_cancelled() {
				return Err(ControlFlow::Err(anyhow::anyhow!(Error::QueryCancelled)));
			}
			let res = scan(&mut range, bctx.txn, INDEX_BATCH_SIZE)
				.await
				.context("Failed to scan index entries")?;
			if res.is_empty() {
				break;
			}
			drained += decode_entry_doc_ids(res, &mut docs, &mut missing)
				.context("Failed to decode index entry doc-IDs")?;
			if !anchored && bctx.budget > 0 && drained > bctx.budget {
				return Ok(BranchBitmap::Overflow);
			}
		}
		// Entries written before the index's doc-ID format (e.g. by an older
		// binary during a rolling upgrade) carry no doc-ID; resolve them
		// through the shared `!di` mapping instead.
		for rid in missing {
			match bctx
				.doc_ids
				.get_doc_id(bctx.txn, &rid.key)
				.await
				.context("Failed to resolve a doc-ID for an index entry")?
			{
				Some(doc_id) => {
					docs.insert(doc_id);
				}
				None => {
					return Err(ControlFlow::Err(anyhow::anyhow!(
						"Index '{}' on table '{}' contains an entry without a doc-ID mapping; \
						 run `REBUILD INDEX {} ON {}` to repair it",
						ix.name,
						ix.table_name,
						ix.name,
						ix.table_name,
					)));
				}
			}
		}
		Ok(BranchBitmap::Ready(docs))
	}

	/// Expose a full-text query's merged posting bitmap.
	async fn build_fulltext_bitmap(
		&self,
		bctx: &BitmapBuildContext<'_>,
		index_ref: &IndexRef,
		query: &str,
		operator: &MatchesOperator,
	) -> std::result::Result<RoaringTreemap, ControlFlow> {
		let index_def = index_ref.definition();
		index_def.ensure_current_format()?;
		let ft_params = match &index_def.index {
			Index::FullText(params) => params,
			_ => {
				return Err(ControlFlow::Err(anyhow::anyhow!(
					"Index '{}' is not a full-text index",
					index_def.name
				)));
			}
		};
		let root = bctx.ctx.root();
		let frozen_ctx = &root.ctx;
		let opt =
			root.options.as_ref().context("Bitmap full-text scan requires Options context")?;
		let ikb = IndexKeyBase::new(bctx.ns, bctx.db, bctx.table.clone(), index_def.index_id);
		let fti = FullTextIndex::new(
			frozen_ctx.get_index_stores(),
			bctx.txn,
			ikb,
			ft_params,
			&frozen_ctx.config.file_allowlist,
		)
		.await
		.context("Failed to open full-text index")?;
		let query_terms = {
			let mut stack = TreeStack::new();
			stack
				.enter(|stk| fti.extract_querying_terms(stk, frozen_ctx, opt, query.to_owned()))
				.finish()
				.await
				.context("Failed to extract query terms")?
		};
		if query_terms.is_empty() {
			return Ok(RoaringTreemap::new());
		}
		Ok(FullTextIndex::merged_postings(&query_terms, operator.operator).unwrap_or_default())
	}
}

impl ExecOperator for BitmapNode {
	fn name(&self) -> &'static str {
		match &self.kind {
			BitmapNodeKind::BTree {
				..
			} => "BitmapIndexScan",
			BitmapNodeKind::FullText {
				..
			} => "BitmapFullTextScan",
			BitmapNodeKind::And {
				..
			} => "BitmapAnd",
			BitmapNodeKind::Or {
				..
			} => "BitmapOr",
			BitmapNodeKind::AndNot {
				..
			} => "BitmapAndNot",
		}
	}

	fn attrs(&self) -> Vec<(String, String)> {
		match &self.kind {
			BitmapNodeKind::BTree {
				index_ref,
				access,
			} => vec![
				("index".to_string(), index_ref.name.to_string()),
				("access".to_string(), access.describe()),
			],
			BitmapNodeKind::FullText {
				index_ref,
				query,
				..
			} => vec![
				("index".to_string(), index_ref.name.to_string()),
				("query".to_string(), query.clone()),
			],
			_ => vec![],
		}
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		self.children_dyn.iter().collect()
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn execute(&self, _ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		// Bitmap nodes are evaluated by their owning `BitmapResolve` via
		// `build_bitmap`; they appear as plan children for EXPLAIN only.
		Err(ControlFlow::Err(anyhow::anyhow!(
			"{} is not directly executable; it is evaluated by its BitmapResolve parent",
			self.name()
		)))
	}
}

/// Shared inputs for one evaluation of a bitmap expression tree.
struct BitmapBuildContext<'a> {
	ctx: &'a ExecutionContext,
	txn: &'a Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &'a TableName,
	doc_ids: &'a TableDocIds,
	/// Per-branch drained-entry budget; `0` disables it.
	budget: usize,
}

/// Physical operator that evaluates a bitmap candidate expression tree and
/// resolves the surviving doc-IDs into records.
///
/// See the module docs for the full contract. The planner keeps the whole
/// WHERE clause as a residual `Filter` above this operator, so the candidate
/// set only needs to be a superset of the matching rows *per dropped branch*
/// — every branch that completes contributes exact predicate semantics.
#[derive(Debug)]
pub struct BitmapResolve {
	/// Table whose records are resolved.
	pub table_name: TableName,
	/// Root of the bitmap expression tree (typed handle).
	root: Arc<BitmapNode>,
	/// The same root coerced for `children()` / EXPLAIN.
	root_dyn: Arc<dyn ExecOperator>,
	/// Projection-aware field set for computed-field materialization.
	/// `None` = all fields, `Some(set)` = specific fields.
	needed_fields: Option<HashSet<String>>,
	/// Plan-time resolved table context. When present, `execute()` skips
	/// runtime table def + permission lookup.
	resolved: Option<ResolvedTableContext>,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	metrics: Arc<OperatorMetrics>,
}

impl BitmapResolve {
	pub(crate) fn new(
		table_name: TableName,
		root: Arc<BitmapNode>,
		needed_fields: Option<HashSet<String>>,
	) -> Self {
		let root_dyn = Arc::clone(&root) as Arc<dyn ExecOperator>;
		Self {
			table_name,
			root,
			root_dyn,
			needed_fields,
			resolved: None,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	/// Set the plan-time resolved table context.
	pub(crate) fn with_resolved(mut self, resolved: ResolvedTableContext) -> Self {
		self.resolved = Some(resolved);
		self
	}
}

impl ExecOperator for BitmapResolve {
	fn name(&self) -> &'static str {
		"BitmapResolve"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("table".to_string(), self.table_name.to_string())]
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.root_dyn]
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let db_ctx = ctx.database()?.clone();
		validate_record_user_access(&db_ctx)?;
		let check_perms = should_check_perms(&db_ctx, Action::View)?;

		let table_name = self.table_name.clone();
		let root = Arc::clone(&self.root);
		let resolved = self.resolved.clone();
		let needed_fields = self.needed_fields.clone();
		let ctx = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			// The planner never emits a bitmap plan for versioned queries; if
			// a version context reaches this operator anyway, fail loudly
			// rather than return current-state rows for a historical query.
			reject_versioned_execution(&ctx)?;
			let db_ctx = ctx.database().context("BitmapResolve requires database context")?;
			let ns = Arc::clone(&db_ctx.ns_ctx.ns);
			let db = Arc::clone(&db_ctx.db);
			let txn = ctx.txn();

			// Resolve table permissions: plan-time fast path or runtime fallback.
			let select_permission = if let Some(ref res) = resolved {
				res.select_permission(check_perms)
			} else if check_perms {
				let table_def =
					db_ctx.get_table_def(&table_name, None).await.context("Failed to get table")?;
				if let Some(def) = &table_def {
					convert_permission_to_physical_runtime(&def.permissions.select, ctx.ctx())
						.await
						.context("Failed to convert permission")?
				} else {
					Err(ControlFlow::Err(anyhow::Error::new(Error::TbNotFound {
						name: table_name.clone(),
					})))?
				}
			} else {
				PhysicalPermission::Allow
			};

			// Early exit if denied.
			if matches!(select_permission, PhysicalPermission::Deny) {
				return Ok(());
			}

			// Field state for computed fields and field-level permissions.
			let field_state = if let Some(ref res) = resolved {
				res.field_state_for_projection(needed_fields.as_ref())
			} else {
				build_field_state(&ctx, &table_name, check_perms, needed_fields.as_ref()).await?
			};

			// Table-level permissions are handled by fetch_and_filter_records_batch.
			// The pipeline handles computed fields and field-level permissions.
			let mut pipeline = ScanPipeline::new(
				PhysicalPermission::Allow,
				None,
				field_state,
				check_perms,
				None,
				0,
			);

			// Evaluate the bitmap expression tree over the shared doc-ID space.
			let doc_ids = TableDocIds::new(ns.namespace_id, db.database_id, table_name.clone());
			let bctx = BitmapBuildContext {
				ctx: &ctx,
				txn: txn.as_ref(),
				ns: ns.namespace_id,
				db: db.database_id,
				table: &table_name,
				doc_ids: &doc_ids,
				budget: *crate::cnf::BITMAP_BRANCH_BUDGET,
			};
			let bitmap = match root.build_bitmap(&bctx, true).await? {
				BranchBitmap::Ready(bitmap) => bitmap,
				// The root is anchored; overflow is unreachable.
				BranchBitmap::Overflow => {
					Err(ControlFlow::Err(anyhow::anyhow!(
						"The anchored bitmap plan root overflowed its branch budget"
					)))?;
					unreachable!()
				}
			};

			// Resolve surviving doc-IDs to record IDs in batches, then fetch
			// through the standard permission/field pipeline.
			let mut iter = bitmap.into_iter();
			loop {
				if ctx.cancellation().is_cancelled() {
					Err(ControlFlow::Err(anyhow::anyhow!(Error::QueryCancelled)))?;
				}
				let chunk: Vec<u64> = iter.by_ref().take(RESOLVE_BATCH_SIZE).collect();
				if chunk.is_empty() {
					break;
				}
				let keys = doc_ids
					.get_record_ids_batch(txn.as_ref(), &chunk)
					.await
					.context("Failed to resolve doc-IDs to record IDs")?;
				let mut rids = Vec::with_capacity(keys.len());
				for key in keys.into_iter().flatten() {
					// A doc-ID without a reverse mapping corresponds to a
					// stale index entry for a deleted record; the streaming
					// path drops those at fetch time, so skip it here too.
					rids.push(RecordId {
						table: table_name.clone(),
						key,
					});
				}
				if rids.is_empty() {
					continue;
				}
				let mut values = fetch_and_filter_records_batch(
					&ctx,
					&txn,
					ns.namespace_id,
					db.database_id,
					&rids,
					&select_permission,
					check_perms,
					None,
					CachePolicy::ReadOnly,
				)
				.await?;
				pipeline.process_batch(&mut values, &ctx).await?;
				if !values.is_empty() {
					yielder
						.emit(ValueBatch {
							values,
						})
						.await;
				}
			}
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "BitmapResolve", &self.metrics))
	}
}
