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
//!   any ORDER BY is enforced by a downstream Sort. An OR fusion plan additionally carries the
//!   streaming union it replaced as an overflow fallback: when any union branch exceeds its
//!   drained-entry budget, the bitmap is abandoned and the fallback's stream is emitted instead
//!   (`union_tier: fallback` under EXPLAIN ANALYZE).
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
use futures::StreamExt;
use reblessive::TreeStack;
use roaring::RoaringTreemap;

use super::common::{
	DecodeFields, RECORD_PROBE_ROWS, ReadAhead, decode_fields_for_execution, drain_read_ahead,
	next_batch_len, probe_batch_len,
};
use super::pipeline::{ScanPipeline, build_field_state};
use super::resolved::ResolvedTableContext;
use crate::catalog::{DatabaseId, Error, Index, NamespaceId};
use crate::err::EngineError;
use crate::exec::index::access_path::{BTreeAccess, IndexRef};
use crate::exec::index::iterator::btree::{
	INDEX_BATCH_SIZE, bitmap_scan_range, decode_entry_doc_ids,
};
use crate::exec::permission::{
	PhysicalPermission, should_check_perms, table_select_permission_physical_runtime,
	tables_have_full_select_permission, validate_record_user_access,
};
use crate::exec::{
	AccessMode, BoxFut, ContextLevel, ExecOperator, ExecutionContext, FlowResult, OperatorMetrics,
	ValueBatchStream, monitor_stream,
};
use crate::expr::operator::MatchesOperator;
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::iam::Action;
use crate::idx::IndexKeyBase;
use crate::idx::docids::TableDocIds;
use crate::idx::ft::fulltext::FullTextIndex;
use crate::kvs::Transaction;
use crate::kvs::util::scan;
use crate::legacy::analyzer_function::LegacyAnalyzerFunction;
use crate::val::{RecordId, TableName};

/// Maximum number of doc-IDs resolved and fetched per output batch.
const RESOLVE_BATCH_SIZE: usize = 1000;

/// Multiplier applied to the accumulated intersection's cardinality to budget
/// each droppable AND child: a branch may drain at most `acc.len()` times this
/// factor before it is cut off, since past that point completing it cannot
/// beat re-checking its predicate per surviving row through the residual
/// filter.
const AND_CHILD_BUDGET_FACTOR: u64 = 64;

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
	/// entry value (see [`crate::idx::entry::IndexEntryValue`]).
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
	/// A single-hop graph reachability semi-join (issue #549): scanned-table
	/// rows adjacent to `source` through `edge_tables` in `direction`, read
	/// entirely from adjacency pointer keys. See
	/// [`crate::exec::index::access_path::BitmapPlan::Graph`] for the
	/// contract; the build arm documents the budget and legacy-key rules.
	Graph {
		source: crate::val::RecordId,
		direction: crate::expr::Dir,
		edge_tables: Vec<TableName>,
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
/// per-branch entry budget ([`surrealdb_cnf::BITMAP_BRANCH_BUDGET`]). An AND
/// simply drops such a branch — the full WHERE clause is kept as a residual
/// filter above [`BitmapResolve`], so dropping a conjunct only widens the
/// candidate set. A branch that cannot be dropped (a union member or an
/// AND-NOT base) propagates its overflow to the whole evaluation instead;
/// under an anchored plan root that propagation cannot occur, because an
/// anchored node — one whose bitmap is required for the plan to produce rows
/// at all — is evaluated without a budget and never overflows. A root is
/// anchored only when the owning [`BitmapResolve`] has no overflow fallback;
/// with one attached, every branch is budgeted and a root overflow switches
/// execution to the fallback's streaming plan.
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

	pub(crate) fn graph(
		source: crate::val::RecordId,
		direction: crate::expr::Dir,
		edge_tables: Vec<TableName>,
	) -> Arc<Self> {
		Arc::new(Self {
			kind: BitmapNodeKind::Graph {
				source,
				direction,
				edge_tables,
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
				BitmapNodeKind::Graph {
					source,
					direction,
					edge_tables,
				} => self.build_graph_bitmap(bctx, source, *direction, edge_tables).await?,
				BitmapNodeKind::And {
					children,
				} => {
					// The first child anchors the intersection: it is drained
					// without a budget so the AND always has a base. Later
					// children that overflow are dropped — the residual WHERE
					// filter enforces their predicate per surviving row. In
					// strict mode nothing may be dropped: any overflow
					// invalidates the whole evaluation.
					let mut acc: Option<RoaringTreemap> = None;
					for (i, child) in children.iter().enumerate() {
						// Droppable children are budgeted in proportion to the
						// intersection accumulated so far: once `acc` is known
						// to be small, a branch draining past `acc.len() *
						// AND_CHILD_BUDGET_FACTOR` entries cannot pay for
						// itself against the residual per-row filter, so it is
						// cut off there instead of at the full branch budget.
						// Never tightened in strict mode (an overflow there
						// invalidates the whole tree, see
						// [`Self::build_allowlist`]) or in exact/unbudgeted
						// mode (`budget == 0`).
						let child_budget = match &acc {
							Some(acc) if !bctx.strict && bctx.budget > 0 => bctx.budget.min(
								usize::try_from(acc.len().saturating_mul(AND_CHILD_BUDGET_FACTOR))
									.unwrap_or(usize::MAX),
							),
							_ => bctx.budget,
						};
						let child_bctx = BitmapBuildContext {
							budget: child_budget,
							..*bctx
						};
						match child.build_bitmap(&child_bctx, anchored && i == 0).await? {
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
							BranchBitmap::Overflow if bctx.strict => {
								return Ok(BranchBitmap::Overflow);
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
							// In strict mode a subtraction overflow invalidates
							// the evaluation like any other branch (unreachable
							// for v1 KNN prefilter trees, which carry no NOTs —
							// kept airtight regardless).
							if !acc.is_empty() {
								match subtract.build_bitmap(bctx, false).await? {
									BranchBitmap::Ready(sub) => acc -= sub,
									BranchBitmap::Overflow if bctx.strict => {
										return Ok(BranchBitmap::Overflow);
									}
									BranchBitmap::Overflow => {}
								}
							}
							BranchBitmap::Ready(acc)
						}
						BranchBitmap::Overflow => BranchBitmap::Overflow,
					}
				}
			};
			match &result {
				// Candidate cardinality for EXPLAIN ANALYZE.
				BranchBitmap::Ready(bitmap) => self.metrics.add_output_rows(bitmap.len()),
				// A declined branch renders as `dropped: true` so it is
				// distinguishable from a genuinely empty candidate set.
				BranchBitmap::Overflow => self.metrics.set_branch_dropped(),
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
			strict: false,
		};
		match self.build_bitmap(&bctx, true).await? {
			BranchBitmap::Ready(bitmap) => Ok(bitmap.len()),
			// Unreachable with the budget disabled.
			BranchBitmap::Overflow => Err(ControlFlow::Err(anyhow::anyhow!(
				"An exact bitmap plan overflowed its branch budget"
			))),
		}
	}

	/// Evaluate the tree into a KNN allow-list bitmap (#548, pre-filtered
	/// vector search).
	///
	/// Strict and budget-bounded: each branch is one covered WHERE conjunct
	/// already dropped from the scan's in-traversal residual, so no branch may
	/// be dropped (a widened bitmap would admit candidates that fail the
	/// covered conjuncts, letting them consume top-K slots). No branch is
	/// anchored either — the allow-list is an optimization, never required for
	/// the plan to produce rows, so nothing may drain unbounded. Any branch
	/// exceeding `budget` therefore disables the whole prefilter — `Ok(None)`
	/// — and the caller falls back to full in-traversal filtering (past the
	/// budget the filter is non-selective enough for that path to be
	/// acceptable).
	pub(crate) async fn build_allowlist(
		&self,
		ctx: &ExecutionContext,
		table: &TableName,
		budget: usize,
	) -> std::result::Result<Option<RoaringTreemap>, ControlFlow> {
		// The planner never attaches a prefilter to a versioned KNN query;
		// this is the defense-in-depth backstop.
		reject_versioned_execution(ctx)?;
		let db_ctx = ctx.database().context("KNN prefilter requires database context")?;
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
			budget,
			strict: true,
		};
		match self.build_bitmap(&bctx, false).await? {
			BranchBitmap::Ready(bitmap) => Ok(Some(bitmap)),
			BranchBitmap::Overflow => Ok(None),
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
				return Err(ControlFlow::Err(anyhow::anyhow!(EngineError::QueryCancelled)));
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

	/// Drain a single-hop reachability scope into a doc-ID bitmap
	/// (issue #549).
	///
	/// Reads the anchor's adjacency pointer keys — which embed the far
	/// vertex — so the reachable set costs one key-range scan plus one
	/// batched doc-ID lookup per scanned batch, with no edge or record
	/// fetches. Each batch's far-vertex keys are resolved as soon as they
	/// are scanned, so peak memory is one batch of keys rather than the
	/// whole reachable set; deduplication is per batch only, and a target
	/// reachable through several batches (parallel edges, multiple edge
	/// tables) costs a redundant point lookup against an idempotent bitmap
	/// insert. On the overflow path, lookups already paid for earlier
	/// batches are discarded with the branch — bounded by the same budget
	/// that triggers the overflow. Three rules keep it a superset producer
	/// under the residual WHERE filter:
	///
	/// - **Always budgeted**, even as the plan's anchor: an adjacency band has no selectivity
	///   guarantee (a hub vertex drains unbounded), and the analyzer only ever emits this leaf as a
	///   droppable AND conjunct.
	/// - **A legacy-format key overflows the branch**: it carries no target, so the reachable set
	///   would silently under-approximate — dropping the branch instead only widens the candidate
	///   set.
	/// - **Strict mode overflows immediately**: an allow-list branch is dropped from the
	///   in-traversal residual, so nothing downstream could correct a miss.
	///
	/// A far vertex with no doc-ID in the scanned table's space is skipped:
	/// it cannot appear in any sibling index bitmap (doc-IDs are assigned
	/// when index entries are written), so the intersection is unchanged.
	async fn build_graph_bitmap(
		&self,
		bctx: &BitmapBuildContext<'_>,
		source: &crate::val::RecordId,
		direction: crate::expr::Dir,
		edge_tables: &[TableName],
	) -> std::result::Result<BranchBitmap, ControlFlow> {
		use std::ops::Bound;

		use super::graph_keys::{EdgeTableSpec, compute_graph_ranges};
		if bctx.strict {
			return Ok(BranchBitmap::Overflow);
		}
		// SECURITY: this leaf resolves the far vertex straight from the
		// anchor's adjacency key, never fetching -- and so never
		// permission-checking -- the edge record. That is the same bypass
		// `Planner::try_fast_path_pair` gates behind `Permission::Full` for
		// the equivalent `GraphEdgeScan` `TargetVertex` collapse, so it is
		// gated identically here. `IndexAnalyzer::try_bitmap_fusion` only
		// ever appends this leaf alongside another AND branch, never as the
		// plan's anchor, so declining it just drops it from the
		// intersection: the unchanged residual `Filter` re-evaluates the
		// original lookup through the permission-checked traversal path and
		// still answers correctly. The check runs before any adjacency scan
		// so a denied session pays a fixed, schema-only cost rather than one
		// that scales with the relationship count it has no SELECT
		// permission to observe.
		let db_ctx = bctx.ctx.database().context("Bitmap graph leaf requires database context")?;
		if should_check_perms(db_ctx, Action::View)?
			&& !tables_have_full_select_permission(
				bctx.txn,
				db_ctx.ns_name(),
				db_ctx.db_name(),
				edge_tables,
			)
			.await
		{
			return Ok(BranchBitmap::Overflow);
		}
		let specs: Vec<EdgeTableSpec> = edge_tables
			.iter()
			.map(|table| EdgeTableSpec {
				table: table.clone(),
				range_start: Bound::Unbounded,
				range_end: Bound::Unbounded,
			})
			.collect();
		let ranges =
			compute_graph_ranges(bctx.ns, bctx.db, source, direction, &specs, bctx.ctx).await?;
		let mut docs = RoaringTreemap::new();
		let mut batch_targets: Vec<crate::val::RecordIdKey> = Vec::new();
		let mut drained = 0usize;
		for mut range in ranges {
			loop {
				if bctx.ctx.cancellation().is_cancelled() {
					return Err(ControlFlow::Err(anyhow::anyhow!(EngineError::QueryCancelled)));
				}
				let keys =
					crate::kvs::util::scan_keys(&mut range.range, bctx.txn, INDEX_BATCH_SIZE)
						.await
						.context("Failed to scan adjacency keys")?;
				if keys.is_empty() {
					break;
				}
				drained += keys.len();
				if bctx.budget > 0 && drained > bctx.budget {
					return Ok(BranchBitmap::Overflow);
				}
				batch_targets.clear();
				for key in &keys {
					let decoded = range.decoder.decode(key)?;
					match decoded.target {
						Some(target) if target.table == *bctx.table => {
							batch_targets.push(target.key);
						}
						Some(_) => {}
						None => return Ok(BranchBitmap::Overflow),
					}
				}
				if batch_targets.is_empty() {
					continue;
				}
				// Sorted for lookup locality on LSM backends, and rid of repeats
				// of one record. Equality alone is not enough: it compares
				// numbers across variants, so it would drop one of `[1]` and
				// `[1dec]`, two records with a doc-ID each; it only screens
				// before the exact comparison, which encodes. A repeat that sorts
				// apart from its twin stays, which the batch read tolerates: it
				// answers every position it is asked for.
				batch_targets.sort_unstable();
				batch_targets.dedup_by(|a, b| a == b && a.addresses_same_record(b));
				let ids = bctx
					.doc_ids
					.get_doc_ids_batch(bctx.txn, &batch_targets)
					.await
					.context("Failed to resolve reachable records to doc-IDs")?;
				docs.extend(ids.into_iter().flatten());
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
			&frozen_ctx.config.idx.file_allowlist,
			index_def.format_version,
		)
		.await
		.context("Failed to open full-text index")?;
		let query_terms = {
			let az_fn = LegacyAnalyzerFunction::new(frozen_ctx, opt);
			let mut stack = TreeStack::new();
			stack
				.enter(|stk| fti.extract_querying_terms(stk, frozen_ctx, &az_fn, query.to_owned()))
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
			BitmapNodeKind::Graph {
				..
			} => "BitmapGraphScan",
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
			BitmapNodeKind::Graph {
				source,
				direction,
				edge_tables,
			} => vec![
				("source".to_string(), surrealdb_types::ToSql::to_sql(source)),
				(
					"direction".to_string(),
					match direction {
						crate::expr::Dir::In => "<-",
						crate::expr::Dir::Out => "->",
						crate::expr::Dir::Both => "<->",
					}
					.to_string(),
				),
				(
					"edges".to_string(),
					edge_tables.iter().map(|t| t.to_string()).collect::<Vec<_>>().join(", "),
				),
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
	/// All-or-nothing mode (#548 KNN allow-lists): a branch overflow
	/// invalidates the whole evaluation instead of being dropped. Dropping an
	/// AND child (or an AND-NOT subtraction) widens the bitmap — safe when
	/// the full WHERE is re-applied as a residual filter (#547 plans), but
	/// wrong for an allow-list whose covered conjuncts were dropped from the
	/// in-traversal residual.
	strict: bool,
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
	/// The fields to decode from each fetched record, when the plan reads no
	/// other field of it; `None` decodes whole records.
	decode_fields: Option<DecodeFields>,
	/// Plan-time resolved table context. When present, `execute()` skips
	/// runtime table def + permission lookup, and its enforcement mode says
	/// whether the scan applies the table permission, computed fields and
	/// field permissions itself or yields records as stored for the plan
	/// above it to apply them.
	resolved: Option<ResolvedTableContext>,
	/// Streaming plan to run when a bitmap branch overflows its budget.
	///
	/// Attached for OR fusion plans (a `UnionIndexScan` over the same
	/// branches): an OR may not drop an overflowing branch, so instead of
	/// draining branches unbudgeted the root is evaluated unanchored and any
	/// branch overflow abandons the bitmap in favour of this operator's
	/// stream. `None` (AND fusion) keeps the root anchored: overflowing
	/// conjuncts are dropped and the residual filter compensates.
	overflow_fallback: Option<Arc<dyn ExecOperator>>,
	/// Whether the overflow fallback ran, recorded during `execute()` for
	/// EXPLAIN ANALYZE (`union_tier: fallback`).
	fallback_taken: Arc<std::sync::atomic::AtomicBool>,
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
			decode_fields: None,
			resolved: None,
			overflow_fallback: None,
			fallback_taken: Arc::new(std::sync::atomic::AtomicBool::new(false)),
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	/// Set the plan-time resolved table context.
	pub(crate) fn with_resolved(mut self, resolved: ResolvedTableContext) -> Self {
		self.resolved = Some(resolved);
		self
	}

	/// Decode only `fields` of each fetched record; see
	/// [`DecodeFields`].
	pub(crate) fn with_decode_fields(mut self, fields: DecodeFields) -> Self {
		self.decode_fields = Some(fields);
		self
	}

	/// Attach the streaming plan to run when a bitmap branch overflows its
	/// drained-entry budget. See [`Self::overflow_fallback`].
	pub(crate) fn with_overflow_fallback(mut self, fallback: Arc<dyn ExecOperator>) -> Self {
		self.overflow_fallback = Some(fallback);
		self
	}
}

impl ExecOperator for BitmapResolve {
	fn name(&self) -> &'static str {
		"BitmapResolve"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![("table".to_string(), self.table_name.to_string())];
		// Whether a branch overflow abandoned the bitmap for the streaming
		// union fallback, recorded during execution — present under EXPLAIN
		// ANALYZE (which drains the plan before formatting), absent in a
		// plain EXPLAIN. Mirrors KnnScan's `prefilter_tier: fallback`.
		if self.fallback_taken.load(std::sync::atomic::Ordering::Relaxed) {
			attrs.push(("union_tier".to_string(), "fallback".to_string()));
		}
		attrs
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
		let decode_fields = self.decode_fields.clone();
		let overflow_fallback = self.overflow_fallback.clone();
		let fallback_taken = Arc::clone(&self.fallback_taken);
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
					table_select_permission_physical_runtime(Some(&**def), &ctx)
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
			let decode_fields = decode_fields_for_execution(
				&ctx,
				&table_name,
				resolved.as_ref(),
				&select_permission,
				check_perms,
				decode_fields.as_ref(),
			)
			.await?;

			// Table-level permissions are handled by ReadAhead::decode_batch.
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
				budget: *surrealdb_cnf::BITMAP_BRANCH_BUDGET,
				strict: false,
			};
			// With a fallback attached the root runs unanchored, so every
			// branch respects the drained-entry budget and pre-emission work
			// is bounded to ~budget entries per branch; without one (AND
			// fusion) the root is anchored and never overflows.
			let anchored = overflow_fallback.is_none();
			let bitmap = match root.build_bitmap(&bctx, anchored).await? {
				BranchBitmap::Ready(bitmap) => bitmap,
				BranchBitmap::Overflow => {
					let Some(fallback) = overflow_fallback else {
						// The root is anchored when no fallback is attached;
						// overflow is unreachable.
						Err(ControlFlow::Err(anyhow::anyhow!(
							"The anchored bitmap plan root overflowed its branch budget"
						)))?;
						unreachable!()
					};
					// A union branch exceeded the budget: abandon the bitmap
					// and stream the equivalent plan instead. The operator
					// handles permissions and fields itself, and the full
					// WHERE clause above re-filters its rows exactly as it
					// does the bitmap path's.
					fallback_taken.store(true, std::sync::atomic::Ordering::Relaxed);
					let mut stream = fallback.execute(&ctx)?;
					while let Some(batch) = stream.next().await {
						yielder.emit(batch?).await;
					}
					return Ok(());
				}
			};

			// Resolve surviving doc-IDs to record IDs in chunks and read each
			// chunk's stored bytes ahead, then decode them in batches of at
			// most the byte budget through the standard permission/field
			// pipeline. The first chunk is a small probe and each later one is
			// sized from the bytes the one before it read.
			let mut iter = bitmap.into_iter();
			let batch_bytes = ctx.root().ctx.config.exec.scan_batch_bytes;
			let mut chunk_len = probe_batch_len(batch_bytes, RECORD_PROBE_ROWS);
			let mut read_ahead = ReadAhead::new(
				&select_permission,
				check_perms,
				decode_fields.as_ref(),
				batch_bytes,
			);
			loop {
				if ctx.cancellation().is_cancelled() {
					Err(ControlFlow::Err(anyhow::anyhow!(EngineError::QueryCancelled)))?;
				}
				let chunk: Vec<u64> = iter.by_ref().take(chunk_len).collect();
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
				let (read_bytes, read_rows) =
					read_ahead.read(&txn, ns.namespace_id, db.database_id, rids, None).await?;
				chunk_len = next_batch_len(
					read_bytes,
					read_rows,
					chunk_len,
					batch_bytes,
					RESOLVE_BATCH_SIZE,
				);
				if !drain_read_ahead(&mut read_ahead, &mut pipeline, &mut yielder, &ctx).await? {
					break;
				}
			}
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "BitmapResolve", &self.metrics))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::scan::resolved::test_support::{
		RESTRICTED_TABLE, anonymous, assert_as_stored, assert_reduced, contexts, field, index_ref,
	};
	use crate::exec::operators::test_util::{TestDb, collect};
	use crate::kvs::TransactionType;
	use crate::val::Value;

	/// Under the plan's enforcement the resolve yields the fetched record as
	/// stored, for the nodes above it to reduce; under its own it reduces the
	/// record for a permission-checked caller.
	#[tokio::test]
	async fn a_planned_resolve_yields_the_stored_record_and_a_runtime_resolve_reduces_it() {
		let db = TestDb::new_with_auth(RESTRICTED_TABLE).await;
		let ctx = db.exec_ctx_as(&anonymous(), TransactionType::Read).await;
		let (planned, runtime) = contexts(&ctx, "t").await;
		let index_ref = index_ref(&ctx, "t", "n_idx").await;
		let scan = |tc: ResolvedTableContext| -> Arc<dyn ExecOperator> {
			let root = BitmapNode::btree(index_ref.clone(), BTreeAccess::Equality(Value::from(1)));
			Arc::new(BitmapResolve::new("t".into(), root, None).with_resolved(tc))
		};
		assert_as_stored(&collect(&scan(planned), &ctx).await);
		assert_reduced(&collect(&scan(runtime), &ctx).await);
	}

	/// Resolve every `t` record with `n = 1`, decoding only `fields`.
	async fn resolve_decoding(setup: &str, fields: &[&str]) -> Vec<Value> {
		let db = TestDb::new(setup).await;
		let ctx = db.exec_ctx().await;
		let index_ref = index_ref(&ctx, "t", "n_idx").await;
		let root = BitmapNode::btree(index_ref, BTreeAccess::Equality(Value::from(1)));
		let fields = DecodeFields::new(fields.iter().map(|f| f.to_string()));
		let scan: Arc<dyn ExecOperator> =
			Arc::new(BitmapResolve::new("t".into(), root, None).with_decode_fields(fields));
		collect(&scan, &ctx).await
	}

	/// A resolve given a decode set yields each record's id and the listed
	/// fields it stores, and none of the fields it does not list.
	#[tokio::test]
	async fn a_projected_resolve_decodes_only_the_listed_fields() {
		let rows = resolve_decoding(
			"DEFINE TABLE t SCHEMALESS;
			DEFINE INDEX n_idx ON t FIELDS n;
			CREATE t:1 SET n = 1, label = 'a', emb = [0.5, 0.5];
			CREATE t:2 SET n = 1, emb = [0.5, 0.5];",
			&["n", "label"],
		)
		.await;
		assert_eq!(rows.len(), 2, "{rows:?}");
		for row in &rows {
			assert!(matches!(field(row, "id"), Value::RecordId(_)), "id kept: {row:?}");
			assert_eq!(field(row, "n"), Value::from(1), "listed field decoded: {row:?}");
			assert_eq!(field(row, "emb"), Value::None, "unlisted field not decoded: {row:?}");
		}
		let labels: HashSet<Value> = rows.iter().map(|r| field(r, "label")).collect();
		assert_eq!(
			labels,
			HashSet::from([Value::from("a"), Value::None]),
			"absent field stays absent"
		);
	}

	/// A projected resolve sizes its batches by the stored bytes it reads, so
	/// narrow projected rows of wide records still arrive in smaller batches.
	#[tokio::test]
	async fn a_projected_resolve_sizes_batches_by_the_stored_bytes() {
		const ROWS: usize = 2500;
		let db = TestDb::new(&format!(
			"DEFINE TABLE t SCHEMALESS;
			DEFINE INDEX n_idx ON t FIELDS n;
			FOR $i IN 0..{ROWS} {{ CREATE t SET n = 1, emb = array::repeat(0.5f, 2048) }};"
		))
		.await;
		let ctx = db.exec_ctx().await;
		let index_ref = index_ref(&ctx, "t", "n_idx").await;
		let root = BitmapNode::btree(index_ref, BTreeAccess::Equality(Value::from(1)));
		let scan: Arc<dyn ExecOperator> = Arc::new(
			BitmapResolve::new("t".into(), root, None)
				.with_decode_fields(DecodeFields::new(["n".to_string()])),
		);

		let mut stream = scan.execute(&ctx).expect("execute should succeed");
		let mut lens = Vec::new();
		while let Some(batch) = stream.next().await {
			let rows = batch.expect("batch should be Ok").into_values();
			assert!(rows.iter().all(|r| field(r, "emb") == Value::None), "emb not decoded");
			lens.push(rows.len());
		}
		assert_eq!(lens.iter().sum::<usize>(), ROWS, "every record once: {lens:?}");
		assert_eq!(lens[0], RECORD_PROBE_ROWS, "the first batch is the probe: {lens:?}");
		assert!(lens[1] < RESOLVE_BATCH_SIZE, "later batches are sized by stored bytes: {lens:?}");
	}

	/// A computed field may read any stored field, so a table defining one is
	/// resolved as whole records whatever the decode set.
	#[tokio::test]
	async fn a_table_with_a_computed_field_resolves_whole_records() {
		let rows = resolve_decoding(
			"DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD first ON t COMPUTED emb[0];
			DEFINE INDEX n_idx ON t FIELDS n;
			CREATE t:1 SET n = 1, emb = [0.5, 0.5];",
			&["n"],
		)
		.await;
		assert_eq!(rows.len(), 1, "{rows:?}");
		assert_eq!(field(&rows[0], "emb"), Value::from(vec![Value::from(0.5f64); 2]), "{rows:?}");
	}

	/// A resolve planned with the table resolved gates its decode set on that
	/// table's field state, under either enforcement, so a computed field
	/// still makes it decode whole records.
	#[tokio::test]
	async fn a_resolved_table_with_a_computed_field_resolves_whole_records() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD first ON t COMPUTED emb[0];
			DEFINE INDEX n_idx ON t FIELDS n;
			CREATE t:1 SET n = 1, emb = [0.5, 0.5];",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let (planned, runtime) = contexts(&ctx, "t").await;
		let index_ref = index_ref(&ctx, "t", "n_idx").await;
		for tc in [runtime, planned] {
			let root = BitmapNode::btree(index_ref.clone(), BTreeAccess::Equality(Value::from(1)));
			let scan: Arc<dyn ExecOperator> = Arc::new(
				BitmapResolve::new("t".into(), root, None)
					.with_decode_fields(DecodeFields::new(["n".to_string()]))
					.with_resolved(tc),
			);
			let rows = collect(&scan, &ctx).await;
			assert_eq!(rows.len(), 1, "{rows:?}");
			assert_eq!(
				field(&rows[0], "emb"),
				Value::from(vec![Value::from(0.5f64); 2]),
				"{rows:?}"
			);
		}
	}

	/// The decoded size of `rows`, and of the widest of them.
	fn decoded_bytes(rows: &[Value]) -> (usize, usize) {
		let sizes = rows.iter().map(super::super::common::approx_value_size);
		(sizes.clone().sum(), sizes.max().unwrap_or(0))
	}

	/// The batches a resolve of `n_idx` on `n = 1` over table `t` yields.
	async fn resolve_batches(db: &TestDb) -> Vec<Vec<Value>> {
		let ctx = db.exec_ctx().await;
		let index_ref = index_ref(&ctx, "t", "n_idx").await;
		let root = BitmapNode::btree(index_ref, BTreeAccess::Equality(Value::from(1)));
		let scan: Arc<dyn ExecOperator> = Arc::new(BitmapResolve::new("t".into(), root, None));
		let mut stream = scan.execute(&ctx).expect("execute should succeed");
		let mut batches = Vec::new();
		while let Some(batch) = stream.next().await {
			batches.push(batch.expect("batch should be Ok").into_values());
		}
		batches
	}

	/// Wide records resolve in batches of at most the byte budget plus one
	/// record of decoded values, and every matching record is still returned
	/// exactly once.
	#[tokio::test]
	async fn wide_records_resolve_in_batches_sized_to_the_byte_budget() {
		const ROWS: usize = 1400;
		let db = TestDb::new(&format!(
			"DEFINE TABLE t SCHEMALESS;
			DEFINE INDEX n_idx ON t FIELDS n;
			FOR $i IN 0..{ROWS} {{ CREATE t SET n = 1, emb = array::repeat(0.5f, 1024) }};"
		))
		.await;
		let batches = resolve_batches(&db).await;

		let ids: HashSet<Value> = batches.iter().flatten().map(|row| field(row, "id")).collect();
		assert_eq!(ids.len(), ROWS, "every record once");
		assert_eq!(batches.iter().map(Vec::len).sum::<usize>(), ROWS, "no record twice");
		assert!(batches[0].len() <= RECORD_PROBE_ROWS, "the first read is the probe");
		for batch in &batches {
			let (bytes, widest) = decoded_bytes(batch);
			assert!(
				bytes < crate::exec::config::DEFAULT_SCAN_BATCH_BYTES + widest,
				"a batch of {} rows holds {bytes} bytes",
				batch.len()
			);
		}
	}

	/// However many narrow records precede them, wide records decode in
	/// batches of at most the byte budget plus one record: the read that
	/// reaches them is sized from the narrow records, but only its stored
	/// bytes are held until a batch decodes them.
	#[tokio::test]
	async fn wide_records_after_a_long_narrow_run_decode_within_the_budget() {
		// Enough narrow records for the reads to double up to the row cap.
		const NARROW: usize = 1100;
		const WIDE: usize = 400;
		let db = TestDb::new(&format!(
			"DEFINE TABLE t SCHEMALESS;
			DEFINE INDEX n_idx ON t FIELDS n;
			FOR $i IN 0..{NARROW} {{ CREATE t SET n = 1 }};
			FOR $i IN 0..{WIDE} {{ CREATE t SET n = 1, emb = array::repeat(0.5f, 1024) }};"
		))
		.await;
		let batches = resolve_batches(&db).await;

		assert_eq!(batches.iter().map(Vec::len).sum::<usize>(), NARROW + WIDE, "every record once");
		let widest = batches.iter().map(|b| decoded_bytes(b).1).max().unwrap_or(0);
		for batch in &batches {
			let (bytes, _) = decoded_bytes(batch);
			assert!(
				bytes < crate::exec::config::DEFAULT_SCAN_BATCH_BYTES + widest,
				"a batch of {} rows holds {bytes} bytes",
				batch.len()
			);
		}
	}

	/// The resolve takes its byte budget from the execution config, so a
	/// smaller `scan_batch_bytes` yields proportionally smaller batches.
	///
	/// Gated like [`TestDb::new_with_config`], which only the in-memory
	/// backend provides.
	#[cfg(all(feature = "kv-mem", not(target_family = "wasm")))]
	#[tokio::test]
	async fn the_configured_batch_bytes_size_the_batches() {
		const ROWS: usize = 400;
		let config = surrealdb_cnf::ConfigMap::empty().with_key_value("scan_batch_bytes", "256KiB");
		let db = TestDb::new_with_config(
			&format!(
				"DEFINE TABLE t SCHEMALESS;
				DEFINE INDEX n_idx ON t FIELDS n;
				FOR $i IN 0..{ROWS} {{ CREATE t SET n = 1, emb = array::repeat(0.5f, 1024) }};"
			),
			config,
		)
		.await;
		let ctx = db.exec_ctx().await;
		let index_ref = index_ref(&ctx, "t", "n_idx").await;
		let root = BitmapNode::btree(index_ref, BTreeAccess::Equality(Value::from(1)));
		let scan: Arc<dyn ExecOperator> = Arc::new(BitmapResolve::new("t".into(), root, None));

		let mut stream = scan.execute(&ctx).expect("execute should succeed");
		let mut lens = Vec::new();
		while let Some(batch) = stream.next().await {
			lens.push(batch.expect("batch should be Ok").into_values().len());
		}
		assert_eq!(lens.iter().sum::<usize>(), ROWS, "every record once: {lens:?}");
		// ~66 KB decoded per record: 256 KiB holds about 4, where the default
		// 2 MiB holds about 31.
		assert!(lens[0] <= 4, "the probe follows the configured budget: {lens:?}");
		assert!(lens[1] <= 8, "later batches follow the configured budget: {lens:?}");
	}

	/// A run of narrow records grows the batch at most twofold per batch, so
	/// wide records that follow it are not fetched in a full row-capped batch.
	#[tokio::test]
	async fn wide_records_after_narrow_ones_stay_in_small_batches() {
		const NARROW: usize = 100;
		const WIDE: usize = 1400;
		let db = TestDb::new(&format!(
			"DEFINE TABLE t SCHEMALESS;
			DEFINE INDEX n_idx ON t FIELDS n;
			FOR $i IN 0..{NARROW} {{ CREATE t SET n = 1 }};
			FOR $i IN 0..{WIDE} {{ CREATE t SET n = 1, emb = array::repeat(0.5f, 1024) }};"
		))
		.await;
		let ctx = db.exec_ctx().await;
		let index_ref = index_ref(&ctx, "t", "n_idx").await;
		let root = BitmapNode::btree(index_ref, BTreeAccess::Equality(Value::from(1)));
		let scan: Arc<dyn ExecOperator> = Arc::new(BitmapResolve::new("t".into(), root, None));

		let mut stream = scan.execute(&ctx).expect("execute should succeed");
		let mut lens = Vec::new();
		while let Some(batch) = stream.next().await {
			lens.push(batch.expect("batch should be Ok").into_values().len());
		}
		assert_eq!(lens.iter().sum::<usize>(), NARROW + WIDE, "every record once: {lens:?}");
		// 32 and 64 narrow rows, then at most 128 before the wide rows shrink it.
		let largest = lens.iter().copied().max().unwrap_or(0);
		assert!(largest <= 4 * RECORD_PROBE_ROWS, "largest batch {largest}: {lens:?}");
	}

	/// Batches are sized from every record a fetch decoded, so wide records a
	/// permission denies keep the batches small even when the records that
	/// survive are narrow.
	#[tokio::test]
	async fn rows_a_permission_denies_still_size_the_batch() {
		const ROWS: usize = 1500;
		let db = TestDb::new_with_auth(&format!(
			"DEFINE TABLE t SCHEMALESS PERMISSIONS FOR select WHERE allow = true;
			DEFINE INDEX n_idx ON t FIELDS n;
			FOR $i IN 0..{ROWS} {{
				IF $i % 10 == 0 {{ CREATE t SET n = 1, allow = true }}
				ELSE {{ CREATE t SET n = 1, allow = false, emb = array::repeat(0.5f, 1024) }}
			}};"
		))
		.await;
		let ctx = db.exec_ctx_as(&anonymous(), TransactionType::Read).await;
		let index_ref = index_ref(&ctx, "t", "n_idx").await;
		let root = BitmapNode::btree(index_ref, BTreeAccess::Equality(Value::from(1)));
		let scan: Arc<dyn ExecOperator> = Arc::new(BitmapResolve::new("t".into(), root, None));

		let mut stream = scan.execute(&ctx).expect("execute should succeed");
		let mut batches = Vec::new();
		while let Some(batch) = stream.next().await {
			batches.push(batch.expect("batch should be Ok").into_values());
		}
		let rows: usize = batches.iter().map(Vec::len).sum();
		assert_eq!(rows, ROWS / 10, "only the permitted records are returned");
		// Sized from the narrow survivors alone, the requests would double up
		// to the row cap and yield a handful of batches.
		assert!(batches.len() > 10, "requests stay small: {} batches", batches.len());
	}

	/// A computed field can make a row far wider than its stored record, so
	/// the batches are sized from the materialised rows.
	#[tokio::test]
	async fn wide_computed_fields_size_the_batches() {
		const ROWS: usize = 2500;
		let db = TestDb::new(&format!(
			"DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD wide ON t COMPUTED array::repeat(0.5f, 1024);
			DEFINE INDEX n_idx ON t FIELDS n;
			FOR $i IN 0..{ROWS} {{ CREATE t SET n = 1 }};"
		))
		.await;
		let ctx = db.exec_ctx().await;
		let index_ref = index_ref(&ctx, "t", "n_idx").await;
		let root = BitmapNode::btree(index_ref, BTreeAccess::Equality(Value::from(1)));
		let scan: Arc<dyn ExecOperator> = Arc::new(BitmapResolve::new("t".into(), root, None));

		let mut stream = scan.execute(&ctx).expect("execute should succeed");
		let mut lens = Vec::new();
		while let Some(batch) = stream.next().await {
			let rows = batch.expect("batch should be Ok").into_values();
			assert!(rows.iter().all(|r| field(r, "wide") != Value::None), "wide materialised");
			lens.push(rows.len());
		}
		assert_eq!(lens.iter().sum::<usize>(), ROWS, "every record once: {lens:?}");
		// ~66 KB per materialised row: the default 2 MiB holds about 31.
		let largest = lens.iter().copied().max().unwrap_or(0);
		assert!(largest <= 2 * RECORD_PROBE_ROWS, "largest batch {largest}: {lens:?}");
	}
}
