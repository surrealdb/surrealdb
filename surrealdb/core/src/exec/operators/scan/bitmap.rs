//! Bitmap candidate evaluation (roaring bitmap index fusion).
//!
//! A [`BitmapPlan`] is a tree of set algebra over [`roaring::RoaringTreemap`]s in a table's shared
//! doc-ID space, the candidate currency for AND/OR/AND-NOT composition of index-backed
//! predicates. Leaves drain a b-tree index range (reading the doc-ID appended to each entry value),
//! expose a full-text query's merged posting bitmap or read a graph anchor's adjacency; inner nodes
//! apply set algebra (intersection smallest-first, union, difference).
//!
//! The entry points ([`evaluate_fusion`], [`evaluate_fusion_plan`], [`knn_allow_list`],
//! [`exact_bitmap_cardinality`]) evaluate a plan against the query's [`Transaction`]. The VM's
//! bitmap scan batch-resolves the surviving doc-IDs to record IDs through
//! [`TableDocIds::get_record_ids_batch`] and feeds the standard fetch pipeline (table permissions
//! per record, computed fields, field-level permissions). Rows are emitted in doc-ID order, which
//! is unspecified relative to any field, so any ORDER BY is enforced by a downstream sort. An OR
//! fusion plan also carries a streaming union as an overflow fallback: when any union branch
//! exceeds its drained-entry budget, the bitmap is abandoned and the fallback's stream is emitted
//! instead (`union_tier: fallback` under EXPLAIN ANALYZE).
//!
//! An optional [`BitmapTally`] records the cardinality each node produced, for EXPLAIN ANALYZE;
//! EXPLAIN renders the plan from the lowering and the tally only supplies counters.
//!
//! Snapshot consistency is inherited from the query's [`Transaction`]: every
//! branch drains index entries through it, so all branches observe one
//! snapshot. Row-level permissions are never bitmap-evaluated — they apply
//! per record at the resolve/fetch stage, exactly as for other index scans.

use roaring::RoaringTreemap;

use crate::catalog::{DatabaseId, Index, NamespaceId};
use crate::err::EngineError;
use crate::exec::analyzer_function::StreamingAnalyzerFunction;
use crate::exec::index::access_path::{BTreeAccess, BitmapPlan, IndexRef};
use crate::exec::index::iterator::btree::{
	INDEX_BATCH_SIZE, bitmap_scan_range, decode_entry_doc_ids,
};
use crate::exec::permission::{should_check_perms, tables_have_full_select_permission};
use crate::exec::{BoxFut, ExecutionContext, FlowResult, OperatorMetrics};
use crate::expr::operator::MatchesOperator;
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::iam::Action;
use crate::idx::IndexKeyBase;
use crate::idx::docids::TableDocIds;
use crate::idx::ft::fulltext::FullTextIndex;
use crate::kvs::Transaction;
use crate::kvs::util::scan;
use crate::val::{RecordId, TableName};

/// Maximum number of doc-IDs resolved and fetched per output batch.
pub(crate) const RESOLVE_BATCH_SIZE: usize = 1000;

/// Multiplier applied to the accumulated intersection's cardinality to budget
/// each droppable AND child: a branch may drain at most `acc.len()` times this
/// factor before it is cut off, since past that point completing it cannot
/// beat re-checking its predicate per surviving row through the residual
/// filter.
const AND_CHILD_BUDGET_FACTOR: u64 = 64;

/// Multiplier applied to a KNN search's `ef` to bound how many index entries
/// each allow-list branch may drain before overflowing to in-traversal
/// filtering ([`PrefilterTier::Fallback`]).
const KNN_BRANCH_BUDGET_PER_EF: usize = 512;

/// Floor for the `ef`-scaled KNN allow-list branch budget (before clamping to
/// [`surrealdb_cnf::BITMAP_BRANCH_BUDGET`]), so small-`ef` searches can still
/// materialize allow-lists across the exact tier and into the graph tier.
const KNN_BRANCH_BUDGET_FLOOR: usize = 4_096;

/// Compute the per-branch drained-entry budget for a KNN allow-list bitmap
/// plan given the search's exploration width `ef`.
///
/// Scales linearly with `ef` between a floor above the exact-tier threshold and
/// [`surrealdb_cnf::BITMAP_BRANCH_BUDGET`], so non-selective predicates on
/// small-`ef` searches overflow early to in-traversal filtering instead of
/// draining hundreds of thousands of index keys.
pub(crate) fn knn_branch_budget(ef: u32) -> usize {
	let max_budget = *surrealdb_cnf::BITMAP_BRANCH_BUDGET;
	if max_budget == 0 {
		return 0;
	}
	let exact_floor = usize::try_from(*surrealdb_cnf::KNN_PREFILTER_EXACT_THRESHOLD)
		.unwrap_or(usize::MAX)
		.saturating_mul(2)
		.max(KNN_BRANCH_BUDGET_FLOOR);
	let scaled = (ef as usize).saturating_mul(KNN_BRANCH_BUDGET_PER_EF).max(exact_floor);
	scaled.min(max_budget)
}

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

/// Result of evaluating one branch.
///
/// `Overflow` means the branch abandoned its drain because it exceeded the
/// per-branch entry budget ([`surrealdb_cnf::BITMAP_BRANCH_BUDGET`]). An AND
/// simply drops such a branch — the full WHERE clause is kept as a residual
/// filter above the bitmap scan, so dropping a conjunct only widens the
/// candidate set. A branch that cannot be dropped (a union member or an
/// AND-NOT base) propagates its overflow to the whole evaluation instead;
/// under an anchored plan root that propagation cannot occur, because an
/// anchored node — one whose bitmap is required for the plan to produce rows
/// at all — is evaluated without a budget and never overflows. A root is
/// anchored only when the owning bitmap scan has no overflow fallback;
/// with one attached, every branch is budgeted and a root overflow switches
/// execution to the fallback's streaming plan.
enum BranchBitmap {
	Ready(RoaringTreemap),
	Overflow,
}

/// What each node of a bitmap plan produced in one evaluation, shaped as the plan: the operands
/// of an intersection or union are its children in order, of a difference its base then its
/// subtrahend.
#[derive(Debug)]
pub(crate) struct BitmapTally {
	/// The cardinality the node produced, or that its branch was dropped.
	pub(crate) metrics: OperatorMetrics,
	operands: Vec<BitmapTally>,
}

impl BitmapTally {
	/// A tally for each node of `plan`, nothing yet recorded.
	pub(crate) fn of(plan: &BitmapPlan) -> Self {
		let operands = match plan {
			BitmapPlan::BTree {
				..
			}
			| BitmapPlan::FullText {
				..
			}
			| BitmapPlan::Graph {
				..
			} => Vec::new(),
			BitmapPlan::And(children) | BitmapPlan::Or(children) => {
				children.iter().map(Self::of).collect()
			}
			BitmapPlan::AndNot {
				base,
				subtract,
			} => vec![Self::of(base), Self::of(subtract)],
		};
		Self {
			metrics: OperatorMetrics::new(),
			operands,
		}
	}

	/// The tally of the `at`th operand: a child of an intersection or union, the base (`0`) or
	/// the subtrahend (`1`) of a difference.
	pub(crate) fn operand(&self, at: usize) -> Option<&Self> {
		self.operands.get(at)
	}
}

/// Evaluate a bitmap plan into a candidate bitmap.
///
/// `anchored` marks a node whose bitmap is required for the plan to produce
/// rows at all: the budget is disabled for it (and for every descendant that
/// is itself non-droppable), so an anchored node never returns
/// [`BranchBitmap::Overflow`]. `node` is the [`BitmapTally`] shaped as
/// `plan`, when there is one: each of its nodes records its cardinality as
/// its plan node is evaluated. The evaluation reads and writes nothing else.
fn build_bitmap_plan<'a>(
	plan: &'a BitmapPlan,
	bctx: &'a BitmapBuildContext<'a>,
	anchored: bool,
	node: Option<&'a BitmapTally>,
) -> BoxFut<'a, std::result::Result<BranchBitmap, ControlFlow>> {
	Box::pin(async move {
		let result = match plan {
			BitmapPlan::BTree {
				index_ref,
				access,
			} => build_btree_bitmap(bctx, index_ref, access, anchored).await?,
			BitmapPlan::FullText {
				index_ref,
				query,
				operator,
			} => {
				// Posting lists are already materialized bitmaps; no budget.
				BranchBitmap::Ready(build_fulltext_bitmap(bctx, index_ref, query, operator).await?)
			}
			BitmapPlan::Graph {
				source,
				direction,
				edge_tables,
			} => build_graph_bitmap(bctx, source, *direction, edge_tables).await?,
			BitmapPlan::And(children) => {
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
					// [`knn_allow_list`]) or in exact/unbudgeted
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
					match build_bitmap_plan(
						child,
						&child_bctx,
						anchored && i == 0,
						node.and_then(|n| n.operand(i)),
					)
					.await?
					{
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
			BitmapPlan::Or(children) => {
				// A union missing a member would silently drop rows, so
				// every child inherits this node's anchoring; any child
				// overflow makes the whole union overflow. For a spatial
				// multi-cell cover (all children scanning a spatial index),
				// unanchored children share the union's budget so the cover
				// is bounded by the same ceiling as a single range.
				let shared_budget = !children.is_empty()
					&& children.iter().all(|c| {
						matches!(
							c,
							BitmapPlan::BTree { index_ref, .. }
								if matches!(index_ref.definition().index, Index::Spatial)
						)
					});
				let mut acc = RoaringTreemap::new();
				let mut overflow = false;
				for (i, child) in children.iter().enumerate() {
					let child_budget = if shared_budget && !anchored && bctx.budget > 0 {
						let used = usize::try_from(acc.len()).unwrap_or(usize::MAX);
						bctx.budget.saturating_sub(used).max(1)
					} else {
						bctx.budget
					};
					let child_bctx = BitmapBuildContext {
						budget: child_budget,
						..*bctx
					};
					match build_bitmap_plan(
						child,
						&child_bctx,
						anchored,
						node.and_then(|n| n.operand(i)),
					)
					.await?
					{
						BranchBitmap::Ready(bitmap) => {
							acc |= bitmap;
							if shared_budget
								&& !anchored && bctx.budget > 0
								&& usize::try_from(acc.len()).unwrap_or(usize::MAX) > bctx.budget
							{
								overflow = true;
								break;
							}
						}
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
			BitmapPlan::AndNot {
				base,
				subtract,
			} => {
				match build_bitmap_plan(base, bctx, anchored, node.and_then(|n| n.operand(0)))
					.await?
				{
					BranchBitmap::Ready(mut acc) => {
						// A dropped subtraction only widens the candidate
						// set (the residual `NOT ...` filter still
						// applies), so the subtract side is never anchored.
						// In strict mode a subtraction overflow invalidates
						// the evaluation like any other branch.
						// A KNN prefilter tree never contains a NOT, so
						// this arm is unreachable for those trees.
						if !acc.is_empty() {
							match build_bitmap_plan(
								subtract,
								bctx,
								false,
								node.and_then(|n| n.operand(1)),
							)
							.await?
							{
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
		if let Some(node) = node {
			match &result {
				// Candidate cardinality for EXPLAIN ANALYZE.
				BranchBitmap::Ready(bitmap) => node.metrics.add_output_rows(bitmap.len()),
				// A declined branch renders as `dropped: true` so it is
				// distinguishable from a genuinely empty candidate set.
				BranchBitmap::Overflow => node.metrics.set_branch_dropped(),
			}
		}
		Ok(result)
	})
}

/// Drain a b-tree index range into a doc-ID bitmap.
async fn build_btree_bitmap(
	bctx: &BitmapBuildContext<'_>,
	index_ref: &IndexRef,
	access: &BTreeAccess,
	anchored: bool,
) -> std::result::Result<BranchBitmap, ControlFlow> {
	let ix = index_ref.definition();
	let mut range =
		bitmap_scan_range(bctx.ns, bctx.db, ix, access, bctx.ctx.ctx().statement_clock())
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
		drained += decode_entry_doc_ids(
			res,
			&mut docs,
			&mut missing,
			matches!(ix.index, Index::Spatial),
			access.point_filter().map(std::sync::Arc::as_ref),
		)
		.context("Failed to decode index entry doc-IDs")?;
		if !anchored && bctx.budget > 0 && drained > bctx.budget {
			return Ok(BranchBitmap::Overflow);
		}
	}
	// Entries written before the index's doc-ID format (e.g. by an older
	// binary during a rolling upgrade) carry no doc-ID; resolve them
	// through the shared `!dj` mapping instead.
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

/// Drain a single-hop reachability scope into a doc-ID bitmap: the
/// scanned-table rows adjacent to the anchor, for a graph semi-join.
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
/// - **Always budgeted**, even as the plan's anchor: an adjacency band has no selectivity guarantee
///   (a hub vertex drains unbounded), and the analyzer only ever emits this leaf as a droppable AND
///   conjunct.
/// - **A legacy-format key overflows the branch**: it carries no target, so the reachable set would
///   silently under-approximate — dropping the branch instead only widens the candidate set.
/// - **Strict mode overflows immediately**: an allow-list branch is dropped from the in-traversal
///   residual, so nothing downstream could correct a miss.
///
/// A far vertex with no doc-ID in the scanned table's space is skipped:
/// it cannot appear in any sibling index bitmap (doc-IDs are assigned
/// when index entries are written), so the intersection is unchanged.
async fn build_graph_bitmap(
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
			let keys = crate::kvs::util::scan_keys(&mut range.range, bctx.txn, INDEX_BATCH_SIZE)
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
	bctx: &BitmapBuildContext<'_>,
	index_ref: &IndexRef,
	query: &str,
	operator: &MatchesOperator,
) -> std::result::Result<RoaringTreemap, ControlFlow> {
	let index_def = index_ref.definition();
	let Index::FullText(ft_params) = &index_def.index else {
		return Err(ControlFlow::Err(anyhow::anyhow!(
			"Index '{}' is not a full-text index",
			index_def.name
		)));
	};
	let frozen_ctx = &bctx.ctx.root().ctx;
	let ikb = IndexKeyBase::new(bctx.ns, bctx.db, bctx.table.clone(), index_def.index_id);
	let fti = crate::exec::vm::descriptor::object(FullTextIndex::new(
		frozen_ctx.get_index_stores(),
		bctx.txn,
		ikb,
		ft_params,
		&frozen_ctx.config.idx.file_allowlist,
		index_def.format_version,
	))
	.await
	.context("Failed to open full-text index")?;
	let query_terms = {
		let az_fn = StreamingAnalyzerFunction::new(bctx.ctx);
		// The terms are read through the node's cache, keyed by the table's
		// catalog version, which the run reads as it reads the version a
		// compiled unit is kept under.
		crate::exec::compiles::seam(fti.extract_membership_terms(
			frozen_ctx,
			&az_fn,
			query.to_owned(),
			false,
		))
		.await
		.context("Failed to extract query terms")?
	};
	if query_terms.is_empty() {
		return Ok(RoaringTreemap::new());
	}
	Ok(FullTextIndex::merged_postings(&query_terms, operator.operator).unwrap_or_default())
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
	/// All-or-nothing mode (KNN allow-lists): a branch overflow
	/// invalidates the whole evaluation instead of being dropped. Dropping an
	/// AND child (or an AND-NOT subtraction) widens the bitmap — safe when
	/// the full WHERE is re-applied as a residual filter (fusion plans), but
	/// wrong for an allow-list whose covered conjuncts were dropped from the
	/// in-traversal residual.
	strict: bool,
}

/// What a fused bitmap plan's root yields: the candidate doc-IDs, or an
/// overflow of an unanchored root's branch budget, on which the plan's
/// streaming fallback answers instead.
pub(crate) enum FusedCandidates {
	Bitmap(RoaringTreemap),
	Overflow,
}

/// Evaluate a fused plan's root over `table`'s shared doc-ID space, as
/// the VM's bitmap scan evaluates it: `anchored` exactly when the plan has no
/// overflow fallback, so an anchored root drops overflowing conjuncts and
/// never overflows. Each node of `tally` records its metrics.
pub(crate) async fn evaluate_fusion(
	ctx: &ExecutionContext,
	table: &TableName,
	plan: &BitmapPlan,
	tally: &BitmapTally,
	anchored: bool,
) -> FlowResult<FusedCandidates> {
	evaluate_plan(ctx, table, plan, Some(tally), anchored).await
}

/// The cardinality of the exact fused bitmap `plan` over `table`, for an
/// index-only count: every branch bitmap exactly equals its predicate's truth
/// set, so no branch may be dropped and the cardinality is the count, with
/// zero record fetches. A versioned context is rejected.
pub(crate) async fn exact_bitmap_cardinality(
	ctx: &ExecutionContext,
	table: &TableName,
	plan: &BitmapPlan,
) -> std::result::Result<u64, ControlFlow> {
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
	match build_bitmap_plan(plan, &bctx, true, None).await? {
		BranchBitmap::Ready(bitmap) => Ok(bitmap.len()),
		// Unreachable with the budget disabled.
		BranchBitmap::Overflow => Err(ControlFlow::Err(anyhow::anyhow!(
			"An exact bitmap plan overflowed its branch budget"
		))),
	}
}

/// The allow-list bitmap plan `plan` yields over `table`'s shared doc-ID
/// space for a KNN search, or `None` when a branch overflows its budget.
///
/// All-or-nothing: each branch is a conjunct the search does not judge itself, so
/// none may be dropped, and the allow-list is never required for the search
/// to answer, so none is anchored. A versioned context is rejected:
/// doc-ID mappings are current-state only. Each node of `tally`, when given,
/// records its metrics.
pub(crate) async fn knn_allow_list(
	ctx: &ExecutionContext,
	table: &TableName,
	plan: &BitmapPlan,
	ef: u32,
	tally: Option<&BitmapTally>,
) -> std::result::Result<Option<RoaringTreemap>, ControlFlow> {
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
		budget: knn_branch_budget(ef),
		strict: true,
	};
	match build_bitmap_plan(plan, &bctx, false, tally).await? {
		BranchBitmap::Ready(bitmap) => Ok(Some(bitmap)),
		BranchBitmap::Overflow => Ok(None),
	}
}

/// [`evaluate_fusion`] with no tally, which records no metrics. A
/// versioned context is rejected: doc-ID mappings are current-state only.
pub(crate) async fn evaluate_fusion_plan(
	ctx: &ExecutionContext,
	table: &TableName,
	root: &BitmapPlan,
	anchored: bool,
) -> FlowResult<FusedCandidates> {
	evaluate_plan(ctx, table, root, None, anchored).await
}

async fn evaluate_plan(
	ctx: &ExecutionContext,
	table: &TableName,
	plan: &BitmapPlan,
	node: Option<&BitmapTally>,
	anchored: bool,
) -> FlowResult<FusedCandidates> {
	reject_versioned_execution(ctx)?;
	let db_ctx = ctx.database().context("a bitmap plan requires a database context")?;
	let (ns, db) = (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id);
	let txn = ctx.txn();
	let doc_ids = TableDocIds::new(ns, db, table.clone());
	let bctx = BitmapBuildContext {
		ctx,
		txn: txn.as_ref(),
		ns,
		db,
		table,
		doc_ids: &doc_ids,
		budget: *surrealdb_cnf::BITMAP_BRANCH_BUDGET,
		strict: false,
	};
	Ok(match build_bitmap_plan(plan, &bctx, anchored, node).await? {
		BranchBitmap::Ready(bitmap) => FusedCandidates::Bitmap(bitmap),
		BranchBitmap::Overflow => FusedCandidates::Overflow,
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::scan::resolved::test_support::index_ref;
	use crate::exec::operators::test_util::TestDb;
	use crate::kvs::TransactionType;
	use crate::val::Value;

	const FUSION: &str = "
		DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
		DEFINE ANALYZER blank TOKENIZERS blank;
		DEFINE INDEX n_idx ON t FIELDS n;
		DEFINE INDEX m_idx ON t FIELDS m;
		DEFINE INDEX b_ft ON t FIELDS body FULLTEXT ANALYZER blank BM25;
		DEFINE TABLE e TYPE RELATION IN t OUT t PERMISSIONS FULL;
		FOR $i IN 1..=60 {
			CREATE type::record('t', $i) SET
				n = $i % 5,
				m = $i % 3,
				body = IF $i % 2 = 0 { 'even word' } ELSE { 'odd word' };
		};
		RELATE t:1->e->t:2;
		RELATE t:1->e->t:4;
		RELATE t:1->e->t:7;
		RELATE t:3->e->t:4;
	";

	fn eq(index_ref: &IndexRef, value: i64) -> BitmapPlan {
		BitmapPlan::BTree {
			index_ref: index_ref.clone(),
			access: BTreeAccess::literal_equality(Value::from(value)),
		}
	}

	/// What a branch evaluates to, as comparable data.
	fn read(result: std::result::Result<BranchBitmap, ControlFlow>) -> Option<Vec<u64>> {
		match result.expect("evaluates") {
			BranchBitmap::Ready(bitmap) => Some(bitmap.into_iter().collect()),
			BranchBitmap::Overflow => None,
		}
	}

	/// Evaluating a bitmap plan with a tally answers what evaluating it
	/// without one answers, for every combinator, the graph leaf, an
	/// anchored root and one whose branches overflow a tiny budget, strict or
	/// not.
	#[tokio::test]
	async fn evaluating_with_a_tally_answers_what_evaluating_without_one_answers() {
		let db = TestDb::new(FUSION).await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		let n_idx = index_ref(&ctx, "t", "n_idx").await;
		let m_idx = index_ref(&ctx, "t", "m_idx").await;
		let ft_idx = index_ref(&ctx, "t", "b_ft").await;
		let text = BitmapPlan::FullText {
			index_ref: ft_idx,
			query: "even".to_owned(),
			operator: MatchesOperator {
				rf: None,
				operator: crate::expr::operator::BooleanOperator::And,
			},
		};
		let graph = BitmapPlan::Graph {
			source: RecordId::new("t".into(), 1i64),
			direction: crate::expr::Dir::Out,
			edge_tables: vec!["e".into()],
		};
		let plans = [
			eq(&n_idx, 1),
			BitmapPlan::And(vec![eq(&n_idx, 1), eq(&m_idx, 2)]),
			BitmapPlan::Or(vec![eq(&n_idx, 1), eq(&m_idx, 2)]),
			BitmapPlan::Or(Vec::new()),
			BitmapPlan::AndNot {
				base: Box::new(eq(&n_idx, 1)),
				subtract: Box::new(eq(&m_idx, 0)),
			},
			BitmapPlan::And(vec![text.clone(), eq(&m_idx, 1)]),
			BitmapPlan::Or(vec![text.clone(), eq(&n_idx, 3)]),
			BitmapPlan::And(vec![eq(&n_idx, 2), graph.clone()]),
			BitmapPlan::And(vec![
				BitmapPlan::Or(vec![eq(&n_idx, 1), eq(&n_idx, 2)]),
				BitmapPlan::AndNot {
					base: Box::new(eq(&m_idx, 1)),
					subtract: Box::new(text.clone()),
				},
			]),
		];
		let table: TableName = "t".into();
		let txn = ctx.txn();
		let db_ctx = ctx.database().expect("a database");
		let (ns, db_id) = (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id);
		let doc_ids = TableDocIds::new(ns, db_id, table.clone());
		let mut overflowed = 0;
		let mut evaluated = 0;
		for plan in &plans {
			for (budget, strict, anchored) in [
				(0, false, true),
				(0, false, false),
				(1000, false, true),
				(1000, false, false),
				(3, false, true),
				(3, false, false),
				(3, true, false),
				(1, false, false),
			] {
				let bctx = BitmapBuildContext {
					ctx: &ctx,
					txn: txn.as_ref(),
					ns,
					db: db_id,
					table: &table,
					doc_ids: &doc_ids,
					budget,
					strict,
				};
				let tally = BitmapTally::of(plan);
				let by_node = read(build_bitmap_plan(plan, &bctx, anchored, Some(&tally)).await);
				let by_plan = read(build_bitmap_plan(plan, &bctx, anchored, None).await);
				assert_eq!(
					by_plan, by_node,
					"{plan:?} budget {budget} strict {strict} anchored {anchored}"
				);
				evaluated += 1;
				if by_plan.is_none() {
					overflowed += 1;
				}
			}
		}
		assert!(overflowed > 0 && overflowed < evaluated, "{overflowed} of {evaluated} overflowed");
	}

	/// The tally records the cardinality of each node of a plan it evaluates,
	/// which evaluating the plan alone does not.
	#[tokio::test]
	async fn the_tally_records_the_cardinality_of_each_branch() {
		let db = TestDb::new(FUSION).await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		let n_idx = index_ref(&ctx, "t", "n_idx").await;
		let m_idx = index_ref(&ctx, "t", "m_idx").await;
		let plan = BitmapPlan::Or(vec![eq(&n_idx, 1), eq(&m_idx, 2)]);
		let tally = BitmapTally::of(&plan);
		let table: TableName = "t".into();
		let FusedCandidates::Bitmap(bitmap) =
			evaluate_fusion(&ctx, &table, &plan, &tally, true).await.expect("evaluates")
		else {
			panic!("an anchored root overflowed");
		};
		assert_eq!(tally.metrics.output_rows(), bitmap.len());
		let first = tally.operand(0).expect("the first operand").metrics.output_rows();
		assert_eq!(first, 12);
	}

	/// A plan reads the current state only: a versioned context is refused,
	/// with a tally or without one.
	#[tokio::test]
	async fn a_bitmap_plan_refuses_a_versioned_context() {
		let db = TestDb::new(FUSION).await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		let n_idx = index_ref(&ctx, "t", "n_idx").await;
		let plan = eq(&n_idx, 1);
		let table: TableName = "t".into();
		let versioned = ctx.clone().with_version_stamp(Some(1));
		assert!(evaluate_fusion_plan(&versioned, &table, &plan, true).await.is_err());
		assert!(
			evaluate_fusion(&versioned, &table, &plan, &BitmapTally::of(&plan), true)
				.await
				.is_err()
		);
		assert!(matches!(
			evaluate_fusion_plan(&ctx, &table, &plan, true).await,
			Ok(FusedCandidates::Bitmap(_))
		));
	}

	#[test]
	fn knn_branch_budget_scales_with_ef_and_respects_bounds() {
		let max_budget = *surrealdb_cnf::BITMAP_BRANCH_BUDGET;
		let b_small = knn_branch_budget(10);
		let b_med = knn_branch_budget(40);
		let b_large = knn_branch_budget(1_000);
		assert!(b_small <= b_med, "{b_small} <= {b_med}");
		assert!(b_med <= b_large, "{b_med} <= {b_large}");
		assert_eq!(b_large, max_budget);
		if max_budget == 250_000 {
			assert_eq!(b_small, 10 * KNN_BRANCH_BUDGET_PER_EF);
			assert_eq!(b_med, 40 * KNN_BRANCH_BUDGET_PER_EF);
			assert_eq!(knn_branch_budget(1), KNN_BRANCH_BUDGET_FLOOR);
		}
	}
}
