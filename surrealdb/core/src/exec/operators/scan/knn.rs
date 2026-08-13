//! KNN scan operator for ANN index-backed vector search.
//!
//! This operator performs approximate nearest-neighbor search using an ANN
//! index. It retrieves the top-K records closest to a query vector, ordered by
//! distance (nearest first).

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use common::future::stream::{self, Yielder};
use reblessive::TreeStack;
use roaring::RoaringTreemap;
use surrealdb_types::ToSql;

use super::bitmap::BitmapNode;
use super::common::fetch_and_filter_records_batch;
use super::pipeline::{ScanPipeline, build_field_state};
use super::resolved::ResolvedTableContext;
use crate::catalog::{DatabaseId, Distance, Error, Index, NamespaceId, VectorType};
use crate::exec::index::access_path::IndexRef;
use crate::exec::operators::{KnnTopKHeap, check_cancelled, extract_vector};
use crate::exec::permission::{
	PhysicalPermission, PhysicalTableSelect, convert_permission_to_physical_runtime,
	should_check_perms, validate_record_user_access,
};
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, EvalContext, ExecOperator, ExecutionContext,
	FlowResult, OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream, monitor_stream,
};
use crate::expr::{Cond, ControlFlow, ControlFlowExt, Idiom};
use crate::iam::Action;
use crate::idx::docids::TableDocIds;
use crate::idx::trees::gate::{CachedTableSelect, CandidateFetchCounter};
use crate::idx::trees::vector::{score_raw_vector, typed_query_vector};
use crate::idx::trees::{KnnCondFilter, KnnIteratorResult};
use crate::kvs::{CachePolicy, Transaction};
use crate::legacy::knn::LegacyCondition;
use crate::val::{Number, TableName, Value};

/// Execution strategy chosen by the KNN prefilter triage (#548), keyed on
/// the allow-list cardinality.
///
/// Stored as a `u8` on the operator after execution so EXPLAIN ANALYZE can
/// render the tier actually taken (`0` = no prefilter ran).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum PrefilterTier {
	/// The allow-list build overflowed its branch budget (or a versioned
	/// execution reached the operator): run the pre-prefilter path with the
	/// full residual evaluated in-traversal.
	Fallback = 1,
	/// Graph-free exact scoring over the allow-list members.
	Exact = 2,
	/// Allow-list-gated graph traversal with boosted ef.
	Graph = 3,
	/// Allow-list-gated graph traversal without ef boost: above the boost
	/// threshold the allow-list barely rejects candidates on typical tables,
	/// so no recall compensation is needed — and when the list IS still
	/// selective relative to a huge table, gated traversal stays correct and
	/// merely walks further. (An unrestricted-search-plus-post-check tier was
	/// deliberately rejected: an absolute cardinality says nothing about how
	/// near-universe the predicate is, and post-checking truncates the result
	/// set when the nearest overall neighbours are excluded.)
	GraphUnboosted = 4,
}

impl PrefilterTier {
	/// EXPLAIN label for a stored tier code (`0` = not run ⇒ `None`).
	pub(crate) fn label(code: u8) -> Option<&'static str> {
		match code {
			1 => Some("fallback"),
			2 => Some("exact"),
			3 => Some("graph"),
			4 => Some("graph_unboosted"),
			_ => None,
		}
	}
}

/// Pick the execution strategy from the allow-list cardinality (#548).
///
/// Thresholds are passed in (rather than read from the process-global cnf
/// statics) so the decision table is unit-testable.
pub(crate) fn choose_prefilter_tier(allow_len: u64, t_exact: u64, t_boost: u64) -> PrefilterTier {
	// Includes the empty allow-list: the exact tier over zero members
	// returns no rows without touching the graph — correct, since no record
	// satisfies the covered conjuncts.
	if allow_len <= t_exact {
		return PrefilterTier::Exact;
	}
	if allow_len <= t_boost {
		PrefilterTier::Graph
	} else {
		PrefilterTier::GraphUnboosted
	}
}

/// Boosted ef for allow-list-gated traversal: `ef·boost` capped at `ef_max`,
/// never below the user-requested `ef` (#548). A selective allow-list rejects
/// most candidates, effectively thinning the graph; widening the search
/// counteracts the recall loss.
pub(crate) fn boosted_ef(ef: u32, boost: u32, ef_max: u32) -> u32 {
	ef.saturating_mul(boost).min(ef_max).max(ef)
}

/// Operator-side pre-filter for a KNN scan (#548, pre-filtered vector
/// search).
///
/// Built by the planner from [`KnnPrefilterPlan`]: `node` evaluates — at
/// execute time, against the scan's transaction — into an allow-list
/// `RoaringTreemap` over the table's shared doc-ID space whose members
/// exactly satisfy the covered WHERE conjuncts. `residual` carries the
/// uncovered conjuncts for the in-traversal record-fetch filter, and
/// `residual_phys` the same predicate compiled for batched row evaluation on
/// the graph-free exact tier.
///
/// [`KnnPrefilterPlan`]: crate::exec::index::access_path::KnnPrefilterPlan
#[derive(Debug, Clone)]
pub(crate) struct KnnPrefilter {
	/// The bitmap tree producing the allow-list.
	pub(crate) node: Arc<BitmapNode>,
	/// The same node coerced to `dyn ExecOperator`, so
	/// [`ExecOperator::children`] can hand out a reference for EXPLAIN.
	pub(crate) node_dyn: Arc<dyn ExecOperator>,
	/// The true residual: KNN-stripped conjuncts not covered by `node`,
	/// never containing a MATCHES operator.
	pub(crate) residual: Option<Cond>,
	/// `residual` compiled to a physical expression for per-row evaluation
	/// outside the traversal (exact tier).
	pub(crate) residual_phys: Option<Arc<dyn PhysicalExpr>>,
}

/// KNN scan operator using an ANN index.
///
/// Executes an approximate nearest-neighbor search against an ANN index
/// and returns the top-K matching records ordered by distance.
#[derive(Debug)]
pub struct KnnScan {
	/// Reference to the ANN index definition
	pub index_ref: IndexRef,
	/// The query vector to search for nearest neighbors of
	pub vector: Vec<Number>,
	/// Number of nearest neighbors to return
	pub k: u32,
	/// ANN search expansion factor
	pub ef: u32,
	/// Table name for record fetching
	pub table_name: surrealdb_strand::TableName,
	/// Optional VERSION timestamp for time-travel queries.
	pub(crate) version: Option<Arc<dyn PhysicalExpr>>,
	/// Plan-time resolved table context. When present, `execute()` skips
	/// runtime table def + permission lookup.
	pub(crate) resolved: Option<ResolvedTableContext>,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
	/// KNN distance context, shared with IndexFunctionExec for vector::distance::knn().
	pub(crate) knn_context: Option<Arc<crate::exec::function::KnnContext>>,
	/// Residual WHERE condition (non-KNN, non-MATCHES predicates) to push
	/// down into ANN search. When present, the ANN search will only consider
	/// candidates that satisfy this condition, preventing non-matching rows
	/// from consuming top-K slots.
	///
	/// SECURITY: the cond is evaluated against raw stored records inside the
	/// ANN search (`idx/trees/{hnsw,diskann}/filter.rs::is_record_truthy`),
	/// before any SELECT pipeline filtering. The permission gate that keeps
	/// this safe lives at that chokepoint, which applies the table's SELECT
	/// permission to each candidate BEFORE invoking the cond — so hidden
	/// rows are skipped pre-cond and a record user cannot use the cond to
	/// probe their field values. On this path the gate is the operator's own
	/// resolved `PhysicalPermission`, handed down via
	/// `KnnCondFilter::select_gate` — the same object that filters the
	/// fetched batch after the search. Preserve that ordering when touching
	/// `is_record_truthy`; see the SECURITY note there for the threat model.
	///
	/// The prefilter (#548) never relaxes this contract: its allow-list
	/// bitmap is built purely from index entries and posting lists — row
	/// permissions are never bitmap-evaluated — and whatever cond is pushed
	/// down (this full residual on the fallback path, or the prefilter's
	/// true residual) keeps the per-candidate permission-before-cond order.
	pub(crate) residual_cond: Option<Cond>,
	/// Pre-filtered vector search (#548): evaluates into the allow-list
	/// bitmap gating candidate admission, with the true residual for the
	/// remaining predicates. `None` keeps the pure in-traversal filter path.
	pub(crate) prefilter: Option<KnnPrefilter>,
	/// The [`PrefilterTier`] actually taken, recorded during `execute()` for
	/// EXPLAIN ANALYZE (`0` = no prefilter ran).
	pub(crate) tier: Arc<std::sync::atomic::AtomicU8>,
	/// Projection-aware field set for computed-field materialization.
	/// Outer `None` = sub-operator mode (parent handles fields).
	/// `Some(None)` = all fields, `Some(Some(set))` = specific fields.
	pub(crate) needed_fields: Option<Option<HashSet<String>>>,
}

impl KnnScan {
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		index_ref: IndexRef,
		vector: Vec<Number>,
		k: u32,
		ef: u32,
		table_name: surrealdb_strand::TableName,
		version: Option<Arc<dyn PhysicalExpr>>,
		knn_context: Option<Arc<crate::exec::function::KnnContext>>,
		residual_cond: Option<Cond>,
		prefilter: Option<KnnPrefilter>,
		needed_fields: Option<Option<HashSet<String>>>,
	) -> Self {
		Self {
			index_ref,
			vector,
			k,
			ef,
			table_name,
			version,
			resolved: None,
			metrics: Arc::new(OperatorMetrics::new()),
			knn_context,
			residual_cond,
			prefilter,
			tier: Arc::new(std::sync::atomic::AtomicU8::new(0)),
			needed_fields,
		}
	}

	/// Set the plan-time resolved table context.
	pub(crate) fn with_resolved(mut self, resolved: ResolvedTableContext) -> Self {
		self.resolved = Some(resolved);
		self
	}
}
impl KnnScan {
	/// Operator name, shared with the planner's KNN-source detection
	/// (`source_contains_knn` in `planner/select`).
	pub(crate) const NAME: &'static str = "KnnScan";
}

impl ExecOperator for KnnScan {
	fn name(&self) -> &'static str {
		Self::NAME
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![
			("index".to_string(), self.index_ref.name.to_string()),
			("k".to_string(), self.k.to_string()),
			("ef".to_string(), self.ef.to_string()),
			("dimension".to_string(), self.vector.len().to_string()),
		];
		// Surface the residual WHERE that is pushed into the ANN search as a
		// cond filter, so EXPLAIN shows it on the KnnScan line — mirroring how
		// `TableScan` exposes its `predicate`. Without this, an indexed KNN
		// with a pushed-down filter is indistinguishable in the plan from one
		// without. Render the inner expression rather than the `Cond` to avoid
		// a redundant `WHERE` prefix (matching TableScan's `to_sql()` output).
		// With a prefilter (#548) this is the TRUE residual — the covered
		// conjuncts render as the bitmap child subtree instead.
		let shown_cond = match &self.prefilter {
			Some(p) => p.residual.as_ref(),
			None => self.residual_cond.as_ref(),
		};
		if let Some(cond) = shown_cond {
			attrs.push(("predicate".to_string(), cond.0.to_sql()));
		}
		// The prefilter tier actually taken, recorded during execution —
		// present under EXPLAIN ANALYZE (which drains the plan before
		// formatting), absent in a plain EXPLAIN. Deterministic for a given
		// dataset and thresholds, so language tests can assert it.
		if let Some(label) =
			PrefilterTier::label(self.tier.load(std::sync::atomic::Ordering::Relaxed))
		{
			attrs.push(("prefilter_tier".to_string(), label.to_string()));
		}
		attrs
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		// The prefilter's bitmap tree renders as a child subtree
		// (BitmapAnd / BitmapIndexScan / BitmapFullTextScan ...), mirroring
		// BitmapResolve (#547); its nodes record bitmap cardinalities as
		// `rows` under EXPLAIN ANALYZE.
		match &self.prefilter {
			Some(p) => vec![&p.node_dyn],
			None => vec![],
		}
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::Bounded(self.k as usize)
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let db_ctx = ctx.database()?.clone();

		// Validate record user has access to this namespace/database
		validate_record_user_access(&db_ctx)?;

		// Check if we need to enforce permissions
		let check_perms = should_check_perms(&db_ctx, Action::View)?;

		// Clone for the async block
		let index_ref = self.index_ref.clone();
		let vector = self.vector.clone();
		let k = self.k;
		let ef = self.ef;
		let table_name = self.table_name.clone();
		let version_expr = self.version.clone();
		let knn_context = self.knn_context.clone();
		let residual_cond = self.residual_cond.clone();
		let prefilter = self.prefilter.clone();
		let tier_cell = Arc::clone(&self.tier);
		let op_metrics = Arc::clone(&self.metrics);
		let resolved = self.resolved.clone();
		let needed_fields = self.needed_fields.clone();
		let ctx = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			// Get namespace and database context
			let db_ctx = ctx.database().context("KnnScan requires database context")?;
			let ns = Arc::clone(&db_ctx.ns_ctx.ns);
			let db = Arc::clone(&db_ctx.db);
			let txn = ctx.txn();

			// Evaluate VERSION expression
			let version: Option<u64> = match &version_expr {
				Some(expr) => {
					let eval_ctx = crate::exec::EvalContext::from_exec_ctx(&ctx);
					let v = expr.evaluate(eval_ctx).await?;
					Some(
						v.cast_to::<crate::val::Datetime>()
							.map_err(|e| anyhow::anyhow!("{e}"))?
							.to_version_stamp(txn.timestamp_impl().as_ref())?,
					)
				}
				None => ctx.version_stamp(),
			};

			// Get the FrozenContext from the root context
			let root = ctx.root();
			let frozen_ctx = &root.ctx;

			// Resolve table permissions and table_id: plan-time fast path or runtime fallback
			let (select_permission, table_id) = if let Some(ref res) = resolved {
				let perm = res.select_permission(check_perms);
				(perm, res.table_def.table_id)
			} else {
				let table_def = db_ctx
					.get_table_def(&table_name, version)
					.await
					.context("Failed to get table")?;

				let table_def = match table_def {
					Some(def) => def,
					None => {
						Err(ControlFlow::Err(anyhow::Error::new(Error::TbNotFound {
							name: table_name.clone(),
						})))?;
						unreachable!()
					}
				};

				let select_permission = if check_perms {
					convert_permission_to_physical_runtime(&table_def.permissions.select, &ctx)
						.await
						.context("Failed to convert permission")?
				} else {
					PhysicalPermission::Allow
				};
				(select_permission, table_def.table_id)
			};

			// Early exit if denied
			if matches!(select_permission, PhysicalPermission::Deny) {
				return Ok(());
			}

			// Resolve field state for computed fields and field-level
			// permissions. When needed_fields is None (sub-operator mode),
			// the parent operator handles field processing.
			let field_state = match &needed_fields {
				Some(nf) => {
					if let Some(ref res) = resolved {
						res.field_state_for_projection(nf.as_ref())
					} else {
						build_field_state(&ctx, &table_name, check_perms, nf.as_ref()).await?
					}
				}
				None => super::pipeline::FieldState::empty(),
			};

			// Get the ANN parameters from the index definition
			let index_def = index_ref.definition();
			// Reject an HNSW/DiskANN index whose on-disk format predates the
			// shared table-level doc-ID space; it must be rebuilt before it can
			// serve KNN searches. Mirrors the plan-time gate in
			// idx/planner/tree.rs.
			index_def.ensure_current_format()?;
			let ikb = crate::idx::IndexKeyBase::new(
				ns.namespace_id,
				db.database_id,
				index_def.table_name.clone(),
				index_def.index_id,
			);

			// Pre-filtered vector search (#548): evaluate the covered WHERE
			// conjuncts into an allow-list bitmap over the table's shared
			// doc-ID space, then triage the execution strategy on its
			// cardinality. On the fallback path (build overflow) the FULL
			// residual keeps being evaluated in-traversal, exactly as without
			// a prefilter.
			let mut tier: Option<PrefilterTier> = None;
			let mut allow_list: Option<RoaringTreemap> = None;
			let mut in_traversal_cond: Option<Cond> = residual_cond;
			if let Some(pf) = &prefilter {
				if version.is_some() {
					// The planner never attaches a prefilter to a versioned
					// query (doc-ID mappings are not time-travel-aware);
					// should one slip through, ignore it rather than serve
					// current-state candidates for a historical query.
					tier = Some(PrefilterTier::Fallback);
				} else {
					match pf
						.node
						.build_allowlist(&ctx, &table_name, *surrealdb_cnf::BITMAP_BRANCH_BUDGET)
						.await?
					{
						Some(bm) => {
							let t = choose_prefilter_tier(
								bm.len(),
								*surrealdb_cnf::KNN_PREFILTER_EXACT_THRESHOLD,
								*surrealdb_cnf::KNN_PREFILTER_EF_BOOST_THRESHOLD,
							);
							in_traversal_cond = pf.residual.clone();
							allow_list = Some(bm);
							tier = Some(t);
						}
						None => {
							tier = Some(PrefilterTier::Fallback);
						}
					}
				}
			}
			tier_cell.store(tier.map_or(0, |t| t as u8), std::sync::atomic::Ordering::Relaxed);

			let knn_results = if matches!(tier, Some(PrefilterTier::Exact)) {
				// Exact tier: no graph access — resolve, fetch and score the
				// allow-list members directly (bounded by the exact-tier
				// threshold), reusing the brute-force KnnTopK machinery.
				let allow = allow_list.as_ref().expect("exact tier implies an allow-list");
				let (distance, vector_type, dimension) = match &index_def.index {
					Index::Hnsw(p) => (p.distance.clone(), p.vector_type, p.dimension as usize),
					#[cfg(diskann)]
					Index::DiskAnn(p) => (p.distance.clone(), p.vector_type, p.dimension as usize),
					// Keep "DiskANN indexes are unusable on this platform" an
					// invariant: even though graph-free exact scoring never
					// touches the DiskANN graph and *could* serve this query,
					// answering here while the graph tiers (and the unfiltered
					// path) error would make the same query succeed or fail
					// depending on the filter's selectivity.
					#[cfg(not(diskann))]
					Index::DiskAnn(_) => {
						Err(ControlFlow::Err(anyhow::anyhow!(
							"DISKANN indexes require a 64-bit, non-WASM platform"
						)))?;
						unreachable!()
					}
					_ => {
						Err(ControlFlow::Err(anyhow::anyhow!(
							"Index '{}' is not an ANN index",
							index_def.name
						)))?;
						unreachable!()
					}
				};
				let field = match index_def.cols.first() {
					Some(f) => f.clone(),
					None => {
						Err(ControlFlow::Err(anyhow::anyhow!(
							"Index '{}' has no indexed column",
							index_def.name
						)))?;
						unreachable!()
					}
				};
				exact_prefiltered_knn(
					&ctx,
					&txn,
					ns.namespace_id,
					db.database_id,
					&table_name,
					&field,
					&distance,
					vector_type,
					dimension,
					&vector,
					k as usize,
					allow,
					prefilter.as_ref().and_then(|p| p.residual_phys.as_ref()),
					&select_permission,
					check_perms,
				)
				.await?
			} else {
				// Graph tiers: gate admission on the allow-list (with boosted
				// ef on the mid-selectivity tier); the fallback/no-prefilter
				// paths search unrestricted.
				let (ef_eff, search_allow) = match tier {
					Some(PrefilterTier::Graph) => (
						boosted_ef(
							ef,
							*surrealdb_cnf::KNN_PREFILTER_EF_BOOST,
							*surrealdb_cnf::KNN_PREFILTER_EF_MAX,
						),
						allow_list.as_ref(),
					),
					Some(PrefilterTier::GraphUnboosted) => (ef, allow_list.as_ref()),
					_ => (ef, None),
				};
				// Pushed-down residual WHERE: gate every candidate inside the ANN
				// search on the same physical SELECT permission that filters the
				// fetched batch below, so hidden rows are skipped before the cond
				// can probe them and never consume top-K slots (see the
				// `residual_cond` docs for the threat model). With a fully-covered
				// WHERE (no in-traversal cond) but a per-record SELECT permission,
				// a permission-only filter (cond: None) keeps hidden rows from
				// consuming slots; with unconditional permissions and no cond, no
				// filter is built at all — admission is pure bitmap membership,
				// with zero record fetches inside the traversal.
				let needs_perm_gate = !matches!(select_permission, PhysicalPermission::Allow);
				let select_gate = || {
					CachedTableSelect::Gate(Arc::new(PhysicalTableSelect::new(
						select_permission.clone(),
						ctx.clone(),
					)))
				};
				let fetch_counter = || Arc::clone(&op_metrics) as Arc<dyn CandidateFetchCounter>;
				let cond_filter = match (&in_traversal_cond, ctx.options()) {
					(Some(cond), Some(opt)) => Some(KnnCondFilter {
						select_gate: select_gate(),
						cond: Some(Arc::new(LegacyCondition::new(
							frozen_ctx,
							opt,
							Arc::new(cond.clone()),
						))),
						metrics: Some(fetch_counter()),
					}),
					(None, Some(_)) if needs_perm_gate && search_allow.is_some() => {
						Some(KnnCondFilter {
							select_gate: select_gate(),
							cond: None,
							metrics: Some(fetch_counter()),
						})
					}
					_ => None,
				};
				match &index_def.index {
					Index::Hnsw(hnsw_params) => {
						// Obtain the shared HNSW index
						let hnsw_index = frozen_ctx
							.get_index_stores()
							.get_index_hnsw(frozen_ctx, table_id, &ikb, hnsw_params)
							.await
							.context("Failed to get HNSW index")?;

						// Ensure the HNSW index state is current
						hnsw_index
							.check_state(frozen_ctx)
							.await
							.context("Failed to check HNSW index state")?;

						let mut stack = TreeStack::new();
						stack
							.enter(|stk| {
								let hnsw_index = &hnsw_index;
								let vector = &vector;
								async move {
									hnsw_index
										.knn_search(
											frozen_ctx,
											stk,
											vector,
											k as usize,
											ef_eff as usize,
											cond_filter,
											search_allow,
										)
										.await
								}
							})
							.finish()
							.await
							.context("HNSW KNN search failed")?
					}
					#[cfg(diskann)]
					Index::DiskAnn(diskann_params) => {
						let diskann_index = frozen_ctx
							.get_index_stores()
							.get_index_diskann(table_id, &ikb, diskann_params)
							.await
							.context("Failed to get DiskANN index")?;

						diskann_index
							.check_state()
							.await
							.context("Failed to check DiskANN index state")?;

						let mut stack = TreeStack::new();
						stack
							.enter(|stk| {
								let diskann_index = &diskann_index;
								let vector = &vector;
								async move {
									diskann_index
										.knn_search(
											frozen_ctx,
											stk,
											vector,
											k as usize,
											ef_eff as usize,
											cond_filter,
											search_allow,
										)
										.await
								}
							})
							.finish()
							.await
							.context("DiskANN KNN search failed")?
					}
					#[cfg(not(diskann))]
					Index::DiskAnn(_) => {
						Err(ControlFlow::Err(anyhow::anyhow!(
							"DISKANN indexes require a 64-bit, non-WASM platform"
						)))?;
						unreachable!()
					}
					_ => {
						Err(ControlFlow::Err(anyhow::anyhow!(
							"Index '{}' is not an ANN index",
							index_def.name
						)))?;
						unreachable!()
					}
				}
			};

			let mut rids = Vec::with_capacity(knn_results.len());
			// Populate KNN distance context (if present) before yielding records.
			// This makes distances available to vector::distance::knn() during
			// downstream projection evaluation.
			if let Some(ref knn_ctx) = knn_context {
				for (rid, distance, _) in &knn_results {
					knn_ctx.insert(rid.as_ref().clone(), Number::Float(*distance)).await;
					rids.push(rid.as_ref().clone());
				}
			} else {
				for (rid, _, _) in &knn_results {
					rids.push(rid.as_ref().clone());
				}
			}

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

			// Batch-fetch all records and apply permission filtering
			let mut values = fetch_and_filter_records_batch(
				&ctx,
				&txn,
				ns.namespace_id,
				db.database_id,
				&rids,
				&select_permission,
				check_perms,
				version,
				CachePolicy::ReadWrite,
			)
			.await?;

			pipeline.process_batch(&mut values, &ctx).await?;

			if !values.is_empty() {
				yielder.emit(ValueBatch::new(values)).await;
			}
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "KnnScan", &self.metrics))
	}
}

/// Number of allow-list members resolved and fetched per batch on the exact
/// tier (mirrors `BitmapResolve`'s resolve batching).
const EXACT_TIER_BATCH_SIZE: usize = 1000;

/// Tier [`PrefilterTier::Exact`] of the KNN prefilter triage (#548): exact
/// scoring over the allow-list members, with no graph access.
///
/// Iterates the bitmap in batches: doc-IDs resolve to record IDs through the
/// table's shared doc-ID space (dangling entries for deleted records are
/// skipped, mirroring `BitmapResolve`), records are fetched and
/// permission-filtered — `fetch_and_filter_records_batch` applies the table
/// SELECT permission BEFORE the residual predicate below, preserving the
/// permission-before-cond contract of
/// `idx/trees/hnsw/filter.rs::is_record_truthy` — then the true residual is
/// evaluated per row, and only survivors are scored into a bounded top-k
/// heap, so rejected rows never consume top-K slots. `CachePolicy::ReadWrite`
/// warms the transaction record cache, making the shared result tail's
/// re-fetch of the k winners a cache hit.
///
/// Bounded by the exact-tier threshold in record fetches and — unlike graph
/// traversal under a highly selective filter — guaranteed to find the true
/// top-k. Distances are computed on raw record values (brute-force
/// semantics, matching the recall ground truth); reduced-precision index
/// vector types may show ulp-level differences vs graph-tier distances. Ties
/// break by allow-list (doc-ID) order, matching the graph result builder's
/// `(distance, doc-id)` ordering.
#[expect(clippy::too_many_arguments)]
async fn exact_prefiltered_knn(
	ctx: &ExecutionContext,
	txn: &Arc<Transaction>,
	ns: NamespaceId,
	db: DatabaseId,
	table_name: &TableName,
	field: &Idiom,
	distance: &Distance,
	vector_type: VectorType,
	dimension: usize,
	query_vector: &[Number],
	k: usize,
	allow: &RoaringTreemap,
	residual_phys: Option<&Arc<dyn PhysicalExpr>>,
	select_permission: &PhysicalPermission,
	check_perms: bool,
) -> std::result::Result<VecDeque<KnnIteratorResult>, ControlFlow> {
	// Score through the index's vector type, exactly as the graph search
	// scores stored vectors, so reduced-precision index types rank
	// identically across tiers.
	let query = typed_query_vector(vector_type, dimension, query_vector)
		.context("Invalid KNN query vector")?;
	let doc_ids = TableDocIds::new(ns, db, table_name.clone());
	let mut heap: KnnTopKHeap<Arc<crate::val::RecordId>> = KnnTopKHeap::new(k);
	let mut iter = allow.iter();
	loop {
		check_cancelled(ctx)?;
		let chunk: Vec<u64> = iter.by_ref().take(EXACT_TIER_BATCH_SIZE).collect();
		if chunk.is_empty() {
			break;
		}
		let keys = doc_ids
			.get_record_ids_batch(txn.as_ref(), &chunk)
			.await
			.context("Failed to resolve doc-IDs to record IDs")?;
		let mut rids = Vec::with_capacity(keys.len());
		for key in keys.into_iter().flatten() {
			rids.push(crate::val::RecordId {
				table: table_name.clone(),
				key,
			});
		}
		// Fetch + table SELECT permission gate (permission strictly before
		// the residual and before any top-K slot is occupied).
		let mut values = fetch_and_filter_records_batch(
			ctx,
			txn,
			ns,
			db,
			&rids,
			select_permission,
			check_perms,
			None,
			CachePolicy::ReadWrite,
		)
		.await?;
		// True residual: keep only rows satisfying the uncovered conjuncts.
		if let Some(phys) = residual_phys {
			let eval_ctx = EvalContext::from_exec_ctx(ctx);
			let results = phys.evaluate_batch(eval_ctx, &values).await?;
			let mut kept = Vec::with_capacity(values.len());
			for (value, res) in values.into_iter().zip(results) {
				if res.is_truthy() {
					kept.push(value);
				}
			}
			values = kept;
		}
		for value in values {
			// Score the record's vector field against the query vector.
			let Some(record_vec) = extract_vector(&value, field) else {
				continue;
			};
			let Some(dist) =
				score_raw_vector(distance, vector_type, dimension, &query, &record_vec)
			else {
				continue;
			};
			let dist = Number::Float(dist);
			let Value::Object(ref obj) = value else {
				continue;
			};
			let Some(Value::RecordId(rid)) = obj.get("id") else {
				continue;
			};
			heap.offer(dist, Arc::new(rid.clone()));
		}
	}
	let mut res = VecDeque::with_capacity(k);
	for (dist, rid) in heap.into_sorted_nearest_first() {
		res.push_back((rid, dist.to_float(), None));
	}
	Ok(res)
}

#[cfg(test)]
mod tests {
	use super::{PrefilterTier, boosted_ef, choose_prefilter_tier};

	/// The full decision table of the prefilter triage (#548): every tier is
	/// allow-list-gated, differing only in graph use and ef boosting.
	#[test]
	fn prefilter_tier_decision_table() {
		use PrefilterTier::*;
		const T_EXACT: u64 = 2_000;
		const T_BOOST: u64 = 100_000;
		let choose = |len| choose_prefilter_tier(len, T_EXACT, T_BOOST);
		// Empty allow-list short-circuits through the exact tier.
		assert_eq!(choose(0), Exact);
		// Boundaries are inclusive on the lower tier.
		assert_eq!(choose(T_EXACT), Exact);
		assert_eq!(choose(T_EXACT + 1), Graph);
		assert_eq!(choose(T_BOOST), Graph);
		// Above the boost threshold, gated traversal continues without ef
		// boost — never an unrestricted search (post-checking would truncate
		// results whenever the nearest overall neighbours are excluded).
		assert_eq!(choose(T_BOOST + 1), GraphUnboosted);
		assert_eq!(choose(u64::MAX), GraphUnboosted);
	}

	/// ef boosting saturates, respects the cap, and never clamps below the
	/// user-requested ef.
	#[test]
	fn boosted_ef_bounds() {
		// Standard boost.
		assert_eq!(boosted_ef(40, 4, 1_024), 160);
		// Cap applies.
		assert_eq!(boosted_ef(400, 4, 1_024), 1_024);
		// The cap never reduces a user ef already above it.
		assert_eq!(boosted_ef(2_000, 4, 1_024), 2_000);
		// A zero boost disables widening but keeps the user ef.
		assert_eq!(boosted_ef(40, 0, 1_024), 40);
		// Saturating multiply.
		assert_eq!(boosted_ef(u32::MAX, 2, u32::MAX), u32::MAX);
	}

	/// Tier codes round-trip through the EXPLAIN label mapping.
	#[test]
	fn prefilter_tier_labels() {
		assert_eq!(PrefilterTier::label(0), None);
		assert_eq!(PrefilterTier::label(PrefilterTier::Fallback as u8), Some("fallback"));
		assert_eq!(PrefilterTier::label(PrefilterTier::Exact as u8), Some("exact"));
		assert_eq!(PrefilterTier::label(PrefilterTier::Graph as u8), Some("graph"));
		assert_eq!(
			PrefilterTier::label(PrefilterTier::GraphUnboosted as u8),
			Some("graph_unboosted")
		);
		assert_eq!(PrefilterTier::label(5), None);
	}
}
