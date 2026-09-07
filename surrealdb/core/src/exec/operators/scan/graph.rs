//! Graph edge scanning operator for the streaming execution engine.
//!
//! This operator scans graph edges based on a source record, direction, and
//! target edge tables. It is used to implement graph traversal idioms like
//! `person:alice->knows->person`.

use std::borrow::Cow;
use std::sync::Arc;

use common::future::stream::{self, Yielder};
use futures::StreamExt;
use surrealdb_datastore::values::inline_cache::{CacheValue, EdgeCacheEntry};

use super::common::{extract_record_ids_into, resolve_record_batch, resolve_version_stamp};
// Re-exported so the existing `pub use graph::{EdgeTableSpec, ...}` surface is
// unchanged after the key-scan machinery moved into `graph_keys`.
pub use super::graph_keys::EdgeTableSpec;
use super::graph_keys::{compute_graph_ranges, observe_fold_candidate};
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId, TableDefinition};
use crate::exec::parts::LookupDirection;
use crate::exec::permission::{
	PhysicalPermission, should_check_perms, validate_record_user_access,
};
use crate::exec::{
	AccessMode, ContextLevel, ControlFlowExt, EvalContext, ExecOperator, ExecutionContext,
	FlowResult, OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream, buffer_stream,
	monitor_stream,
};
use crate::expr::{ControlFlow, Dir};
use crate::iam::Action;
use crate::idx::adjacency::{
	AdjacencyScope, MergedAdjacencyCursor, VertexAdjacency, table_adjacency,
};
use crate::key::Resumable;
use crate::key::schema::EdgeCacheKey;
use crate::kvs::{CachePolicy, Direction, Transaction};
use crate::val::{RecordId, RecordIdKey, TableName, Value};

/// Plan-time snapshot of the inline-field state a payload-resident
/// predicate was admitted under: per edge table, the inline generation and
/// the complete inline field set in canonical (payload) order.
///
/// The snapshot is the trust anchor at read time: a payload whose stamped
/// generation or value count differs was written under a different field
/// set and falls back to the record — the predicate is never evaluated
/// against values that mean something else than the plan assumed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PayloadPredicateSpec {
	pub(crate) tables: std::collections::HashMap<TableName, PayloadTableSpec>,
}

/// One table's inline-field snapshot inside a [`PayloadPredicateSpec`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PayloadTableSpec {
	/// The table's inline generation at plan time.
	pub(crate) generation: u32,
	/// The raw names of every inline field, in the canonical payload order.
	pub(crate) fields: Vec<String>,
}

/// What kind of output the GraphEdgeScan should produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GraphScanOutput {
	/// Return the target record IDs (e.g., `person:bob`)
	#[default]
	TargetId,
	/// Return the full edge records (fetched from the datastore)
	FullEdge,
	/// Skip the edge entirely and return the final target vertex.
	///
	/// Used when a `->edge->vertex` traversal touches no edge data and the
	/// edge table has no permissions / events that need to gate access.
	/// New-format adjacency keys carry the target vertex directly; legacy
	/// keys trigger an inline fallback scan of the edge's own adjacency to
	/// recover the target, preserving correctness on unmigrated data.
	TargetVertex,
}

/// Scans graph edges for records received from a child operator stream.
///
/// This operator implements a nested-loop-join pattern: it reads RecordIds from
/// its `input` child operator stream, then for each RecordId scans graph edges
/// in the specified direction and target tables. It produces a stream of either
/// target IDs or full edge records depending on the output mode.
///
/// This forms part of a streaming DAG where the data flows explicitly:
/// ```text
/// CurrentValueSource → GraphEdgeScan("knows") → GraphEdgeScan("person")
/// ```
#[derive(Debug, Clone)]
pub struct GraphEdgeScan {
	/// Child operator that provides source RecordId(s)
	pub(crate) input: Arc<dyn ExecOperator>,

	/// Direction of the edge traversal (In = `<-`, Out = `->`, Both = `<->`)
	pub(crate) direction: LookupDirection,

	/// Target edge table(s) to scan, optionally with range bounds.
	/// If empty, scans all edge tables in that direction.
	pub(crate) edge_tables: Vec<EdgeTableSpec>,

	/// What to output: EdgeId, TargetId, or FullEdge
	pub(crate) output_mode: GraphScanOutput,

	/// Filter on the final vertex table(s) for `TargetVertex` mode.
	///
	/// Populated by the planner when collapsing `->edge->vertex` into a
	/// single scan. New-format keys whose embedded target table does not
	/// match this list are skipped; legacy keys trigger an inline fallback
	/// that scans the edge's adjacency restricted to these tables. The
	/// planner only produces this scan when the next-hop vertex tables are
	/// known (see `Planner::try_fast_path_pair`), so this is always
	/// non-empty in practice; an empty list is treated defensively as
	/// "match any target", but no current call site constructs that case.
	pub(crate) target_tables: Vec<TableName>,

	/// Optional VERSION timestamp for time-travel queries.
	pub(crate) version: Option<Arc<dyn PhysicalExpr>>,

	/// Optional limit on the total number of edges yielded per source record.
	/// When set, edge scanning stops early after this many results.
	pub(crate) limit: Option<usize>,

	/// Optional `WHERE` predicate pushed down from the graph lookup's `cond`,
	/// replacing the downstream `Filter` operator. Rows for which this
	/// evaluates falsy are dropped inside the scan.
	pub(crate) predicate: Option<Arc<dyn PhysicalExpr>>,

	/// Whether `predicate` is "key-resident": it references only fields
	/// decodable from the adjacency key (currently `id`), is free of
	/// subqueries / functions / side effects, and can therefore be evaluated
	/// against a synthesized value *before* fetching the edge record. When
	/// `false` (and `predicate` is `Some`), the predicate is
	/// "record-referencing" and is evaluated after fetch + permission
	/// filtering. Set once at plan time; see
	/// `Planner::graph_predicate_is_key_resident`.
	pub(crate) predicate_key_resident: bool,

	/// When set, `predicate` is payload-resident: every field it references
	/// is inlined into the adjacency payloads of every scanned edge table,
	/// so it is evaluated against the synthesized `{id, in, out, …inline}`
	/// object before any fetch — a falsy edge is dropped without touching
	/// its record. Edges whose payload is unusable (absent, spilled, or
	/// written under a different generation) fall back per flush to the
	/// record path, where the same predicate is applied after fetch.
	pub(crate) payload_spec: Option<Arc<PayloadPredicateSpec>>,

	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl GraphEdgeScan {
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		direction: LookupDirection,
		edge_tables: Vec<EdgeTableSpec>,
		output_mode: GraphScanOutput,
		version: Option<Arc<dyn PhysicalExpr>>,
	) -> Self {
		Self {
			input,
			direction,
			edge_tables,
			output_mode,
			target_tables: Vec::new(),
			version,
			limit: None,
			predicate: None,
			predicate_key_resident: false,
			payload_spec: None,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	pub(crate) fn with_limit(mut self, limit: usize) -> Self {
		self.limit = Some(limit);
		self
	}

	/// Configure the next-hop target vertex tables. Only meaningful when
	/// `output_mode == TargetVertex`; ignored otherwise.
	pub(crate) fn with_target_tables(mut self, tables: Vec<TableName>) -> Self {
		self.target_tables = tables;
		self
	}

	/// Attach a pushed-down `WHERE` predicate, replacing the downstream
	/// `Filter` operator. `key_resident` must be `true` only when the
	/// predicate is safe to evaluate against a synthesized key value before
	/// fetching the edge record (see
	/// `Planner::graph_predicate_is_key_resident`).
	pub(crate) fn with_predicate(
		mut self,
		predicate: Arc<dyn PhysicalExpr>,
		key_resident: bool,
	) -> Self {
		self.predicate = Some(predicate);
		self.predicate_key_resident = key_resident;
		self
	}

	/// Attach a payload-resident `WHERE` predicate together with the
	/// plan-time inline-field snapshot it was admitted under. See
	/// [`GraphEdgeScan::payload_spec`].
	pub(crate) fn with_payload_predicate(
		mut self,
		predicate: Arc<dyn PhysicalExpr>,
		spec: PayloadPredicateSpec,
	) -> Self {
		self.predicate = Some(predicate);
		self.predicate_key_resident = false;
		self.payload_spec = Some(Arc::new(spec));
		self
	}
}
impl ExecOperator for GraphEdgeScan {
	fn name(&self) -> &'static str {
		"GraphEdgeScan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let dir = match self.direction {
			LookupDirection::Out => "->",
			LookupDirection::In => "<-",
			LookupDirection::Both => "<->",
			LookupDirection::Reference => "<~",
		};
		let tables = if self.edge_tables.is_empty() {
			"*".to_string()
		} else {
			self.edge_tables.iter().map(|t| t.table.as_str()).collect::<Vec<_>>().join(", ")
		};
		let mut attrs = vec![
			("direction".to_string(), dir.to_string()),
			("tables".to_string(), tables),
			("output".to_string(), format!("{:?}", self.output_mode)),
		];
		if let Some(ref version) = self.version {
			attrs.push(("version".to_string(), version.to_sql()));
		}
		if let Some(limit) = self.limit {
			attrs.push(("limit".to_string(), limit.to_string()));
		}
		if let Some(ref predicate) = self.predicate {
			attrs.push(("predicate".to_string(), predicate.to_sql()));
			attrs.push((
				"predicate_scope".to_string(),
				if self.predicate_key_resident {
					"key".to_string()
				} else if self.payload_spec.is_some() {
					"payload".to_string()
				} else {
					"record".to_string()
				},
			));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		// GraphEdgeScan needs database context, combined with expression contexts
		let mut level = self.input.required_context().max(ContextLevel::Database);
		if let Some(ref predicate) = self.predicate {
			level = level.max(predicate.required_context());
		}
		level
	}

	fn access_mode(&self) -> AccessMode {
		let mut mode = self.input.access_mode();
		if let Some(ref version) = self.version {
			mode = mode.combine(version.access_mode());
		}
		// A record-referencing predicate may contain a mutating subquery, so
		// its access mode must propagate up the tree.
		if let Some(ref predicate) = self.predicate {
			mode = mode.combine(predicate.access_mode());
		}
		mode
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let db_ctx = ctx.database()?.clone();
		// SECURITY (GHSA-2v9j): confine the traversal to the caller's tenant. A
		// graph scan can be rooted at a literal record (`CurrentValueSource`), so
		// unlike the other scan operators it is not guaranteed to sit downstream
		// of a validated entry scan; enforce the namespace/database boundary here.
		validate_record_user_access(&db_ctx)?;
		// SECURITY: graph edge results bypass `Document::pluck_select`, so we
		// must enforce the edge/target table's SELECT permission here. Without
		// this check a low-privileged user could traverse `->edge` to
		// enumerate otherwise-hidden relationships, and the `FullEdge` output
		// mode would additionally return raw record data for tables they
		// cannot SELECT. `resolve_record_batch` enforces the table-level
		// permission and — in `FullEdge` mode — additionally applies
		// field-level SELECT permissions and computed fields, matching a
		// direct `SELECT *` on the edge table.
		let check_perms = should_check_perms(&db_ctx, Action::View)?;
		let mut input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
		let direction = self.direction;
		let edge_tables = self.edge_tables.clone();
		let output_mode = self.output_mode;
		let target_tables = self.target_tables.clone();
		let edge_limit = self.limit;
		let version_expr = self.version.clone();
		let scan_batch_size = ctx.root().ctx.config.exec.scan_batch_size;
		let fold_threshold = ctx.root().ctx.config.idx.graph_fold_threshold;
		let ctx = ctx.clone();
		let fetch_full = output_mode == GraphScanOutput::FullEdge;
		// A pushed-down `WHERE` predicate, replacing the downstream `Filter`.
		// Split into the mutually-exclusive key-resident vs record-referencing
		// forms so the flush path applies each at the right point.
		let has_predicate = self.predicate.is_some();
		let key_predicate: Option<Arc<dyn PhysicalExpr>> =
			self.predicate.clone().filter(|_| self.predicate_key_resident);
		let record_predicate: Option<Arc<dyn PhysicalExpr>> =
			self.predicate.clone().filter(|_| !self.predicate_key_resident);
		let payload_spec = self.payload_spec.clone();
		let metrics = Arc::clone(&self.metrics);
		let record_metrics = metrics.is_enabled();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			let txn = ctx.txn();
			let ns_id = db_ctx.ns_ctx.ns.namespace_id;
			let db_id = db_ctx.db.database_id;
			// Compiled SELECT permissions, cached by table. Different edge
			// tables may have different permission policies, so we resolve
			// lazily per table on first use.
			let mut perm_cache: std::collections::HashMap<
				surrealdb_strand::TableName,
				PhysicalPermission,
			> = std::collections::HashMap::new();

			let version: Option<u64> = resolve_version_stamp(&ctx, version_expr.as_ref()).await?;

			// Determine the directions to scan
			// Note: For Both, we scan In first then Out to match legacy executor behavior
			let directions: Vec<Dir> = match direction {
				LookupDirection::Out => vec![Dir::Out],
				LookupDirection::In => vec![Dir::In],
				LookupDirection::Both => vec![Dir::In, Dir::Out],
				LookupDirection::Reference => Err(ControlFlow::Err(anyhow::anyhow!(
					"Reference lookups should use ReferenceScan, not GraphEdgeScan"
				)))?,
			};

			// Inline sidecar caches serve a `(source, dir)`'s complete current
			// adjacency in one value, so they are usable only when this scan
			// reads current data over unbounded edge-table specs — a windowed
			// (`->edge:a..b`) or versioned read needs the cursor.
			// A payload-resident predicate needs the adjacency values, which
			// the caches do not carry — payload mode always reads the cursor.
			let cache_eligible = payload_spec.is_none()
				&& version.is_none()
				&& edge_tables.iter().all(|s| {
					matches!(s.range_start, std::ops::Bound::Unbounded)
						&& matches!(s.range_end, std::ops::Bound::Unbounded)
				});

			// The specs' encoded table names, for the cache fast path's
			// spec-order grouping: a table name is a prefix-free leading
			// segment of an entry's identity bytes, so a byte-prefix test
			// selects exactly the entries whose decoded edge table equals
			// the spec's — without decoding the rejects.
			let spec_prefixes: Vec<Vec<u8>> = if cache_eligible {
				edge_tables
					.iter()
					.map(|spec| {
						storekey::encode_vec(&spec.table).map_err(anyhow::Error::from_boxed)
					})
					.collect::<Result<_, anyhow::Error>>()?
			} else {
				Vec::new()
			};

			// Read from the child operator stream and extract RecordIds
			let mut rid_batch: Vec<RecordId> = Vec::with_capacity(scan_batch_size);
			// Payload-mode edges buffered for the flush-time batched
			// evaluation; always drained by the per-cursor-batch flush.
			let mut payload_pending: Vec<PayloadPending> = Vec::new();

			while let Some(batch_result) = input_stream.next().await {
				let batch = batch_result?;
				let source_rids: Vec<RecordId> = batch
					.into_iter()
					.flat_map(|v| {
						let mut rids = Vec::new();
						extract_record_ids_into(v, &mut rids);
						rids
					})
					.collect();

				// One catalog lookup per distinct source table in this input
				// batch — the cache-eligibility check and the folded-adjacency
				// flag below both read the same definition, and sources
				// overwhelmingly share one table. `get_tb` is snapshot-stable
				// within the transaction, so batching the lookups observes the
				// same definitions a per-source lookup would.
				let mut source_tables: std::collections::HashMap<
					TableName,
					Option<Arc<TableDefinition>>,
				> = std::collections::HashMap::new();
				for rid in &source_rids {
					if !source_tables.contains_key(&rid.table) {
						let tb = txn
							.get_tb(ns_id, db_id, &rid.table, None)
							.await
							.context("Failed to resolve the source vertex's table")?;
						source_tables.insert(rid.table.clone(), tb);
					}
				}
				let source_table =
					|rid: &RecordId| source_tables.get(&rid.table).and_then(|tb| tb.as_deref());

				// One multi-get fetches every eligible source's cache keys for
				// this input batch — the fan-out the caches exist to amortise.
				// A live hit is that `(source, dir)`'s complete adjacency,
				// held in its raw entry form until the per-source loop below
				// consumes it, so at most one source's adjacency is ever
				// decoded at a time; anything else falls back to the merged
				// cursor.
				let mut cache_map: std::collections::HashMap<(usize, Dir), Vec<EdgeCacheEntry>> =
					std::collections::HashMap::new();
				if cache_eligible {
					let mut slots: Vec<(usize, Dir)> = Vec::new();
					let mut keys: Vec<EdgeCacheKey> = Vec::new();
					for (src_idx, rid) in source_rids.iter().enumerate() {
						if crate::idx::inline_cache::effective_edges_cap(source_table(rid))
							.is_none()
						{
							continue;
						}
						for &dir in &directions {
							slots.push((src_idx, dir));
							keys.push(EdgeCacheKey {
								ns: ns_id,
								db: db_id,
								tb: Cow::Borrowed(&rid.table),
								id: Cow::Borrowed(&rid.key),
								dir,
							});
						}
					}
					if !keys.is_empty() {
						let values = txn
							.get_many_key(keys, None)
							.await
							.context("Failed to read the inline adjacency caches")?;
						let mut hits: u64 = 0;
						let mut misses: u64 = 0;
						for (slot, value) in slots.into_iter().zip(values) {
							match value {
								Some(CacheValue::Live(entries)) => {
									hits += 1;
									cache_map.insert(slot, entries);
								}
								_ => misses += 1,
							}
						}
						if record_metrics {
							metrics.add_cache_hits(hits);
							metrics.add_cache_misses(misses);
						}
					}
				}

				// Scan edges for each source record
				for (src_idx, rid) in source_rids.iter().enumerate() {
					let mut edges_yielded: usize = 0;
					// How the source's adjacency is read: merged with packed
					// blocks once its table folds, or synthesized from the id
					// when the source is itself a lightweight edge. Read off
					// the batch's memoized definitions, so classifying a
					// source costs no extra catalog lookup.
					let adjacency = table_adjacency(source_table(rid));
					// A lightweight edge's synthesized adjacency derives from
					// its id alone, which says nothing about existence — and
					// sources can be literal ids (`follows:[a,b]->person`),
					// not just live upstream rows. Gate on the edge existing
					// at this snapshot, exactly as the fallback executor does.
					if adjacency == VertexAdjacency::Lightweight
						&& !txn
							.record_exists(ns_id, db_id, &rid.table, &rid.key, version)
							.await
							.context("Failed to check a lightweight source's existence")?
					{
						continue;
					}
					'dir_loop: for &dir in &directions {
						// Cache fast path: emit the cached adjacency through
						// the same limit/predicate flush machinery the cursor
						// path uses, grouped in the requested spec order — no
						// cursor is opened for this direction. Entries are
						// taken out of the batch map and decoded one at a
						// time as they are consumed, so a LIMIT stops both
						// the decoding and the `edges_scanned` count at the
						// entries it actually visited, and the adjacency
						// drops as soon as this direction is done.
						if let Some(entries) = cache_map.remove(&(src_idx, dir)) {
							let selected: Box<dyn Iterator<Item = &EdgeCacheEntry> + Send + '_> =
								if spec_prefixes.is_empty() {
									Box::new(entries.iter())
								} else {
									Box::new(spec_prefixes.iter().flat_map(|prefix| {
										entries
											.iter()
											.filter(move |e| e.edge.starts_with(prefix.as_slice()))
									}))
								};
							let mut scanned: u64 = 0;
							let mut limit_hit = false;
							for entry in selected {
								scanned += 1;
								let (edge, target) = decode_edge_cache_entry(entry)?;
								match output_mode {
									GraphScanOutput::TargetVertex => {
										if target_tables.is_empty()
											|| target_tables.contains(&target.table)
										{
											rid_batch.push(target);
											edges_yielded += 1;
										}
									}
									_ => {
										rid_batch.push(edge);
										if !has_predicate {
											edges_yielded += 1;
										}
									}
								}
								if !has_predicate {
									if edge_limit.is_some_and(|l| edges_yielded >= l) {
										limit_hit = true;
										break;
									}
									if rid_batch.len() >= scan_batch_size {
										let values = resolve_and_filter_batch(
											&ctx,
											&txn,
											ns_id,
											db_id,
											&rid_batch,
											fetch_full,
											check_perms,
											version,
											&mut perm_cache,
											key_predicate.as_ref(),
											record_predicate.as_ref(),
										)
										.await?;
										rid_batch.clear();
										yielder.emit(ValueBatch::new(values)).await;
									}
								}
							}
							if record_metrics && scanned > 0 {
								metrics.add_edges_scanned(scanned);
							}
							// With a predicate, flush per (source, direction) so
							// filtering and per-source LIMIT counting stay
							// within one source, exactly as the cursor path
							// flushes per cursor batch.
							if has_predicate && !rid_batch.is_empty() {
								let mut values = resolve_and_filter_batch(
									&ctx,
									&txn,
									ns_id,
									db_id,
									&rid_batch,
									fetch_full,
									check_perms,
									version,
									&mut perm_cache,
									key_predicate.as_ref(),
									record_predicate.as_ref(),
								)
								.await?;
								rid_batch.clear();
								if let Some(l) = edge_limit {
									let room = l.saturating_sub(edges_yielded);
									if values.len() >= room {
										values.truncate(room);
										limit_hit = true;
									}
								}
								edges_yielded += values.len();
								if !values.is_empty() {
									yielder.emit(ValueBatch::new(values)).await;
								}
							}
							if limit_hit {
								break 'dir_loop;
							}
							continue;
						}
						let scopes =
							compute_graph_ranges(ns_id, db_id, rid, dir, &edge_tables, &ctx)
								.await?;
						// Unfolded keys seen across this direction's scopes,
						// for the fold observation below.
						let mut dir_delta_hits: u64 = 0;

						for scope in scopes {
							// Merged cursor over the source vertex's adjacency.
							// In `TargetVertex` mode, legacy-format entries (no
							// embedded target) are buffered into a bounded
							// chunk and resolved after the cursor closes; when
							// the buffer fills before the scope is exhausted,
							// we drain it via inner scans and resume the scan
							// past the last processed position.
							let mut chunk = scope.range.clone();
							let mut limit_hit = false;
							'range_chunks: loop {
								let mut legacy_edges: Vec<RecordId> = Vec::new();
								let mut chunk_bound_hit = false;
								let mut last_processed_key: Option<Vec<u8>> = None;
								{
									let adjacency_scope = AdjacencyScope {
										ns: ns_id,
										db: db_id,
										vertex: rid,
										dir: Some(dir),
										edge_table: scope.edge_table.as_ref(),
										fk_lower: scope.fk_lower.as_ref(),
										delta_range: chunk.clone(),
									};
									let mut cursor = match adjacency {
										VertexAdjacency::Lightweight => {
											MergedAdjacencyCursor::open_lightweight(
												&txn,
												&adjacency_scope,
											)
											.context("Failed to open graph cursor")?
										}
										// Payload mode needs the adjacency
										// values even on a never-folded table,
										// whose keys-only scan would otherwise
										// skip them.
										VertexAdjacency::Normal {
											folded,
										} if payload_spec.is_some() => MergedAdjacencyCursor::open_with_values(
											&txn,
											adjacency_scope,
											folded,
											version,
											Some(
												ctx.root()
													.ctx
													.get_index_stores()
													.adjacency_resolve(),
											),
										)
										.await
										.context("Failed to open graph cursor")?,
										VertexAdjacency::Normal {
											folded,
										} => MergedAdjacencyCursor::open(
											&txn,
											adjacency_scope,
											folded,
											version,
											Some(
												ctx.root()
													.ctx
													.get_index_stores()
													.adjacency_resolve(),
											),
										)
										.await
										.context("Failed to open graph cursor")?,
									};
									'cursor_loop: loop {
										// Without a predicate, `edges_yielded` counts scanned
										// edges, so cap each batch to the remaining LIMIT
										// budget to avoid over-fetching. With a predicate,
										// `edges_yielded` counts post-filter matches, so we may
										// need to scan many more keys than the budget to find
										// that many matches — request full chunks and rely on
										// the post-filter LIMIT check at flush time.
										let batch_size = if has_predicate {
											crate::kvs::NORMAL_BATCH_SIZE
										} else {
											let remaining = edge_limit.map(|l| {
												l.saturating_sub(edges_yielded)
													.min(crate::kvs::NORMAL_BATCH_SIZE as usize)
											});
											match remaining {
												Some(0) => {
													limit_hit = true;
													break;
												}
												Some(r) => r as u32,
												None => crate::kvs::NORMAL_BATCH_SIZE,
											}
										};
										let batch = cursor
											.next_batch(batch_size)
											.await
											.context("Failed to scan graph edge")?;
										if batch.is_empty() {
											break;
										}
										let mut scanned_in_batch: u64 = 0;
										for item in batch {
											scanned_in_batch += 1;
											if output_mode == GraphScanOutput::TargetVertex {
												match item.target {
													// New-format entry: the embedded
													// target vertex lets us skip
													// the edge-record hop entirely.
													Some(target)
														if target_tables.is_empty()
															|| target_tables
																.contains(&target.table) =>
													{
														rid_batch.push(target);
														edges_yielded += 1;
													}
													Some(_) => {
														// Target table doesn't
														// match the next-hop
														// filter; skip.
													}
													None => {
														// Legacy entry: defer the
														// fallback scan until the
														// outer cursor closes
														// (see below). Bound the
														// buffer to `scan_batch_size`
														// so a vertex with many
														// un-migrated edges
														// doesn't OOM the scan.
														legacy_edges.push(item.edge);
														if legacy_edges.len() >= scan_batch_size {
															chunk_bound_hit = true;
															last_processed_key = Some(item.key);
															break;
														}
													}
												}
											} else if let Some(spec) = payload_spec.as_deref() {
												// Payload-resident predicate: synthesize the
												// candidate object now, evaluate at flush time
												// in one batch — a falsy edge is dropped there
												// without touching its record (the selectivity
												// win), a truthy one proceeds to the batched
												// existence and permission resolve. An edge
												// whose payload is unusable falls back to the
												// one trustworthy source, the edge record,
												// prefetched per flush with a single multi-get.
												// Scan order is preserved throughout.
												let entry = synthesize_payload_candidate(
													spec, rid, dir, item,
												);
												if record_metrics {
													if entry.1.is_some() {
														metrics.add_props_evals(1);
													} else {
														metrics.add_props_fallbacks(1);
													}
												}
												payload_pending.push(entry);
											} else {
												// `TargetId` / `FullEdge` operate on the
												// edge identity. Defer counting until after
												// filtering at flush time when a predicate is
												// pushed down; otherwise count each edge as it
												// is buffered.
												rid_batch.push(item.edge);
												if !has_predicate {
													edges_yielded += 1;
												}
											}
											if !has_predicate
												&& edge_limit.is_some_and(|l| edges_yielded >= l)
											{
												limit_hit = true;
												break;
											}
										}
										// Record scanned adjacency keys for EXPLAIN
										// selectivity (matched = output_rows).
										if record_metrics && scanned_in_batch > 0 {
											metrics.add_edges_scanned(scanned_in_batch);
										}
										// With a predicate we flush after every
										// cursor batch so filtering and per-source LIMIT
										// counting stay within one source (cursors are per
										// source+direction); without one we flush at
										// `scan_batch_size` to amortise the batch fetch.
										let should_flush = if has_predicate {
											!rid_batch.is_empty() || !payload_pending.is_empty()
										} else {
											rid_batch.len() >= scan_batch_size
										};
										if should_flush {
											// In payload mode the buffered edges are first
											// reduced to the predicate's matches — evaluated
											// in batch against their payloads or their
											// prefetched records — so the flush resolves
											// without re-applying it: that resolve is the
											// batched existence-and-permissions check.
											if !payload_pending.is_empty() {
												let pred = record_predicate
													.as_ref()
													.expect("payload mode implies a predicate");
												filter_payload_pending(
													&ctx,
													&txn,
													ns_id,
													db_id,
													version,
													pred,
													std::mem::take(&mut payload_pending),
													&mut rid_batch,
												)
												.await?;
											}
											let flush_record_predicate = if payload_spec.is_some() {
												None
											} else {
												record_predicate.as_ref()
											};
											let mut values = if rid_batch.is_empty() {
												Vec::new()
											} else {
												resolve_and_filter_batch(
													&ctx,
													&txn,
													ns_id,
													db_id,
													&rid_batch,
													fetch_full,
													check_perms,
													version,
													&mut perm_cache,
													key_predicate.as_ref(),
													flush_record_predicate,
												)
												.await?
											};
											rid_batch.clear();
											if has_predicate {
												// Count post-filter matches and enforce the
												// per-source LIMIT against them.
												if let Some(l) = edge_limit {
													let room = l.saturating_sub(edges_yielded);
													if values.len() >= room {
														values.truncate(room);
														limit_hit = true;
													}
												}
												edges_yielded += values.len();
												if !values.is_empty() {
													yielder.emit(ValueBatch::new(values)).await;
												}
											} else {
												yielder.emit(ValueBatch::new(values)).await;
											}
										}
										if limit_hit || chunk_bound_hit {
											break 'cursor_loop;
										}
									}
									let merge_stats = cursor.stats();
									dir_delta_hits += merge_stats.delta_hits;
									if record_metrics {
										metrics.add_block_hits(merge_stats.block_hits);
										metrics.add_delta_hits(merge_stats.delta_hits);
									}
									drop(cursor);
								}

								// Legacy-format fallback: for each adjacency entry
								// that did not embed a target, walk the edge's
								// own adjacency (restricted to the requested
								// target tables) to recover the target vertex.
								// Runs after the outer cursor is dropped so we
								// can hold an inner cursor on `txn`.
								if !legacy_edges.is_empty() && !limit_hit {
									let inner_specs: Vec<EdgeTableSpec> =
										if target_tables.is_empty() {
											Vec::new()
										} else {
											target_tables
												.iter()
												.cloned()
												.map(|t| EdgeTableSpec {
													table: t,
													range_start: std::ops::Bound::Unbounded,
													range_end: std::ops::Bound::Unbounded,
												})
												.collect()
										};
									'legacy_loop: for edge_rid in legacy_edges {
										// Edge-side inner keys are never folded,
										// so the fallback stays a plain key scan.
										let inner_scopes = compute_graph_ranges(
											ns_id,
											db_id,
											&edge_rid,
											dir,
											&inner_specs,
											&ctx,
										)
										.await?;
										for inner in inner_scopes {
											let mut inner_cursor = txn
												.open_keys_cursor_raw(
													inner.range,
													Direction::Forward,
													0,
													version,
												)
												.await
												.context(
													"Failed to open legacy-fallback graph cursor",
												)?;
											loop {
												let inner_batch = inner_cursor
													.next_batch(crate::kvs::NORMAL_BATCH_SIZE)
													.await
													.context(
														"Failed to scan edge adjacency for legacy graph fallback",
													)?;
												if inner_batch.is_empty() {
													break;
												}
												for ik in &inner_batch {
													// On edge-side adjacency keys the
													// `(ft, fk)` slot holds the endpoint
													// vertex, not an edge -- the legacy
													// fallback walks the edge's own
													// adjacency precisely to recover
													// that vertex. Bind it under a name
													// that reflects what it actually is.
													let endpoint = inner
														.decoder
														.decode_edge(ik)
														.context("Failed to decode graph key")?;
													rid_batch.push(endpoint);
													edges_yielded += 1;
													if edge_limit
														.is_some_and(|l| edges_yielded >= l)
													{
														limit_hit = true;
														break;
													}
												}
												if rid_batch.len() >= scan_batch_size {
													let values = resolve_record_batch(
														&ctx,
														&txn,
														ns_id,
														db_id,
														&rid_batch,
														fetch_full,
														check_perms,
														version,
														CachePolicy::ReadWrite,
														&mut perm_cache,
													)
													.await?;
													yielder.emit(ValueBatch::new(values)).await;
													rid_batch.clear();
												}
												if limit_hit {
													break;
												}
											}
											drop(inner_cursor);
											if limit_hit {
												break 'legacy_loop;
											}
										}
									}
								}

								// Continue chunking only when the cursor was
								// suspended because the legacy buffer filled.
								// `last_processed_key` is the legacy key that
								// triggered the bound; the next chunk picks the
								// range up immediately after it.
								if !chunk_bound_hit || limit_hit {
									break 'range_chunks;
								}
								let resume_from = last_processed_key
									.expect("chunk_bound_hit implies a key was processed");
								chunk = chunk.resume_after(&resume_from, Direction::Forward);
							}

							if limit_hit {
								break 'dir_loop;
							}
						}
						observe_fold_candidate(
							&txn,
							ns_id,
							db_id,
							rid,
							dir,
							dir_delta_hits,
							fold_threshold,
						);
					}
				}
			}

			// Yield any remaining rows. With a predicate this is
			// normally empty (each cursor batch is flushed above);
			// the helper degrades to a plain resolve when there is
			// no predicate.
			if !rid_batch.is_empty() {
				let values = resolve_and_filter_batch(
					&ctx,
					&txn,
					ns_id,
					db_id,
					&rid_batch,
					fetch_full,
					check_perms,
					version,
					&mut perm_cache,
					key_predicate.as_ref(),
					record_predicate.as_ref(),
				)
				.await?;
				if !values.is_empty() {
					yielder.emit(ValueBatch::new(values)).await;
				}
			}
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "GraphEdgeScan", &self.metrics))
	}
}

/// One payload-mode edge awaiting its flush-time predicate evaluation:
/// the edge's record id paired with the synthesized `{id, in, out,
/// …inline}` candidate object — `None` when the payload was unusable and
/// the edge record is the only source of truth.
type PayloadPending = (RecordId, Option<Value>);

/// Synthesizes the candidate object a payload-resident predicate is
/// evaluated against, for one adjacency entry. Trust is anchored on the
/// plan-time snapshot: the payload's stamped generation and value count
/// must match it exactly. A `None` candidate marks a fallback — the
/// payload was absent, spilled, written under a different generation or
/// field count, undecodable, or a legacy entry with no embedded target —
/// and the edge record is the only source of truth.
fn synthesize_payload_candidate(
	spec: &PayloadPredicateSpec,
	source: &RecordId,
	dir: Dir,
	item: crate::idx::adjacency::MergedAdjacencyEdge,
) -> PayloadPending {
	use surrealdb_datastore::values::graph::InlineProps;
	let edge = item.edge;
	let Some(table_spec) = spec.tables.get(&edge.table) else {
		return (edge, None);
	};
	let Some(target) = item.target else {
		return (edge, None);
	};
	let Some(body) = item.props.as_deref() else {
		return (edge, None);
	};
	let Ok(props) = InlineProps::decode(body) else {
		return (edge, None);
	};
	if props.generation != table_spec.generation {
		return (edge, None);
	}
	let Some(values) = props.values else {
		return (edge, None);
	};
	if values.len() != table_spec.fields.len() {
		return (edge, None);
	}
	let (in_rid, out_rid) = match dir {
		Dir::Out => (source.clone(), target),
		Dir::In => (target, source.clone()),
		Dir::Both => return (edge, None),
	};
	let mut object = crate::val::Object::default();
	object.insert("id".to_owned(), Value::RecordId(edge.clone()));
	object.insert("in".to_owned(), Value::RecordId(in_rid));
	object.insert("out".to_owned(), Value::RecordId(out_rid));
	for (name, value) in table_spec.fields.iter().zip(values) {
		object.insert(name.clone(), value);
	}
	(edge, Some(Value::Object(object)))
}

/// Reduces one flush's worth of payload-mode edges to the predicate's
/// matches, appended to `out` in scan order.
///
/// Edges carrying a synthesized candidate are evaluated in one batch.
/// Fallback edges are prefetched with a single multi-get and evaluated
/// in a second batch over the fetched records — a missing record (a
/// vertex-side orphan) simply doesn't match. The prefetch populates the
/// transaction's read cache, so the subsequent existence-and-permissions
/// resolve does not re-read the surviving records from the store.
#[allow(clippy::too_many_arguments)]
async fn filter_payload_pending(
	ctx: &ExecutionContext,
	txn: &Transaction,
	ns_id: NamespaceId,
	db_id: DatabaseId,
	version: Option<u64>,
	predicate: &Arc<dyn PhysicalExpr>,
	pending: Vec<PayloadPending>,
	out: &mut Vec<RecordId>,
) -> Result<(), ControlFlow> {
	// Split the flush into its two evaluation groups, remembering each
	// edge's group so survivors merge back in scan order.
	let mut candidates: Vec<Value> = Vec::new();
	let mut fallback_rids: Vec<RecordId> = Vec::new();
	let mut order: Vec<(RecordId, bool)> = Vec::with_capacity(pending.len());
	for (rid, candidate) in pending {
		match candidate {
			Some(value) => {
				candidates.push(value);
				order.push((rid, true));
			}
			None => {
				fallback_rids.push(rid.clone());
				order.push((rid, false));
			}
		}
	}
	let candidate_results = if candidates.is_empty() {
		Vec::new()
	} else {
		predicate.evaluate_batch(EvalContext::from_exec_ctx(ctx), &candidates).await?
	};
	let fallback_matches = if fallback_rids.is_empty() {
		Vec::new()
	} else {
		let records = txn
			.get_records(ns_id, db_id, &fallback_rids, version, CachePolicy::ReadWrite)
			.await
			.context("Failed to fetch the payload-fallback edge records")?;
		let mut matches = vec![false; records.len()];
		let mut present_slots: Vec<usize> = Vec::new();
		let mut present_values: Vec<Value> = Vec::new();
		for (slot, record) in records.iter().enumerate() {
			if record.metadata.is_some() || !record.data.is_none() {
				present_slots.push(slot);
				present_values.push(record.data.clone());
			}
		}
		if !present_values.is_empty() {
			let results =
				predicate.evaluate_batch(EvalContext::from_exec_ctx(ctx), &present_values).await?;
			for (slot, result) in present_slots.into_iter().zip(results) {
				matches[slot] = result.is_truthy();
			}
		}
		matches
	};
	let mut candidate_results = candidate_results.into_iter();
	let mut fallback_matches = fallback_matches.into_iter();
	for (rid, evaluated) in order {
		let keep = if evaluated {
			candidate_results.next().is_some_and(|v| v.is_truthy())
		} else {
			fallback_matches.next().unwrap_or(false)
		};
		if keep {
			out.push(rid);
		}
	}
	Ok(())
}

/// Decodes one cached adjacency entry's identity bytes into its owned
/// `(edge, target)` record ids.
fn decode_edge_cache_entry(entry: &EdgeCacheEntry) -> Result<(RecordId, RecordId), anyhow::Error> {
	let (table, key): (TableName, RecordIdKey) = storekey::decode_borrow(&entry.edge)
		.map_err(|e| anyhow::anyhow!("Failed to decode a cached edge identity: {e}"))?;
	let (t_table, t_key): (TableName, RecordIdKey) = storekey::decode_borrow(&entry.target)
		.map_err(|e| anyhow::anyhow!("Failed to decode a cached edge target: {e}"))?;
	Ok((RecordId::new(table, key), RecordId::new(t_table, t_key)))
}

/// Fetch and filter a batch of candidate edges/vertices for one flush.
///
/// Applies, in order: (1) a key-resident predicate against a synthesized
/// `RecordId`, dropping non-matches *before* any fetch; (2) the batch record
/// fetch + table SELECT permission (`resolve_record_batch`); (3) a
/// record-referencing predicate against the decoded record, *after*
/// permission filtering. `key_predicate` and `record_predicate` are mutually
/// exclusive in practice; both `None` degrades to a plain
/// `resolve_record_batch`.
#[allow(clippy::too_many_arguments)]
async fn resolve_and_filter_batch(
	ctx: &ExecutionContext,
	txn: &Transaction,
	ns_id: NamespaceId,
	db_id: DatabaseId,
	rids: &[RecordId],
	fetch_full: bool,
	check_perms: bool,
	version: Option<u64>,
	perm_cache: &mut std::collections::HashMap<TableName, PhysicalPermission>,
	key_predicate: Option<&Arc<dyn PhysicalExpr>>,
	record_predicate: Option<&Arc<dyn PhysicalExpr>>,
) -> Result<Vec<Value>, ControlFlow> {
	let values = if let Some(pred) = key_predicate {
		// Key-resident pre-fetch skip: evaluate against the
		// adjacency-key identity so non-matches are never fetched
		// or permission-checked.
		let mut survivors: Vec<RecordId> = Vec::with_capacity(rids.len());
		for rid in rids {
			let candidate = Value::RecordId(rid.clone());
			let keep = pred
				.evaluate(EvalContext::from_exec_ctx(ctx).with_value(&candidate))
				.await?
				.is_truthy();
			if keep {
				survivors.push(rid.clone());
			}
		}
		if survivors.is_empty() {
			return Ok(Vec::new());
		}
		resolve_record_batch(
			ctx,
			txn,
			ns_id,
			db_id,
			&survivors,
			fetch_full,
			check_perms,
			version,
			CachePolicy::ReadWrite,
			perm_cache,
		)
		.await?
	} else {
		resolve_record_batch(
			ctx,
			txn,
			ns_id,
			db_id,
			rids,
			fetch_full,
			check_perms,
			version,
			CachePolicy::ReadWrite,
			perm_cache,
		)
		.await?
	};
	match record_predicate {
		// Record-referencing predicate runs after fetch + permission,
		// on the decoded record, preserving fetch -> permission ->
		// filter order.
		Some(pred) => apply_record_predicate(pred, ctx, values).await,
		None => Ok(values),
	}
}

/// Retain only rows for which `predicate` is truthy, evaluated against the
/// decoded record. Mirrors `Filter::filter_batch_in_place`.
async fn apply_record_predicate(
	predicate: &Arc<dyn PhysicalExpr>,
	ctx: &ExecutionContext,
	mut values: Vec<Value>,
) -> Result<Vec<Value>, ControlFlow> {
	let results = {
		let eval_ctx = EvalContext::from_exec_ctx(ctx);
		predicate.evaluate_batch(eval_ctx, &values).await?
	};
	let mut write = 0;
	for (read, result) in results.into_iter().enumerate() {
		if result.is_truthy() {
			if write != read {
				values.swap(write, read);
			}
			write += 1;
		}
	}
	values.truncate(write);
	Ok(values)
}

#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use super::*;
	use crate::exec::operators::CurrentValueSource;

	#[test]
	fn test_graph_edge_scan_attrs() {
		let scan = GraphEdgeScan::new(
			Arc::new(CurrentValueSource::new()),
			LookupDirection::Out,
			vec![
				EdgeTableSpec {
					table: "knows".into(),
					range_start: Bound::Unbounded,
					range_end: Bound::Unbounded,
				},
				EdgeTableSpec {
					table: "follows".into(),
					range_start: Bound::Unbounded,
					range_end: Bound::Unbounded,
				},
			],
			GraphScanOutput::TargetId,
			None,
		);

		assert_eq!(scan.name(), "GraphEdgeScan");
		let attrs = scan.attrs();
		assert!(attrs.iter().any(|(k, v)| k == "direction" && v == "->"));
		assert!(attrs.iter().any(|(k, v)| k == "tables" && v.contains("knows")));
	}
}
