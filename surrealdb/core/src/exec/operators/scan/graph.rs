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
use crate::catalog::{DatabaseId, NamespaceId};
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
	AdjacencyScope, MergedAdjacencyCursor, VertexAdjacency, VisitFlow, vertex_adjacency_of,
};
use crate::key::Resumable;
use crate::key::schema::EdgeCacheKey;
use crate::kvs::{CachePolicy, Direction, Transaction};
use crate::val::{RecordId, RecordIdKey, TableName, Value};

/// The per-source-table facts the scan needs for every source row —
/// adjacency classification and the inline-cache cap. Memoized for the
/// operator's lifetime, not per execution: a recursion re-executes the
/// scan once per frontier vertex, and a catalog lookup per vertex is
/// measurable. Entries come from the transaction-cached catalog, which is
/// stable for the duration of the statement the plan was built for. The
/// memo is read and written once per input batch — never per source row —
/// so concurrent executions contend on it at batch granularity only.
type SourceTableInfo =
	std::sync::Mutex<std::collections::HashMap<TableName, (VertexAdjacency, Option<u32>)>>;

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
	/// Whether the predicate references the synthetic `id` / `in` / `out`
	/// fields. The synthesized candidate carries only what the predicate
	/// reads — every skipped field is a per-edge record-id clone or value
	/// insert saved on the scan's hottest path.
	pub(crate) needs_id: bool,
	pub(crate) needs_in: bool,
	pub(crate) needs_out: bool,
	/// A key-resident predicate: every field it reads is `id`, `in` or
	/// `out`, all of which the adjacency entry itself carries — no inline
	/// payload is needed, so evaluation prunes non-matching edges before
	/// any fetch on any table, with no `INLINE` fields declared and no
	/// per-table generation to validate. Pruning on key material is
	/// permission-safe: an edge the predicate drops would have been
	/// dropped by the same predicate after the fetch, so the visible
	/// result set is unchanged, and every surviving edge still resolves
	/// through the permission-checked record path.
	pub(crate) key_only: bool,
	/// A comparison-shaped key-resident predicate additionally compiles
	/// to a synchronous matcher (see [`super::key_matcher`]); when set,
	/// the scan tests each entry directly instead of synthesizing a
	/// candidate and running the evaluator. The matcher covers the whole
	/// predicate or is absent — never a partial residue.
	pub(crate) matcher: Option<Arc<super::key_matcher::KeyPredicateMatcher>>,
}

/// One table's inline-field snapshot inside a [`PayloadPredicateSpec`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PayloadTableSpec {
	/// The table's inline generation at plan time.
	pub(crate) generation: u32,
	/// The raw names of every inline field, in the canonical payload order.
	pub(crate) fields: Vec<String>,
	/// Which of `fields` the predicate references, in the same order. The
	/// full list is still what stamps the payload's value count; only the
	/// referenced ones land in the synthesized candidate.
	pub(crate) referenced: Vec<bool>,
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

	/// When set, `limit` was pushed beneath an `ORDER BY id ASC` and the cap
	/// is sound only where the scan emits in ascending edge-id order. The
	/// planner only sets this for a single edge table in one fixed
	/// direction — one adjacency scope per source, whose pointer keys (and
	/// folded blocks, numeric or plain) sort by edge id, and whose
	/// lightweight synthesis holds at most one entry. The one consumer not
	/// driven by a cursor is the inline cache fast path, which orders its
	/// stored entries before the cap applies.
	pub(crate) limit_needs_id_order: bool,

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

	/// Operator-lifetime memo of per-source-table facts; see
	/// [`SourceTableInfo`].
	pub(crate) table_info: Arc<SourceTableInfo>,
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
			limit_needs_id_order: false,
			predicate: None,
			predicate_key_resident: false,
			payload_spec: None,
			metrics: Arc::new(OperatorMetrics::new()),
			table_info: Arc::new(SourceTableInfo::default()),
		}
	}

	pub(crate) fn with_limit(mut self, limit: usize) -> Self {
		self.limit = Some(limit);
		self
	}

	/// Set a per-source cap pushed beneath an `ORDER BY id ASC`: the first
	/// `limit` rows in the scan's ascending edge-id order are exactly the
	/// rows the downstream sort keeps. See [`Self::limit_needs_id_order`]
	/// for the emission orders the scan must compensate for at runtime.
	pub(crate) fn with_ordered_limit(mut self, limit: usize) -> Self {
		self.limit = Some(limit);
		self.limit_needs_id_order = true;
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
			if self.limit_needs_id_order {
				attrs.push(("order_pushdown".to_string(), "id".to_string()));
			}
		}
		if let Some(ref predicate) = self.predicate {
			attrs.push(("predicate".to_string(), predicate.to_sql()));
			attrs.push((
				"predicate_scope".to_string(),
				if self.predicate_key_resident {
					"key".to_string()
				} else if let Some(spec) = self.payload_spec.as_deref() {
					if spec.key_only {
						"key".to_string()
					} else {
						"payload".to_string()
					}
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
		// Arc'd rather than owned: the concurrent fan-out captures these
		// loop-invariant lists per spawned source, and an Arc clone there is
		// a refcount bump instead of a deep Vec clone.
		let edge_tables = Arc::new(self.edge_tables.clone());
		let output_mode = self.output_mode;
		let target_tables = Arc::new(self.target_tables.clone());
		let edge_limit = self.limit;
		let limit_needs_id_order = self.limit_needs_id_order;
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
		let table_info = Arc::clone(&self.table_info);

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			let txn = ctx.txn();
			let ns_id = db_ctx.ns_ctx.ns.namespace_id;
			let db_id = db_ctx.db.database_id;
			// Compiled SELECT permissions, cached by table. Different edge
			// tables may have different permission policies, so we resolve
			// lazily per table on first use.
			let mut perm_cache = PermCache::new();

			let version: Option<u64> = resolve_version_stamp(&ctx, version_expr.as_ref()).await?;

			// Determine the directions to scan
			// Note: For Both, we scan In first then Out to match legacy executor behavior
			// Arc'd for the same reason as the edge/target table lists: the
			// fan-out shares it with each spawned source.
			let directions: Arc<Vec<Dir>> = Arc::new(match direction {
				LookupDirection::Out => vec![Dir::Out],
				LookupDirection::In => vec![Dir::In],
				LookupDirection::Both => vec![Dir::In, Dir::Out],
				LookupDirection::Reference => Err(ControlFlow::Err(anyhow::anyhow!(
					"Reference lookups should use ReferenceScan, not GraphEdgeScan"
				)))?,
			});

			// Inline sidecar caches serve a `(source, dir)`'s complete current
			// adjacency in one value, so they are usable only when this scan
			// reads current data over unbounded edge-table specs — a windowed
			// (`->edge:a..b`) or versioned read needs the cursor.
			// A payload-resident predicate needs the adjacency values, which
			// the caches do not carry, so it reads the cursor — except a
			// key-only spec, which reads nothing beyond the `(edge, target)`
			// pairs the caches store and evaluates against them in the
			// cache-hit loop below.
			let cache_eligible = payload_spec.as_deref().is_none_or(|s| s.key_only)
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
			// The pipelined resolve engages only when the resolve performs a
			// batch read — a permission check or a `FullEdge` fetch. An
			// identity-only resolve (a plain id traversal) completes with no
			// I/O, so those batches emit inline and keep the single-buffer
			// memory bound.
			#[cfg(not(target_family = "wasm"))]
			let pipelined_resolve = check_perms || fetch_full;
			// A flushed batch resolving on its own runtime task while the
			// scan continues — spawned rather than merely polled
			// concurrently: on an embedded backend both the resolve and the
			// next cursor fetch complete synchronously on their first poll,
			// and only separate runtime tasks let them use separate cores.
			// The `JoinSet` aborts the task if the stream drops mid-flight.
			// Values always emit in buffer order — the pending batch drains
			// before anything newer flushes or the cursor closes. The stash
			// exists only on the predicate-free path, so the resolve carries
			// no key or record predicate; a pipelined predicate flush would
			// have to carry its predicates through the stash. While a stash
			// is pending, two id batches plus one resolved value batch are
			// in flight at once, so the pipelined scan's peak memory is ~2x
			// `scan_batch_size` ids plus one value batch. The permission
			// cache travels through the task and returns at the drain; every
			// drain precedes the next resolve, so no resolve runs without it.
			#[cfg(not(target_family = "wasm"))]
			let mut inflight: Option<InflightResolve> = None;
			// The recycled id buffer: each drained resolve hands its cleared
			// buffer back here, and each stash moves it into `rid_batch`, so
			// the steady state reuses two buffers with no per-flush
			// allocation.
			#[cfg(not(target_family = "wasm"))]
			let mut spare: Vec<RecordId> = Vec::new();
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

				// Per-source-table facts for this batch, resolved once per
				// distinct table: known entries are copied out of the
				// operator-lifetime memo under one lock, missing tables are
				// fetched from the catalog outside it, and the new entries
				// are published back under a second lock — the per-source
				// loops below read the local copy with no locking at all.
				// The guarded state is a plain map, so a poisoned lock still
				// holds a consistent value and is read through.
				let mut batch_table_info: std::collections::HashMap<
					TableName,
					(VertexAdjacency, Option<u32>),
				> = std::collections::HashMap::new();
				{
					let memo = table_info.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
					for rid in &source_rids {
						if let Some(info) = memo.get(&rid.table) {
							batch_table_info.insert(rid.table.clone(), *info);
						}
					}
				}
				let mut resolved: Vec<(TableName, (VertexAdjacency, Option<u32>))> = Vec::new();
				for rid in &source_rids {
					if batch_table_info.contains_key(&rid.table) {
						continue;
					}
					let tb = txn
						.get_tb(ns_id, db_id, &rid.table, None)
						.await
						.context("Failed to resolve the source vertex's table")?;
					let info = (
						vertex_adjacency_of(tb.as_deref()),
						crate::idx::inline_cache::effective_edges_cap(tb.as_deref()),
					);
					batch_table_info.insert(rid.table.clone(), info);
					resolved.push((rid.table.clone(), info));
				}
				if !resolved.is_empty() {
					table_info
						.lock()
						.unwrap_or_else(std::sync::PoisonError::into_inner)
						.extend(resolved);
				}

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
						let (_, edges_cap) = batch_table_info[&rid.table];
						if edges_cap.is_none() {
							continue;
						}
						for &dir in directions.iter() {
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

				// Concurrent fan-out for the plain traversal shape — no
				// predicate, no payload: cache-missed sources scan side by
				// side (bounded, in input order) into owned buffers the
				// per-source loop below drains, so a multi-source traversal
				// overlaps its per-source cursor work instead of paying one
				// sequential open-and-drain per source, and its flushes
				// coalesce several sources' results into each record-resolve
				// batch. A pushed LIMIT rides along: the cap is per source,
				// so each fan-out scan enforces its own budget exactly as the
				// sequential path does. Every source the fan-out declines —
				// legacy-format entries, over-cap adjacencies, any cached
				// direction — falls through to the sequential machinery
				// unchanged.
				//
				// The batch is processed in contiguous windows of the
				// fan-out width, each prescanned and then drained/emitted
				// before the next begins: results stream between windows,
				// so a downstream operator that stops early (LIMIT) drops
				// the stream after one window's work rather than the whole
				// batch's, and the aggregate prescan footprint is bounded
				// at one window of capped buffers.
				let mut prescanned: Vec<Option<Vec<RecordId>>> = Vec::new();
				prescanned.resize_with(source_rids.len(), || None);
				#[cfg(not(target_family = "wasm"))]
				let plain_shape = !has_predicate && payload_spec.is_none();
				// Whether spawning pays for this batch: undecided until the
				// first window has run sequentially as a timing probe, then
				// fixed for the batch's remaining windows.
				#[cfg(not(target_family = "wasm"))]
				let mut fanout_engaged: Option<bool> = None;
				let mut win_start = 0usize;
				while win_start < source_rids.len() {
					let win_end = (win_start + GRAPH_FANOUT_CONCURRENCY).min(source_rids.len());
					// Spawned rather than merely polled concurrently: on an
					// embedded backend the per-source scans are CPU-bound,
					// and only separate runtime tasks let them use separate
					// cores. wasm has no worker threads, so it keeps the
					// sequential machinery below unchanged.
					#[cfg(not(target_family = "wasm"))]
					if plain_shape && fanout_engaged == Some(true) {
						let missed: Vec<usize> = (win_start..win_end)
							.filter(|src_idx| {
								!directions
									.iter()
									.any(|dir| cache_map.contains_key(&(*src_idx, *dir)))
							})
							.collect();
						// A window with one cache-missed source has nothing
						// to overlap; the futures are built only for windows
						// that spawn, so a declined window allocates no
						// captures.
						if missed.len() > 1 {
							let mut pending: std::collections::VecDeque<_> = missed
								.into_iter()
								.map(|src_idx| {
									let ctx = ctx.clone();
									let txn = Arc::clone(&txn);
									let rid = source_rids[src_idx].clone();
									let (adjacency, _) = batch_table_info[&rid.table];
									let directions = Arc::clone(&directions);
									let edge_tables = Arc::clone(&edge_tables);
									let target_tables = Arc::clone(&target_tables);
									let metrics = Arc::clone(&metrics);
									async move {
										let out = scan_source_plain(
											&ctx,
											&txn,
											adjacency,
											ns_id,
											db_id,
											&rid,
											&directions,
											&edge_tables,
											output_mode,
											&target_tables,
											version,
											edge_limit,
											fold_threshold,
											scan_batch_size,
											&metrics,
											record_metrics,
										)
										.await;
										(src_idx, out)
									}
								})
								.collect();
							// A `JoinSet` aborts whatever is still in flight
							// when it drops, so an error or a cancelled
							// stream never leaks detached tasks. On failure
							// the lowest-index source's error surfaces: no
							// further tasks are spawned and the in-flight set
							// is drained, so which error a caller sees never
							// depends on the scheduler. A panicked task
							// surfaces only when no source error was
							// collected — its position carries no meaning.
							let mut set = tokio::task::JoinSet::new();
							let mut first_err: Option<(usize, ControlFlow)> = None;
							let mut join_err: Option<ControlFlow> = None;
							loop {
								if first_err.is_none() && join_err.is_none() {
									while set.len() < GRAPH_FANOUT_CONCURRENCY {
										let Some(task) = pending.pop_front() else {
											break;
										};
										set.spawn(task);
									}
								}
								let Some(joined) = set.join_next().await else {
									break;
								};
								match joined {
									Ok((src_idx, Ok(out))) => prescanned[src_idx] = out,
									Ok((src_idx, Err(err))) => {
										if first_err
											.as_ref()
											.is_none_or(|(lowest, _)| src_idx < *lowest)
										{
											first_err = Some((src_idx, err));
										}
									}
									Err(e) => {
										join_err.get_or_insert_with(|| {
											ControlFlow::Err(anyhow::anyhow!(
												"graph fan-out task failed: {e}"
											))
										});
									}
								}
							}
							if let Some((_, err)) = first_err {
								return Err(err);
							}
							if let Some(err) = join_err {
								return Err(err);
							}
						}
					}
					#[cfg(not(target_family = "wasm"))]
					let win_timer = (plain_shape && fanout_engaged.is_none()).then(web_time::Instant::now);

					// Scan edges for each source record in this window
					for (offset, rid) in source_rids[win_start..win_end].iter().enumerate() {
						let src_idx = win_start + offset;
						// A prescanned source's results flow through the same
						// flush machinery, coalesced with its neighbours'.
						if let Some(found) = prescanned[src_idx].take() {
							for target in found {
								rid_batch.push(target);
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
										None,
										None,
									)
									.await?;
									rid_batch.clear();
									yielder.emit(ValueBatch::new(values)).await;
								}
							}
							continue;
						}
						let mut edges_yielded: usize = 0;
						// How the source's adjacency is read: merged with packed
						// blocks once its table folds, or synthesized from the id
						// when the source is itself a lightweight edge. Read off
						// the batch's memoized facts, so classifying a source
						// costs no extra catalog lookup.
						let (adjacency, _) = batch_table_info[&rid.table];
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
						'dir_loop: for &dir in directions.iter() {
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
								let selected: Box<
									dyn Iterator<Item = &EdgeCacheEntry> + Send + '_,
								> = if spec_prefixes.is_empty() {
									Box::new(entries.iter())
								} else {
									Box::new(spec_prefixes.iter().flat_map(|prefix| {
										entries
											.iter()
											.filter(move |e| e.edge.starts_with(prefix.as_slice()))
									}))
								};
								// A cap pushed beneath `ORDER BY id` consumes rows
								// in ascending edge-id order. Ordering the entries
								// by their identity bytes — the same storekey
								// encoding whose byte order is edge-id order on the
								// cursor path's pointer keys — makes that the fast
								// path's emission order at the point the cap applies,
								// rather than inheriting it from how the cache is
								// maintained. The cache holds this (source,
								// direction)'s complete adjacency, so the first
								// `edge_limit` entries are exactly the rows the
								// downstream sort keeps.
								let selected: Box<
									dyn Iterator<Item = &EdgeCacheEntry> + Send + '_,
								> = if limit_needs_id_order && edge_limit.is_some() {
									let mut ordered: Vec<&EdgeCacheEntry> = selected.collect();
									ordered.sort_unstable_by(|a, b| a.edge.cmp(&b.edge));
									Box::new(ordered.into_iter())
								} else {
									selected
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
										_ if payload_spec.is_some() => {
											// The cache gate admits only key-only specs,
											// and a cached `(edge, target)` pair carries
											// everything such a spec reads — the
											// predicate prunes here, before any fetch,
											// exactly as on the cursor path.
											let spec = payload_spec
												.as_deref()
												.expect("guarded by the match arm");
											let pred = record_predicate
												.as_ref()
												.expect("payload mode implies a predicate");
											let item = crate::idx::adjacency::MergedAdjacencyEdge {
												key: Vec::new(),
												edge,
												target: Some(target),
												props: None,
											};
											let (edge, candidate) =
												synthesize_payload_candidate(spec, rid, dir, item);
											let Some(candidate) = candidate else {
												return Err(ControlFlow::Err(anyhow::anyhow!(
													"a key-only predicate cannot fall back on a cached entry"
												)));
											};
											if record_metrics {
												metrics.add_props_evals(1);
											}
											if pred
												.evaluate(
													EvalContext::from_exec_ctx(&ctx)
														.with_value(&candidate),
												)
												.await?
												.is_truthy()
											{
												rid_batch.push(edge);
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
								// flushes per cursor batch. In payload mode every
								// buffered edge already matched the key-resident
								// predicate above, so the flush resolves without
								// re-applying it.
								if has_predicate && !rid_batch.is_empty() {
									let flush_record_predicate = if payload_spec.is_some() {
										None
									} else {
										record_predicate.as_ref()
									};
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
										flush_record_predicate,
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
											// skip them. A key-resident predicate
											// reads only the keys and stays on the
											// plain open below.
											VertexAdjacency::Normal {
												folded,
											} if payload_spec
												.as_deref()
												.is_some_and(|s| !s.key_only) =>
											{
												MergedAdjacencyCursor::open_with_values(
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
												.context("Failed to open graph cursor")?
											}
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
											// Never-folded fast path: consume the batch straight
											// from the cursor's borrowed buffer, no per-edge
											// materialization. Payload mode is excluded — its
											// edges need the synthesized candidate the
											// materialized loop builds — so these arms mirror
											// only the TargetVertex / identity arms of that
											// loop; keep the two in step.
											let visited = if payload_spec.is_none() {
												cursor
													.visit_unfolded_batch(batch_size, |key| {
														let decoded =
														crate::key::schema::DecodedGraph::decode(
															key,
														)?;
														if output_mode
															== GraphScanOutput::TargetVertex
														{
															match decoded.target {
																Some(target)
																	if target_tables.is_empty()
																		|| target_tables
																			.contains(
																				&target.table,
																			) =>
																{
																	rid_batch.push(target);
																	edges_yielded += 1;
																}
																Some(_) => {}
																None => {
																	legacy_edges.push(decoded.edge);
																	if legacy_edges.len()
																		>= scan_batch_size
																	{
																		chunk_bound_hit = true;
																		last_processed_key =
																			Some(key.to_vec());
																		return Ok(VisitFlow::Stop);
																	}
																}
															}
														} else {
															rid_batch.push(decoded.edge);
															if !has_predicate {
																edges_yielded += 1;
															}
														}
														if !has_predicate
															&& edge_limit
																.is_some_and(|l| edges_yielded >= l)
														{
															limit_hit = true;
															return Ok(VisitFlow::Stop);
														}
														Ok(VisitFlow::Continue)
													})
													.await
													.context("Failed to scan graph edge")?
											} else {
												None
											};
											match visited {
												// The cursor is exhausted.
												Some(0) => break,
												Some(n) => {
													if record_metrics {
														metrics.add_edges_scanned(n as u64);
													}
												}
												// Folded, lightweight or payload-carrying:
												// the materialized batch path.
												None => {
													let batch = cursor
														.next_batch_scan(batch_size)
														.await
														.context("Failed to scan graph edge")?;
													if batch.is_empty() {
														break;
													}
													let mut scanned_in_batch: u64 = 0;
													for item in batch {
														scanned_in_batch += 1;
														if output_mode
															== GraphScanOutput::TargetVertex
														{
															match item.target {
																// New-format entry: the embedded
																// target vertex lets us skip
																// the edge-record hop entirely.
																Some(target)
																	if target_tables.is_empty()
																		|| target_tables
																			.contains(
																				&target.table,
																			) =>
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
																	if legacy_edges.len()
																		>= scan_batch_size
																	{
																		chunk_bound_hit = true;
																		last_processed_key =
																			Some(item.key);
																		break;
																	}
																}
															}
														} else if let Some(spec) =
															payload_spec.as_deref()
														{
															// Payload-resident predicate. A
															// compiled
															// matcher answers
															// synchronously from the entry itself —
															// no candidate to
															// synthesize and nothing
															// to defer. Every other shape
															// synthesizes the candidate now and
															// evaluates at flush time in
															// one batch: a falsy edge is dropped
															// there without touching
															// its record
															// (the selectivity win), a truthy one
															// proceeds to the
															// batched existence and permission
															// resolve. An edge whose
															// payload is
															// unusable falls back to the one
															// trustworthy source,
															// the edge record, prefetched per flush
															// with a single multi-get.
															// Target-less (legacy) entries carry no
															// far vertex, so they
															// take that fallback
															// either way. Scan order is
															// preserved throughout.
															let matched =
																match (&spec.matcher, &item.target)
																{
																	(
																		Some(matcher),
																		Some(target),
																	) if dir != Dir::Both => Some(matcher.matches(
																		rid,
																		&item.edge,
																		target,
																		dir == Dir::Out,
																	)),
																	_ => None,
																};
															if let Some(keep) = matched {
																if record_metrics {
																	metrics.add_props_evals(1);
																}
																if keep {
																	rid_batch.push(item.edge);
																}
															} else {
																let entry =
																	synthesize_payload_candidate(
																		spec, rid, dir, item,
																	);
																if record_metrics {
																	if entry.1.is_some() {
																		metrics.add_props_evals(1);
																	} else {
																		metrics
																			.add_props_fallbacks(1);
																	}
																}
																payload_pending.push(entry);
															}
														} else {
															// `TargetId` / `FullEdge` operate on
															// the edge identity.
															// Defer counting until after
															// filtering at flush time when a
															// predicate
															// is pushed down; otherwise
															// count each edge as it
															// is buffered.
															rid_batch.push(item.edge);
															if !has_predicate {
																edges_yielded += 1;
															}
														}
														if !has_predicate
															&& edge_limit
																.is_some_and(|l| edges_yielded >= l)
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
												}
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
											if should_flush && !has_predicate {
												// No predicate: when the resolve performs a
												// batch read, stash the batch on its own
												// resolve task so it overlaps whatever the
												// scan does next, draining anything already
												// pending first so emission stays in buffer
												// order. An identity-only resolve does no
												// I/O worth hiding, so it emits inline.
												// wasm has no worker threads, so it always
												// resolves and emits in place.
												#[cfg(not(target_family = "wasm"))]
												{
													if pipelined_resolve {
														if let Some(pending) = inflight.take() {
															let (values, cache, drained) =
																join_inflight_resolve(pending)
																	.await?;
															perm_cache = cache;
															spare = drained;
															yielder
																.emit(ValueBatch::new(values))
																.await;
														}
														debug_assert!(
															!has_predicate,
															"inflight stash is only valid without a pushed-down predicate"
														);
														let task_ctx = ctx.clone();
														let task_txn = Arc::clone(&txn);
														let task_cache =
															std::mem::take(&mut perm_cache);
														let rids = std::mem::replace(
															&mut rid_batch,
															std::mem::take(&mut spare),
														);
														if rid_batch.capacity() == 0 {
															rid_batch.reserve(scan_batch_size);
														}
														let mut set = tokio::task::JoinSet::new();
														set.spawn(async move {
															let mut rids = rids;
															let mut cache = task_cache;
															let values = resolve_and_filter_batch(
																&task_ctx,
																&task_txn,
																ns_id,
																db_id,
																&rids,
																fetch_full,
																check_perms,
																version,
																&mut cache,
																None,
																None,
															)
															.await?;
															rids.clear();
															Ok::<_, ControlFlow>((
																values, cache, rids,
															))
														});
														inflight = Some(set);
													} else {
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
															None,
															None,
														)
														.await?;
														rid_batch.clear();
														yielder.emit(ValueBatch::new(values)).await;
													}
												}
												#[cfg(target_family = "wasm")]
												{
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
														None,
														None,
													)
													.await?;
													rid_batch.clear();
													yielder.emit(ValueBatch::new(values)).await;
												}
											} else if should_flush {
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
												let flush_record_predicate =
													if payload_spec.is_some() {
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
									// The cursor is closed: drain the pipelined
									// flush before anything newer — the legacy
									// fallback below and later scopes emit after it.
									#[cfg(not(target_family = "wasm"))]
									if let Some(pending) = inflight.take() {
										let (values, cache, drained) =
											join_inflight_resolve(pending).await?;
										perm_cache = cache;
										spare = drained;
										yielder.emit(ValueBatch::new(values)).await;
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
														let endpoint =
															inner.decoder.decode_edge(ik).context(
																"Failed to decode graph key",
															)?;
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
					#[cfg(not(target_family = "wasm"))]
					if let Some(started) = win_timer {
						// The first window's sequential run doubles as the
						// timing probe: engage the spawned fan-out for the
						// batch's remaining windows only when the measured
						// per-source cost clears the dispatch-overhead bar,
						// so a cheap batch (a couple of edges per source)
						// keeps its exact sequential cost.
						let per_source =
							started.elapsed().as_nanos() as u64 / (win_end - win_start) as u64;
						fanout_engaged = Some(per_source >= GRAPH_SPAWN_PER_SOURCE_NANOS);
					}
					win_start = win_end;
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
	// A target-less (legacy-format) entry still answers a key-only
	// predicate that reads nothing beyond `id`; anything needing `in`,
	// `out` or the payload has only the record to trust.
	if item.target.is_none() && (!spec.key_only || spec.needs_in || spec.needs_out) {
		return (edge, None);
	}
	if matches!(dir, Dir::Both) {
		return (edge, None);
	}
	// A key-resident predicate reads only what the entry itself carries;
	// everything else needs the entry's payload, validated against the
	// plan-time snapshot.
	let fields: Vec<(String, Value)> = if spec.key_only {
		Vec::new()
	} else {
		let Some(table_spec) = spec.tables.get(&edge.table) else {
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
		table_spec
			.fields
			.iter()
			.zip(&table_spec.referenced)
			.zip(values)
			.filter(|((_, referenced), _)| **referenced)
			.map(|((name, _), value)| (name.clone(), value))
			.collect()
	};
	// The candidate carries only the fields the predicate reads — anything
	// more is a per-edge clone or insert spent on a value the evaluation
	// never touches.
	let mut object = crate::val::Object::default();
	if spec.needs_id {
		object.insert("id".to_owned(), Value::RecordId(edge.clone()));
	}
	if spec.needs_in || spec.needs_out {
		let target = item
			.target
			.as_ref()
			.expect("an in/out-reading predicate falls back on target-less entries");
		let (in_rid, out_rid) = match dir {
			Dir::Out => (source, target),
			Dir::In => (target, source),
			Dir::Both => unreachable!("handled above"),
		};
		if spec.needs_in {
			object.insert("in".to_owned(), Value::RecordId(in_rid.clone()));
		}
		if spec.needs_out {
			object.insert("out".to_owned(), Value::RecordId(out_rid.clone()));
		}
	}
	for (name, value) in fields {
		object.insert(name, value);
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
	perm_cache: &mut PermCache,
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

/// Compiled SELECT permissions, cached by table for the lifetime of one scan.
type PermCache = std::collections::HashMap<TableName, PhysicalPermission>;

/// A stashed flush's resolve, running on its own runtime task: a
/// single-task `JoinSet` (so dropping the stream aborts the task)
/// resolving to the batch's values, the permission cache the task
/// borrowed, and the cleared id buffer for recycling.
#[cfg(not(target_family = "wasm"))]
type InflightResolve =
	tokio::task::JoinSet<Result<(Vec<Value>, PermCache, Vec<RecordId>), ControlFlow>>;

/// Await the pipelined resolve task and hand back its values, the
/// permission cache, and the recycled id buffer.
#[cfg(not(target_family = "wasm"))]
async fn join_inflight_resolve(
	mut set: InflightResolve,
) -> Result<(Vec<Value>, PermCache, Vec<RecordId>), ControlFlow> {
	let joined = set.join_next().await.expect("the inflight resolve set holds exactly one task");
	joined.map_err(|e| ControlFlow::Err(anyhow::anyhow!("graph resolve task failed: {e}")))?
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

/// How many cache-missed sources' adjacency scans run side by side in the
/// plain-shape concurrent fan-out — and the width of the drain windows the
/// scan processes an input batch in, which bounds the aggregate prescan
/// footprint and keeps the output streaming between windows.
const GRAPH_FANOUT_CONCURRENCY: usize = 16;

/// The measured per-source scan cost below which the fan-out does not pay:
/// each spawned source costs a context clone, a handful of `Arc` bumps, a
/// `RecordId` clone and a task dispatch, which together sit at roughly this
/// scale — overlapping work cheaper than that loses more to dispatch than
/// it recovers from parallelism.
#[cfg(not(target_family = "wasm"))]
const GRAPH_SPAWN_PER_SOURCE_NANOS: u64 = 5_000;

/// One source's adjacency for the plain traversal shape — no predicate,
/// no payload — collected into an owned buffer of at most `cap` record
/// ids: the self-contained unit the concurrent fan-out runs per source.
/// `edge_limit` is the per-source cap a pushed LIMIT imposes: the buffer
/// stops at the cap, each batch request is sized to the remaining budget
/// so a small cap never over-fetches, and — because the planner admits an
/// ordered cap only for a single edge table in one fixed direction, one
/// adjacency scope whose merged cursor emits in ascending edge-id order —
/// the first `edge_limit` rows are exactly the rows the downstream sort
/// keeps. Mirrors the sequential loop's plain arms (the
/// zero-materialization visitor, the materialized folded/lightweight
/// path) — keep the two in step. Returns `None` when the source needs
/// machinery only the sequential loop carries: a legacy-format
/// (target-less) entry in `TargetVertex` mode, whose inner-scan fallback
/// and chunk resume stay sequential, or an adjacency larger than `cap`,
/// whose incremental flush-as-you-go emission bounds memory where this
/// buffer cannot; metrics and fold observations are withheld on that path
/// so the sequential re-scan records them once.
#[cfg(not(target_family = "wasm"))]
#[allow(clippy::too_many_arguments)]
async fn scan_source_plain(
	ctx: &ExecutionContext,
	txn: &Transaction,
	adjacency: VertexAdjacency,
	ns_id: NamespaceId,
	db_id: DatabaseId,
	rid: &RecordId,
	directions: &[Dir],
	edge_tables: &[EdgeTableSpec],
	output_mode: GraphScanOutput,
	target_tables: &[TableName],
	version: Option<u64>,
	edge_limit: Option<usize>,
	fold_threshold: usize,
	cap: usize,
	metrics: &OperatorMetrics,
	record_metrics: bool,
) -> Result<Option<Vec<RecordId>>, ControlFlow> {
	crate::exec::operators::check_cancelled(ctx)?;
	// A lightweight edge's synthesized adjacency derives from its id alone,
	// which says nothing about existence — gate exactly as the sequential
	// loop does.
	if adjacency == VertexAdjacency::Lightweight
		&& !txn
			.record_exists(ns_id, db_id, &rid.table, &rid.key, version)
			.await
			.context("Failed to check a lightweight source's existence")?
	{
		return Ok(Some(Vec::new()));
	}
	let mut out: Vec<RecordId> = Vec::new();
	let mut scanned: u64 = 0;
	let mut block_hits: u64 = 0;
	let mut delta_hits: u64 = 0;
	let mut capped = false;
	let mut observations: Vec<(Dir, u64)> = Vec::new();
	for &dir in directions {
		let scopes = compute_graph_ranges(ns_id, db_id, rid, dir, edge_tables, ctx).await?;
		let mut dir_delta_hits: u64 = 0;
		for scope in scopes {
			let adjacency_scope = AdjacencyScope {
				ns: ns_id,
				db: db_id,
				vertex: rid,
				dir: Some(dir),
				edge_table: scope.edge_table.as_ref(),
				fk_lower: scope.fk_lower.as_ref(),
				delta_range: scope.range.clone(),
			};
			let mut cursor = match adjacency {
				VertexAdjacency::Lightweight => {
					MergedAdjacencyCursor::open_lightweight(txn, &adjacency_scope)
						.context("Failed to open graph cursor")?
				}
				VertexAdjacency::Normal {
					folded,
				} => MergedAdjacencyCursor::open(
					txn,
					adjacency_scope,
					folded,
					version,
					Some(ctx.root().ctx.get_index_stores().adjacency_resolve()),
				)
				.await
				.context("Failed to open graph cursor")?,
			};
			// One flag for both bail-out shapes — a legacy entry and a
			// buffer at `cap` — since they share the contract: drop the
			// cursor and hand the whole source back to the sequential loop.
			let mut bail = false;
			loop {
				// Size each batch request to the remaining LIMIT budget so a
				// small cap never over-fetches, exactly as the sequential
				// path does; the buffer counts rows toward the cap, so this
				// is only reached with budget left.
				let batch_size = match edge_limit {
					Some(l) => {
						let remaining = l.saturating_sub(out.len());
						if remaining == 0 {
							capped = true;
							break;
						}
						remaining.min(crate::kvs::NORMAL_BATCH_SIZE as usize) as u32
					}
					None => crate::kvs::NORMAL_BATCH_SIZE,
				};
				let visited = cursor
					.visit_unfolded_batch(batch_size, |key| {
						let decoded = crate::key::schema::DecodedGraph::decode(key)?;
						if output_mode == GraphScanOutput::TargetVertex {
							match decoded.target {
								Some(target) => {
									if target_tables.is_empty()
										|| target_tables.contains(&target.table)
									{
										out.push(target);
									}
								}
								None => {
									bail = true;
									return Ok(VisitFlow::Stop);
								}
							}
						} else {
							out.push(decoded.edge);
						}
						if edge_limit.is_some_and(|l| out.len() >= l) {
							capped = true;
							return Ok(VisitFlow::Stop);
						}
						if out.len() >= cap {
							bail = true;
							return Ok(VisitFlow::Stop);
						}
						Ok(VisitFlow::Continue)
					})
					.await
					.context("Failed to scan graph edge")?;
				match visited {
					Some(0) => break,
					Some(n) => scanned += n as u64,
					None => {
						let batch = cursor
							.next_batch_scan(batch_size)
							.await
							.context("Failed to scan graph edge")?;
						if batch.is_empty() {
							break;
						}
						scanned += batch.len() as u64;
						for item in batch {
							if output_mode == GraphScanOutput::TargetVertex {
								match item.target {
									Some(target) => {
										if target_tables.is_empty()
											|| target_tables.contains(&target.table)
										{
											out.push(target);
										}
									}
									None => {
										bail = true;
										break;
									}
								}
							} else {
								out.push(item.edge);
							}
							if edge_limit.is_some_and(|l| out.len() >= l) {
								capped = true;
								break;
							}
							if out.len() >= cap {
								bail = true;
								break;
							}
						}
					}
				}
				if bail || capped {
					break;
				}
			}
			let stats = cursor.stats();
			dir_delta_hits += stats.delta_hits;
			block_hits += stats.block_hits;
			delta_hits += stats.delta_hits;
			drop(cursor);
			if bail {
				return Ok(None);
			}
			if capped {
				break;
			}
		}
		if capped {
			// A limit hit ends the scan before the direction completes, so
			// its fold observation is skipped — the sequential path breaks
			// out of its direction loop at the same point.
			break;
		}
		observations.push((dir, dir_delta_hits));
	}
	if record_metrics {
		metrics.add_edges_scanned(scanned);
		metrics.add_block_hits(block_hits);
		metrics.add_delta_hits(delta_hits);
	}
	for (dir, dir_delta_hits) in observations {
		observe_fold_candidate(txn, ns_id, db_id, rid, dir, dir_delta_hits, fold_threshold);
	}
	Ok(Some(out))
}

#[cfg(all(feature = "kv-mem", not(target_family = "wasm")))]
#[cfg(test)]
mod prescan_tests {
	use super::*;
	use crate::exec::operators::test_util::TestDb;

	/// A datastore with two source vertices: `person:big` with ten out-edges
	/// and `person:small` with three, every target existing.
	async fn traversal_db() -> TestDb {
		let mut setup = String::from("CREATE person:big; CREATE person:small;");
		for i in 0..10 {
			setup.push_str(&format!("CREATE person:t{i}; RELATE person:big->knows->person:t{i};"));
		}
		for i in 0..3 {
			setup.push_str(&format!("RELATE person:small->knows->person:t{i};"));
		}
		TestDb::new(&setup).await
	}

	/// One prescan of `person:{source}`'s out-adjacency under `cap`,
	/// returning its buffered targets (`None` is the sequential-fallback
	/// contract).
	async fn prescan(db: &TestDb, source: &str, cap: usize) -> Option<Vec<RecordId>> {
		let ctx = db.exec_ctx().await;
		let txn = ctx.txn();
		let db_ctx = ctx.database().expect("database context").clone();
		let tb = txn
			.get_tb(db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id, &"person".into(), None)
			.await
			.expect("the source table resolves");
		let metrics = OperatorMetrics::new();
		scan_source_plain(
			&ctx,
			&txn,
			vertex_adjacency_of(tb.as_deref()),
			db_ctx.ns_ctx.ns.namespace_id,
			db_ctx.db.database_id,
			&RecordId::new("person".into(), source.to_string()),
			&[Dir::Out],
			&[],
			GraphScanOutput::TargetVertex,
			&[],
			None,
			None,
			usize::MAX,
			cap,
			&metrics,
			false,
		)
		.await
		.expect("prescan should succeed")
	}

	#[tokio::test]
	async fn a_source_over_the_cap_defers_to_the_sequential_machinery() {
		let db = traversal_db().await;
		assert!(
			prescan(&db, "big", 5).await.is_none(),
			"a source with more edges than the cap must not buffer them all"
		);
	}

	#[tokio::test]
	async fn a_source_under_the_cap_returns_its_complete_adjacency() {
		let db = traversal_db().await;
		let mut out = prescan(&db, "small", 5).await.expect("three edges fit a cap of five");
		out.sort();
		let expected: Vec<RecordId> =
			(0..3).map(|i| RecordId::new("person".into(), format!("t{i}"))).collect();
		assert_eq!(out, expected);
	}
}

#[cfg(all(feature = "kv-mem", not(target_family = "wasm")))]
#[cfg(test)]
mod window_tests {
	use std::ops::Bound;

	use futures::StreamExt;
	use surrealdb_cnf::ConfigMap;

	use super::*;
	use crate::exec::operators::test_util::{TestDb, ValuesOperator, collect};

	/// A `GraphEdgeScan` over `person:s*->knows->person` in `TargetVertex`
	/// mode, with metrics enabled so a test can observe `edges_scanned`.
	fn knows_scan(sources: Vec<Value>) -> (Arc<dyn ExecOperator>, Arc<OperatorMetrics>) {
		let scan = GraphEdgeScan::new(
			ValuesOperator::new(sources),
			LookupDirection::Out,
			vec![EdgeTableSpec {
				table: "knows".into(),
				range_start: Bound::Unbounded,
				range_end: Bound::Unbounded,
			}],
			GraphScanOutput::TargetVertex,
			None,
		);
		scan.metrics.enable();
		let metrics = Arc::clone(&scan.metrics);
		(Arc::new(scan), metrics)
	}

	fn person(key: impl std::fmt::Display) -> Value {
		Value::RecordId(RecordId::new("person".into(), key.to_string()))
	}

	/// Dropping the output stream after one batch must abandon the sources
	/// not yet reached: the scan works window by window, emitting between
	/// windows, so a downstream `LIMIT` never pays for the whole input
	/// batch's adjacency.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn dropping_the_stream_stops_the_remaining_windows() {
		const SOURCES: usize = 64;
		const EDGES_PER_SOURCE: usize = 8;
		let mut setup = String::new();
		for j in 0..EDGES_PER_SOURCE {
			setup.push_str(&format!("CREATE person:t{j};"));
		}
		for i in 0..SOURCES {
			setup.push_str(&format!("CREATE person:s{i};"));
			for j in 0..EDGES_PER_SOURCE {
				setup.push_str(&format!("RELATE person:s{i}->knows->person:t{j};"));
			}
		}
		let db = TestDb::new_with_config(
			&setup,
			ConfigMap::empty().with_key_value("scan_batch_size", "8"),
		)
		.await;
		let ctx = db.exec_ctx().await;

		let (op, metrics) = knows_scan((0..SOURCES).map(|i| person(format!("s{i}"))).collect());
		let mut stream = op.execute(&ctx).expect("execute should succeed");
		let first = stream.next().await.expect("one batch").expect("batch should be Ok");
		assert!(!first.values().is_empty(), "the first batch should carry rows");
		drop(stream);

		let scanned = metrics.edges_scanned();
		let window = (GRAPH_FANOUT_CONCURRENCY * EDGES_PER_SOURCE) as u64;
		assert!(
			scanned <= 2 * window,
			"dropping after one batch should stop the later windows: \
			 scanned {scanned} edges, expected at most two windows ({})",
			2 * window
		);
	}

	/// Results must be complete and exact across every boundary the
	/// windowed fan-out introduces: window edges, the per-source prescan
	/// cap (an over-cap source falls back to the sequential machinery), and
	/// the flush size — no row dropped, none double-emitted.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn results_are_exact_across_window_and_cap_boundaries() {
		const SOURCES: usize = 40;
		const HUB_EDGES: usize = 20;
		let mut setup = String::from("CREATE person:s0;");
		for j in 0..HUB_EDGES {
			setup.push_str(&format!("CREATE person:u{j}; RELATE person:s0->knows->person:u{j};"));
		}
		for i in 1..SOURCES {
			setup.push_str(&format!(
				"CREATE person:s{i}; CREATE person:v{i}; \
				 RELATE person:s{i}->knows->person:v{i};"
			));
		}
		// A scan_batch_size of 8 is both the prescan cap (under the hub's
		// twenty edges) and the flush size, so every boundary is crossed.
		let db = TestDb::new_with_config(
			&setup,
			ConfigMap::empty().with_key_value("scan_batch_size", "8"),
		)
		.await;
		let ctx = db.exec_ctx().await;

		let (op, _) = knows_scan((0..SOURCES).map(|i| person(format!("s{i}"))).collect());
		let mut out = collect(&op, &ctx).await;
		out.sort();

		let mut expected: Vec<Value> = (0..HUB_EDGES)
			.map(|j| person(format!("u{j}")))
			.chain((1..SOURCES).map(|i| person(format!("v{i}"))))
			.collect();
		expected.sort();
		assert_eq!(out, expected);
	}
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
