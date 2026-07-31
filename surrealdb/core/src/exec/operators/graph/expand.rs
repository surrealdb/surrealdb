//! The `Expand` operator — single-hop graph traversal for GQL v2 `MATCH`.
//!
//! `Expand` is the workhorse of binding-table execution. Given a stream of
//! binding rows (each a `Value::Object` keyed by binding name, per
//! `doc/gql/V2_DESIGN.md` §3), it expands one edge step from a bound source
//! node, binding the traversed edge and the reached target node into each
//! surviving row.
//!
//! For every input row it:
//!
//! 1. reads `row[source].id` (the source node's record id) — see the source-extraction rules below;
//! 2. enumerates the source vertex's adjacency in `direction`, restricted to `edge_tables` (empty ⇒
//!    all edge tables), via the shared [`graph_keys`](super::super::scan::graph_keys) machinery;
//! 3. resolves the edge binding — `Full` fetches the edge record through the FieldState-aware
//!    [`fetch`](crate::exec::operators::scan::fetch) helper; `IdOnly` carries just the edge
//!    `Value::RecordId` with no fetch;
//! 4. determines the target vertex id (see the target-rid decision below);
//! 5. applies `target_label` (a candidate whose target table differs is dropped);
//! 6. batch-fetches the target node through the FieldState-aware helper (a miss or permission
//!    denial drops the candidate);
//! 7. assembles `input ∪ {edge_binding, target_binding}` and evaluates the optional `predicate`
//!    against that candidate row (mirroring `Filter`);
//! 8. emits the survivors.
//!
//! ## Source extraction
//!
//! - `row[source]` is a full record object ⇒ its `id` field (a `Value::RecordId`) is the source id;
//! - `row[source]` is itself a `Value::RecordId` ⇒ used directly (defensive; node bindings normally
//!   hold full objects);
//! - `row[source]` is `Value::Null` ⇒ the source is an optional miss: when `optional` the row
//!   passes through with `edge`/`target` bound to `Null`, otherwise it is skipped;
//! - the binding is missing, or holds any other (non-record, non-null) value ⇒ the row is dropped
//!   with a `debug!` log (never a panic). This only happens on a malformed plan; the lowering
//!   guarantees the source binding is a bound node.
//!
//! ## `IdOnly` edges and the target-rid decision
//!
//! The adjacency keys scanned here are *pointer keys* (vertex-side adjacency):
//! [`DecodedGraph::edge`](super::super::scan::graph_keys::DecodedGraph) is the
//! **edge record id**, and [`DecodedGraph::target`] is **`Some` only for
//! new-format keys**, carrying the far endpoint vertex directly.
//!
//! The target vertex id is therefore resolved in priority order:
//!
//! 1. `decoded.target` when present — covers new-format keys for *both* edge modes with zero extra
//!    work;
//! 2. otherwise the fetched edge object's `out` (for `Out`) / `in` (for `In`) field — available for
//!    free in `Full` mode on legacy keys;
//! 3. otherwise (`IdOnly` mode on a legacy key, where the edge is not fetched) a minimal edge fetch
//!    through the FieldState helper to read its `out`/`in`.
//!
//! `IdOnly` binds the edge as a bare `Value::RecordId` (no record content
//! surfaces) per §3, but still needs the target id; the decode supplies it on
//! new-format data, and we fall back to a minimal fetch only on un-migrated
//! legacy edges. This keeps the common path fetch-free while staying correct on
//! legacy storage.
//!
//! ## Edge SELECT-permission gating (security)
//!
//! Binding the edge id-only must not let a user *traverse* an edge table they
//! cannot SELECT — otherwise `->edge` would enumerate otherwise-hidden
//! relationships even though the edge content is never returned.
//! [`GraphEdgeScan`](super::super::scan::graph::GraphEdgeScan) gates its
//! id-only output on the edge perm for exactly this reason. So when permissions
//! are enforced (`check_perms`), `IdOnly` edges are still resolved through the
//! FieldState-aware [`fetch`](crate::exec::operators::scan::fetch) helper purely to *gate*
//! traversal: an edge denied (or missing) yields `None` and the candidate target is
//! dropped, while the bound value stays the bare `Value::RecordId`. The gating
//! fetch doubles as the legacy-key `out`/`in` source, so it costs at most one
//! extra batched read on the secured path. For root/owner sessions
//! (`check_perms == false`) the gate is skipped and the common path remains
//! fetch-free.

// The GQL v2 MATCH operators are constructed only by the gql-gated
// planner (`Expr::Match` is `#[cfg(feature = "gql")]`), so they are dead
// code when the feature is off — suppress the lint there only, keeping
// dead-code detection active in the default (gql-on) build.
#![cfg_attr(not(feature = "gql"), allow(dead_code))]

use std::sync::Arc;

use common::future::stream::{self, Yielder};
use futures::StreamExt;
use tracing::debug;

use crate::exec::operators::scan::fetch::{FetchFieldStateCache, resolve_with_field_state};
use crate::exec::operators::scan::graph_keys::{
	EdgeTableSpec, compute_graph_ranges, decode_graph_edge,
};
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, ControlFlowExt, EvalContext, ExecOperator,
	ExecutionContext, FlowResult, OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream,
	buffer_stream, monitor_stream,
};
use crate::expr::{ControlFlow, Dir};
use crate::idx::planner::ScanDirection;
use crate::val::{Object, RecordId, TableName, Value};

/// Direction of a single `Expand` hop.
///
/// GQL `MATCH` patterns only ever traverse a directed edge `->` (`Out`) or `<-`
/// (`In`); there is no undirected hop at the operator level. This is the
/// operator-local mirror of the `match_plan::ExpandDirection` IR enum (the
/// planner, which is feature-gated, translates one into the other), kept local
/// so this operator compiles without the `gql` feature — matching the
/// `match/fetch.rs` precedent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpandDir {
	/// Outgoing edge `->`: target is the edge's `out` endpoint.
	Out,
	/// Incoming edge `<-`: target is the edge's `in` endpoint.
	In,
}

impl ExpandDir {
	/// Map to the storage-layer [`Dir`] used for adjacency range computation.
	fn as_dir(self) -> Dir {
		match self {
			ExpandDir::Out => Dir::Out,
			ExpandDir::In => Dir::In,
		}
	}

	/// The edge-record field holding the target vertex for this direction.
	fn target_field(self) -> &'static str {
		match self {
			ExpandDir::Out => "out",
			ExpandDir::In => "in",
		}
	}

	/// EXPLAIN arrow rendering.
	fn arrow(self) -> &'static str {
		match self {
			ExpandDir::Out => "->",
			ExpandDir::In => "<-",
		}
	}
}

/// How the traversed edge is bound into each surviving row.
#[derive(Debug, Clone)]
pub enum EdgeBinding {
	/// Bind the full edge record (FieldState-aware fetch) under this name.
	Full(String),
	/// Bind only the edge `Value::RecordId` under this name (no fetch).
	IdOnly(String),
}

impl EdgeBinding {
	/// The binding name regardless of mode.
	fn name(&self) -> &str {
		match self {
			EdgeBinding::Full(n) | EdgeBinding::IdOnly(n) => n,
		}
	}

	/// Whether the edge record must be fetched.
	fn fetch_full(&self) -> bool {
		matches!(self, EdgeBinding::Full(_))
	}
}

/// Single-hop graph expansion over a stream of binding rows.
///
/// See the module docs for the per-row algorithm and the target-rid decision.
#[derive(Debug, Clone)]
pub struct Expand {
	/// Child operator producing input binding rows.
	pub(crate) input: Arc<dyn ExecOperator>,
	/// Name of the bound source node binding in each input row.
	pub(crate) source: String,
	/// Edge traversal direction.
	pub(crate) direction: ExpandDir,
	/// Edge tables to traverse; empty ⇒ all edge tables in `direction`.
	pub(crate) edge_tables: Vec<TableName>,
	/// How to bind the traversed edge.
	pub(crate) edge_binding: EdgeBinding,
	/// Name of the target node binding to introduce.
	pub(crate) target_binding: String,
	/// Optional label filter on the target vertex's table.
	pub(crate) target_label: Option<TableName>,
	/// Optional predicate over the assembled candidate row.
	pub(crate) predicate: Option<Arc<dyn PhysicalExpr>>,
	/// When `true`, an input row with zero survivors emits one row with the
	/// edge and target bindings set to `Value::Null` (left-outer semantics).
	pub(crate) optional: bool,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl Expand {
	/// Create a new `Expand` operator with fresh metrics.
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		source: String,
		direction: ExpandDir,
		edge_tables: Vec<TableName>,
		edge_binding: EdgeBinding,
		target_binding: String,
		target_label: Option<TableName>,
		predicate: Option<Arc<dyn PhysicalExpr>>,
		optional: bool,
	) -> Self {
		Self {
			input,
			source,
			direction,
			edge_tables,
			edge_binding,
			target_binding,
			target_label,
			predicate,
			optional,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	/// Build the `EdgeTableSpec` list (unbounded ranges) for the configured
	/// edge tables. An empty list yields the all-edges wildcard scan.
	fn edge_specs(&self) -> Vec<EdgeTableSpec> {
		self.edge_tables
			.iter()
			.cloned()
			.map(|table| EdgeTableSpec {
				table,
				range_start: std::ops::Bound::Unbounded,
				range_end: std::ops::Bound::Unbounded,
			})
			.collect()
	}
}

impl ExecOperator for Expand {
	fn name(&self) -> &'static str {
		if self.optional {
			"OptionalExpand"
		} else {
			"Expand"
		}
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let tables = if self.edge_tables.is_empty() {
			"*".to_string()
		} else {
			self.edge_tables.iter().map(|t| t.as_str()).collect::<Vec<_>>().join(", ")
		};
		let mut attrs = vec![
			("source".to_string(), self.source.clone()),
			("direction".to_string(), self.direction.arrow().to_string()),
			("tables".to_string(), tables),
			("edge".to_string(), self.edge_binding.name().to_string()),
			("target".to_string(), self.target_binding.clone()),
		];
		if let Some(label) = self.target_label.as_ref() {
			attrs.push(("target_label".to_string(), label.as_str().to_string()));
		}
		if let Some(predicate) = self.predicate.as_ref() {
			attrs.push(("predicate".to_string(), predicate.to_sql()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		let mut level = self.input.required_context().max(ContextLevel::Database);
		if let Some(predicate) = self.predicate.as_ref() {
			level = level.max(predicate.required_context());
		}
		level
	}

	fn access_mode(&self) -> AccessMode {
		let mut mode = self.input.access_mode();
		if let Some(predicate) = self.predicate.as_ref() {
			mode = mode.combine(predicate.access_mode());
		}
		mode
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		// One input row fans out to an unknown number of edges/targets.
		CardinalityHint::Unbounded
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		match self.predicate.as_ref() {
			Some(predicate) => vec![("predicate", predicate)],
			None => vec![],
		}
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let db_ctx = ctx.database()?.clone();
		// SECURITY: target/edge binding contents are produced by the FieldState
		// helper ([`resolve_with_field_state`]), which resolves whether SELECT
		// permissions must be enforced (once, internally) and applies table +
		// field SELECT permissions and computed fields per record. The operator
		// therefore does not re-derive the permission-check flag itself.
		let mut input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);

		let source = self.source.clone();
		let direction = self.direction;
		let edge_specs = self.edge_specs();
		let edge_binding = self.edge_binding.clone();
		let target_binding = self.target_binding.clone();
		let target_label = self.target_label.clone();
		let predicate = self.predicate.clone();
		let optional = self.optional;
		let scan_batch_size = ctx.root().ctx.config.exec.scan_batch_size;
		let max_output_rows = ctx.root().ctx.config.exec.gql_max_output_rows;
		let ctx = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			let txn = ctx.txn();
			let ns_id = db_ctx.ns_ctx.ns.namespace_id;
			let db_id = db_ctx.db.database_id;
			let version = ctx.version_stamp();
			let dir = direction.as_dir();
			let fetch_full_edge = edge_binding.fetch_full();
			let edge_name = edge_binding.name().to_string();

			// FieldState caches: one for edges, one for targets. They are
			// keyed by table internally and survive the whole stream so each
			// table's permission + FieldState are resolved at most once.
			let mut edge_cache = FetchFieldStateCache::new();
			let mut target_cache = FetchFieldStateCache::new();

			// Cumulative emitted-row count across all batches and source rows —
			// bounds high-fan-out expand output (a single dense source vertex, or
			// a long streaming input, can fan out far past memory). The per-source
			// optional null-fills are 1-per-row and bounded by the input, so only
			// the survivor pushes inside `expand_row` are counted.
			let mut emitted: usize = 0;

			while let Some(batch_result) = input_stream.next().await {
				crate::exec::operators::check_cancelled(&ctx)?;
				let batch = batch_result?;
				let mut out: Vec<Value> = Vec::with_capacity(batch.values.len());

				for row in batch.values {
					match extract_source_id(&row, &source) {
						SourceId::Found(rid) => {
							expand_row(
								&ctx,
								&txn,
								ns_id,
								db_id,
								version,
								&row,
								&rid,
								dir,
								direction,
								&edge_specs,
								&edge_name,
								fetch_full_edge,
								&target_binding,
								target_label.as_ref(),
								predicate.as_deref(),
								optional,
								scan_batch_size,
								&mut edge_cache,
								&mut target_cache,
								&mut out,
								&mut emitted,
								max_output_rows,
							)
							.await?;
						}
						SourceId::OptionalMiss => {
							// Source is a prior optional miss (Null). Left-outer
							// propagation: keep the row, nulling this hop's
							// bindings when this expand is itself optional;
							// otherwise an inner expand from a null source
							// produces nothing.
							if optional {
								out.push(null_filled_row(&row, &edge_name, &target_binding));
							}
						}
						SourceId::Drop => {
							// Malformed plan: the source binding is missing or
							// not a record. The lowering guarantees a bound node
							// here, so this is unreachable on valid plans; drop
							// without panicking.
							debug!(
								binding = %source,
								"Expand: source binding is missing or not a record id; dropping row"
							);
						}
					}
				}

				if !out.is_empty() {
					yielder
						.emit(ValueBatch {
							values: out,
						})
						.await;
				}
			}
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), self.name(), &self.metrics))
	}
}

/// Result of locating the source record id within an input binding row.
enum SourceId {
	/// A concrete source record id was found.
	Found(RecordId),
	/// The source binding is `Value::Null` (a prior optional miss).
	OptionalMiss,
	/// The binding is missing or not a record id; the row must be dropped.
	Drop,
}

/// Locate `row[source].id` per the source-extraction rules (module docs).
fn extract_source_id(row: &Value, source: &str) -> SourceId {
	let Value::Object(obj) = row else {
		return SourceId::Drop;
	};
	match obj.get(source) {
		Some(Value::Object(node)) => match node.get("id") {
			Some(Value::RecordId(rid)) => SourceId::Found(rid.clone()),
			_ => SourceId::Drop,
		},
		// Defensive: a node binding that is itself a bare record id.
		Some(Value::RecordId(rid)) => SourceId::Found(rid.clone()),
		Some(Value::Null) => SourceId::OptionalMiss,
		// Missing binding, or a non-record/non-null value.
		_ => SourceId::Drop,
	}
}

/// Assemble a candidate binding row: clone the input object and insert the
/// edge and target bindings.
fn candidate_row(
	input: &Value,
	edge_name: &str,
	edge: Value,
	target_binding: &str,
	target: Value,
) -> Value {
	let mut obj = match input {
		Value::Object(o) => o.clone(),
		// `extract_source_id` only yields `Found` for object rows, so this is
		// unreachable in practice; default to an empty object defensively.
		_ => Object::default(),
	};
	obj.insert(edge_name, edge);
	obj.insert(target_binding, target);
	Value::Object(obj)
}

/// Build a row for an optional miss: the input row with the edge and target
/// bindings set to `Value::Null`.
fn null_filled_row(input: &Value, edge_name: &str, target_binding: &str) -> Value {
	candidate_row(input, edge_name, Value::Null, target_binding, Value::Null)
}

/// Read a `RecordId` from an edge object's `out`/`in` field for `direction`.
fn target_from_edge_obj(edge: &Value, direction: ExpandDir) -> Option<RecordId> {
	let Value::Object(obj) = edge else {
		return None;
	};
	match obj.get(direction.target_field()) {
		Some(Value::RecordId(rid)) => Some(rid.clone()),
		_ => None,
	}
}

/// Enumerate one input row's adjacency and emit surviving candidate rows.
#[allow(clippy::too_many_arguments)]
async fn expand_row(
	ctx: &ExecutionContext,
	txn: &Arc<crate::kvs::Transaction>,
	ns_id: crate::catalog::NamespaceId,
	db_id: crate::catalog::DatabaseId,
	version: Option<u64>,
	input_row: &Value,
	source_rid: &RecordId,
	dir: Dir,
	direction: ExpandDir,
	edge_specs: &[EdgeTableSpec],
	edge_name: &str,
	fetch_full_edge: bool,
	target_binding: &str,
	target_label: Option<&TableName>,
	predicate: Option<&dyn PhysicalExpr>,
	optional: bool,
	scan_batch_size: usize,
	edge_cache: &mut FetchFieldStateCache,
	target_cache: &mut FetchFieldStateCache,
	out: &mut Vec<Value>,
	emitted: &mut usize,
	max_output_rows: usize,
) -> Result<(), ControlFlow> {
	// Enumerate the source vertex's adjacency, collecting (edge rid, optional
	// decoded target) pairs. `decoded.edge` is the edge record id; `decoded.
	// target` is the far vertex on new-format pointer keys.
	let ranges = compute_graph_ranges(ns_id, db_id, source_rid, dir, edge_specs, ctx).await?;

	let mut edge_rids: Vec<RecordId> = Vec::new();
	let mut decoded_targets: Vec<Option<RecordId>> = Vec::new();

	for range in ranges {
		let mut cursor = txn
			.open_keys_cursor_raw(range, ScanDirection::Forward, 0, version)
			.await
			.context("Failed to open graph cursor")?;
		loop {
			crate::exec::operators::check_cancelled(ctx)?;
			let keys = cursor
				.next_batch(crate::kvs::NORMAL_BATCH_SIZE)
				.await
				.context("Failed to scan graph edge")?;
			if keys.is_empty() {
				break;
			}
			for key in &keys {
				let decoded = decode_graph_edge(key)?;
				edge_rids.push(decoded.edge);
				decoded_targets.push(decoded.target);
			}
		}
		drop(cursor);
	}

	if edge_rids.is_empty() {
		if optional {
			out.push(null_filled_row(input_row, edge_name, target_binding));
		}
		return Ok(());
	}

	// SECURITY: an id-only (hidden / anonymous) edge binds just the edge's
	// record id and never fetches the edge record on the common new-format path
	// (the target is decoded straight from the adjacency key). The edge table's
	// SELECT permission would therefore never be evaluated, letting a user
	// denied SELECT on the edge table still traverse `->edge` to surface the
	// far node — leaking otherwise-hidden relationships. `GraphEdgeScan`
	// (scan/graph.rs) gates its id-only output on the edge perm for exactly this
	// reason. So when permissions are enforced we still resolve every id-only
	// edge through the FieldState helper purely to *gate* traversal: a denied /
	// missing edge (`None`) drops the candidate. For root/owner sessions
	// (`check_perms == false`) the gate is a no-op and the fetch-free fast path
	// is preserved.
	let gate_id_only_edges = !fetch_full_edge && edge_cache.check_perms(ctx)?;

	// Survivor count for this input row, so OptionalExpand can emit its
	// null-filled fallback only when nothing matched.
	let before = out.len();

	// Process in batches bounded by `scan_batch_size` so high-fanout sources
	// don't buffer an unbounded number of edges at once.
	let mut start = 0;
	while start < edge_rids.len() {
		let end = (start + scan_batch_size).min(edge_rids.len());
		let edge_slice = &edge_rids[start..end];
		let decoded_slice = &decoded_targets[start..end];

		// Resolve the edge bindings. In `Full` mode every edge is fetched
		// through the FieldState helper (its perm gating is the `Some`/`None`
		// of `edge_values[i]`). In `IdOnly` mode the *bound value* is always the
		// bare record id (per V2_DESIGN §3, no record content surfaces);
		// `edge_perm` separately records whether the edge passed the table /
		// field SELECT permission, computed by a gating fetch when permissions
		// are enforced (see `gate_id_only_edges`). Both vectors are positional
		// with `edge_slice`.
		let (edge_values, edge_perm): (Vec<Option<Value>>, Vec<Option<Value>>) = if fetch_full_edge
		{
			let fetched = resolve_with_field_state(ctx, edge_cache, edge_slice).await?;
			// In Full mode the binding value IS the fetched object; the gate
			// reuses it directly (no second fetch).
			(fetched.clone(), fetched)
		} else if gate_id_only_edges {
			// IdOnly with permissions enforced: bind the bare id, but gate on a
			// FieldState fetch of the edge record. The fetched object also
			// supplies `out`/`in` for legacy keys for free.
			let gated = resolve_with_field_state(ctx, edge_cache, edge_slice).await?;
			let values = edge_slice.iter().map(|rid| Some(Value::RecordId(rid.clone()))).collect();
			(values, gated)
		} else {
			// IdOnly fast path (no permission enforcement): bind the bare id,
			// no edge fetch and no gate (every edge is allowed).
			let values: Vec<Option<Value>> =
				edge_slice.iter().map(|rid| Some(Value::RecordId(rid.clone()))).collect();
			let perm = vec![None; edge_slice.len()];
			(values, perm)
		};

		// Determine the target rid for each adjacency entry (priority order:
		// decoded target ⇒ fetched/gating edge `out`/`in` ⇒ minimal edge fetch),
		// and collect a parallel batch of target rids to resolve.
		// `legacy_positions` records which entries still need a minimal edge
		// fetch (legacy IdOnly keys with no perm-gating fetch already in hand).
		let mut target_rids: Vec<Option<RecordId>> = Vec::with_capacity(edge_slice.len());
		let mut legacy_edge_fetch: Vec<RecordId> = Vec::new();
		let mut legacy_positions: Vec<usize> = Vec::new();

		for (i, edge_rid) in edge_slice.iter().enumerate() {
			// An edge denied by the SELECT permission cannot let its target
			// surface; drop the candidate and never read the denied edge again.
			// In Full mode `edge_values[i]` is the gate; in gated IdOnly mode
			// `edge_perm[i]` is. (Fast-path IdOnly leaves both permissive.)
			let edge_denied = (fetch_full_edge && edge_values[i].is_none())
				|| (gate_id_only_edges && edge_perm[i].is_none());
			if edge_denied {
				target_rids.push(None);
			} else if let Some(target) = decoded_slice[i].clone() {
				// (1) New-format key: target carried in the adjacency. No edge
				// read needed.
				target_rids.push(Some(target));
			} else if let Some(edge_val) = edge_perm[i].as_ref() {
				// (2) Legacy key with the edge record already in hand (Full mode,
				// or gated IdOnly): read the endpoint from its `out`/`in`.
				target_rids.push(target_from_edge_obj(edge_val, direction));
			} else {
				// (3) Legacy key, fast-path IdOnly: the edge was not fetched, so
				// defer a minimal FieldState fetch purely to recover `out`/`in`.
				target_rids.push(None);
				legacy_edge_fetch.push(edge_rid.clone());
				legacy_positions.push(i);
			}
		}

		// Minimal edge fetch for legacy fast-path IdOnly entries: read the edge
		// records (FieldState-aware) purely to recover their `out`/`in`
		// endpoint.
		if !legacy_edge_fetch.is_empty() {
			let fetched = resolve_with_field_state(ctx, edge_cache, &legacy_edge_fetch).await?;
			for (pos, edge_val) in legacy_positions.iter().zip(fetched) {
				if let Some(edge_val) = edge_val {
					target_rids[*pos] = target_from_edge_obj(&edge_val, direction);
				}
			}
		}

		// Apply the target_label filter and gather the target rids to fetch.
		// `candidate_positions` maps each fetched-target slot back to its
		// adjacency index so we can pair it with the right edge value.
		let mut targets_to_fetch: Vec<RecordId> = Vec::new();
		let mut candidate_positions: Vec<usize> = Vec::new();
		for (i, target) in target_rids.iter().enumerate() {
			let Some(target) = target else {
				continue;
			};
			if let Some(label) = target_label
				&& &target.table != label
			{
				continue;
			}
			targets_to_fetch.push(target.clone());
			candidate_positions.push(i);
		}

		// Batch-fetch the target nodes (FieldState-aware). Positional with
		// `targets_to_fetch`; `None` ⇒ missing or permission-denied ⇒ drop.
		let target_values = resolve_with_field_state(ctx, target_cache, &targets_to_fetch).await?;

		// Base eval context for the predicate (no current value yet).
		let base_eval = EvalContext::from_exec_ctx(ctx);

		for (slot, target_value) in target_values.into_iter().enumerate() {
			let Some(target_value) = target_value else {
				continue;
			};
			let i = candidate_positions[slot];

			// The edge binding value. Permission-denied edges (Full or gated
			// IdOnly) were already excluded above (their `target_rids` slot was
			// `None`, so they are absent from `candidate_positions`); this `None`
			// skip is the belt-and-braces guard for Full mode, where the dropped
			// object would otherwise leak the denied edge's existence.
			let edge_value = match edge_values[i].clone() {
				Some(v) => v,
				None => continue,
			};

			let candidate =
				candidate_row(input_row, edge_name, edge_value, target_binding, target_value);

			// Predicate evaluation against the assembled candidate row, copying
			// Filter's per-row idiom (`EvalContext::with_value`).
			if let Some(predicate) = predicate {
				let result = predicate.evaluate(base_eval.with_value(&candidate)).await?;
				if !result.is_truthy() {
					continue;
				}
			}

			*emitted += 1;
			if *emitted > max_output_rows {
				return Err(crate::exec::operators::gql_output_rows_exceeded(max_output_rows));
			}
			out.push(candidate);
		}

		start = end;
	}

	// OptionalExpand: zero survivors for this input row ⇒ emit one null-filled
	// row (left-outer semantics).
	if optional && out.len() == before {
		out.push(null_filled_row(input_row, edge_name, target_binding));
	}

	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::CurrentValueSource;

	fn obj_row(pairs: &[(&str, Value)]) -> Value {
		let mut o = Object::default();
		for (k, v) in pairs {
			o.insert(*k, v.clone());
		}
		Value::Object(o)
	}

	fn rid(table: &str, key: &str) -> RecordId {
		RecordId {
			table: TableName::new(table.to_string()),
			key: crate::val::RecordIdKey::from(key.to_string()),
		}
	}

	fn sample_expand(optional: bool) -> Expand {
		Expand::new(
			Arc::new(CurrentValueSource::new()),
			"a".to_string(),
			ExpandDir::Out,
			vec![TableName::new("knows".to_string())],
			EdgeBinding::Full("k".to_string()),
			"b".to_string(),
			Some(TableName::new("person".to_string())),
			None,
			optional,
		)
	}

	#[test]
	fn name_reflects_optional() {
		assert_eq!(sample_expand(false).name(), "Expand");
		assert_eq!(sample_expand(true).name(), "OptionalExpand");
	}

	#[test]
	fn attrs_render_direction_tables_and_label() {
		let attrs = sample_expand(false).attrs();
		assert!(attrs.iter().any(|(k, v)| k == "source" && v == "a"));
		assert!(attrs.iter().any(|(k, v)| k == "direction" && v == "->"));
		assert!(attrs.iter().any(|(k, v)| k == "tables" && v == "knows"));
		assert!(attrs.iter().any(|(k, v)| k == "edge" && v == "k"));
		assert!(attrs.iter().any(|(k, v)| k == "target" && v == "b"));
		assert!(attrs.iter().any(|(k, v)| k == "target_label" && v == "person"));
	}

	#[test]
	fn attrs_render_wildcard_tables() {
		let mut e = sample_expand(false);
		e.edge_tables.clear();
		let attrs = e.attrs();
		assert!(attrs.iter().any(|(k, v)| k == "tables" && v == "*"));
	}

	#[test]
	fn required_context_is_at_least_database() {
		assert_eq!(sample_expand(false).required_context(), ContextLevel::Database);
	}

	#[test]
	fn edge_specs_unbounded_per_table() {
		let e = sample_expand(false);
		let specs = e.edge_specs();
		assert_eq!(specs.len(), 1);
		assert_eq!(specs[0].table.as_str(), "knows");
		assert!(matches!(specs[0].range_start, std::ops::Bound::Unbounded));
		assert!(matches!(specs[0].range_end, std::ops::Bound::Unbounded));
	}

	#[test]
	fn edge_specs_empty_for_wildcard() {
		let mut e = sample_expand(false);
		e.edge_tables.clear();
		assert!(e.edge_specs().is_empty());
	}

	#[test]
	fn dir_mapping_and_target_field() {
		assert_eq!(ExpandDir::Out.as_dir(), Dir::Out);
		assert_eq!(ExpandDir::In.as_dir(), Dir::In);
		assert_eq!(ExpandDir::Out.target_field(), "out");
		assert_eq!(ExpandDir::In.target_field(), "in");
	}

	#[test]
	fn edge_binding_name_and_fetch_flag() {
		assert_eq!(EdgeBinding::Full("k".to_string()).name(), "k");
		assert_eq!(EdgeBinding::IdOnly("k".to_string()).name(), "k");
		assert!(EdgeBinding::Full("k".to_string()).fetch_full());
		assert!(!EdgeBinding::IdOnly("k".to_string()).fetch_full());
	}

	#[test]
	fn extract_source_id_from_full_object() {
		let row = obj_row(&[("a", obj_row(&[("id", Value::RecordId(rid("person", "alice")))]))]);
		match extract_source_id(&row, "a") {
			SourceId::Found(r) => assert_eq!(r, rid("person", "alice")),
			_ => panic!("expected Found"),
		}
	}

	#[test]
	fn extract_source_id_from_bare_record_id() {
		let row = obj_row(&[("a", Value::RecordId(rid("person", "bob")))]);
		match extract_source_id(&row, "a") {
			SourceId::Found(r) => assert_eq!(r, rid("person", "bob")),
			_ => panic!("expected Found"),
		}
	}

	#[test]
	fn extract_source_id_null_is_optional_miss() {
		let row = obj_row(&[("a", Value::Null)]);
		assert!(matches!(extract_source_id(&row, "a"), SourceId::OptionalMiss));
	}

	#[test]
	fn extract_source_id_missing_or_bad_is_drop() {
		let missing = obj_row(&[("z", Value::Bool(true))]);
		assert!(matches!(extract_source_id(&missing, "a"), SourceId::Drop));
		let bad = obj_row(&[("a", Value::Bool(true))]);
		assert!(matches!(extract_source_id(&bad, "a"), SourceId::Drop));
		// A node object missing its `id` field is also a drop.
		let no_id = obj_row(&[("a", obj_row(&[("name", Value::Bool(true))]))]);
		assert!(matches!(extract_source_id(&no_id, "a"), SourceId::Drop));
		// A non-object row drops.
		assert!(matches!(extract_source_id(&Value::Bool(true), "a"), SourceId::Drop));
	}

	#[test]
	fn candidate_row_inserts_edge_and_target() {
		let input = obj_row(&[("a", Value::RecordId(rid("person", "alice")))]);
		let row = candidate_row(
			&input,
			"k",
			Value::RecordId(rid("knows", "e1")),
			"b",
			Value::RecordId(rid("person", "bob")),
		);
		let Value::Object(o) = row else {
			panic!("expected object");
		};
		assert_eq!(o.get("a"), Some(&Value::RecordId(rid("person", "alice"))));
		assert_eq!(o.get("k"), Some(&Value::RecordId(rid("knows", "e1"))));
		assert_eq!(o.get("b"), Some(&Value::RecordId(rid("person", "bob"))));
	}

	#[test]
	fn null_filled_row_nulls_edge_and_target() {
		let input = obj_row(&[("a", Value::RecordId(rid("person", "alice")))]);
		let row = null_filled_row(&input, "k", "b");
		let Value::Object(o) = row else {
			panic!("expected object");
		};
		assert_eq!(o.get("a"), Some(&Value::RecordId(rid("person", "alice"))));
		assert_eq!(o.get("k"), Some(&Value::Null));
		assert_eq!(o.get("b"), Some(&Value::Null));
	}

	#[test]
	fn target_from_edge_obj_reads_correct_field() {
		let edge = obj_row(&[
			("in", Value::RecordId(rid("person", "alice"))),
			("out", Value::RecordId(rid("person", "bob"))),
		]);
		assert_eq!(target_from_edge_obj(&edge, ExpandDir::Out), Some(rid("person", "bob")));
		assert_eq!(target_from_edge_obj(&edge, ExpandDir::In), Some(rid("person", "alice")));
		// Missing endpoint field ⇒ None.
		let partial = obj_row(&[("in", Value::RecordId(rid("person", "alice")))]);
		assert_eq!(target_from_edge_obj(&partial, ExpandDir::Out), None);
		// Non-object ⇒ None.
		assert_eq!(target_from_edge_obj(&Value::Null, ExpandDir::Out), None);
	}
}
