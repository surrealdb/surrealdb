//! The `PathExpand` operator — variable-length / quantified graph traversal
//! for GQL v2 `MATCH`.
//!
//! `PathExpand` implements the quantified edge step of a lowered
//! [`MatchPlan`](crate::expr::match_plan::MatchPlan): patterns such as
//! `(a)-[:knows]->{1,3}(b)`, `(a)-[:knows]->+(b)`, or `(a)-[:knows]->*(b)`. For
//! every input binding row it walks the graph from a bound *source* node,
//! enumerating one output row **per path** (rule R6 of
//! `doc/gql/V2_DESIGN.md` §0) at every depth in `[min, max]` whose terminal
//! node passes the `target_label` filter.
//!
//! ## Binding rows produced
//!
//! Each emitted row is the input row (a `Value::Object` keyed by binding name,
//! per V2_DESIGN §3) extended with:
//! - `target_binding` → the full terminal node object (FieldState-fetched);
//! - `group_binding` (when set) → a [`Value::Array`] of the traversed edge objects in path order —
//!   `[]` for a zero-length path (R4);
//! - `path_binding` (when set) → the alternating `[n0, e1, n1, … , ed, nd]` array of full records
//!   (R5; a single-node path is `[n0]`).
//!
//! ## Traversal: depth-first, edge-unique within a path
//!
//! Per source row the operator runs an explicit-stack DFS over partial paths
//! `{ tip, edges, nodes }`. Extension enumerates the adjacency of `tip` via the
//! shared [`graph_keys`](super::super::scan::graph_keys) machinery (identical
//! ranges/decode to `GraphEdgeScan` and `Expand`), and:
//! - **skips any edge already present in the current path** — DIFFERENT-EDGES within a path. This
//!   is what makes the unbounded forms (`*`, `+`, `{n,}`) terminate on cyclic graphs: a finite edge
//!   set bounds the number of distinct simple-edge paths, so the DFS stack cannot grow without
//!   bound;
//! - FieldState-fetches the edge record (always — the edge is part of the group / path value and
//!   the uniqueness set) and the next node through [`resolve_with_field_state`]; a `None` (missing,
//!   or table/field permission-denied) **prunes that branch** (permission-prune semantics:
//!   intermediate nodes are always fetched, never short-circuited).
//!
//! The target vertex id for each hop is taken from the decoded pointer key
//! (`decoded.target`, present on new-format keys) and otherwise from the fetched
//! edge object's `out` (for `Out`) / `in` (for `In`) field — the same priority
//! the `Expand` operator uses.
//!
//! A node is emitted at depth `d` iff `min <= d <= max` (or `d >= min` when
//! `max` is `None`) and the node's table matches `target_label`. The branch
//! keeps extending while `d < max` regardless of whether `d` itself emitted, so
//! that e.g. `{2,4}` still reaches depth 4 through a depth-2 node that failed the
//! label filter.
//!
//! ### Zero-length path & label filtering (R6)
//!
//! When `min == 0` a zero-length path is emitted with `target = source`,
//! `group = []`, and `path = [source]` — but only if the *source node itself*
//! passes `target_label` (the source is the terminal node of a zero-hop path).
//! The same label predicate (the terminal node's table equals `target_label`)
//! gates emission at every depth `d >= 1`; the label never gates whether a
//! branch keeps extending, only whether the current tip is surfaced as a row.
//!
//! ### Why DFS, not BFS
//!
//! DFS bounds live memory to a single root-to-tip path plus the per-depth
//! frontier of unexplored siblings on the stack — `O(longest_path × fan-out)`.
//! A BFS would have to materialise the entire frontier of partial paths at each
//! depth, which for a quantified pattern over a dense graph is the full
//! cross-product of paths and defeats the streaming, bounded-memory goal. The
//! per-source path counter (`SURREAL_GQL_MAX_PATH_ROWS`) is the hard backstop
//! for pathological fan-out either way.
//!
//! ### Deterministic emission order
//!
//! Given a fixed KV adjacency order (the order `compute_graph_ranges` +
//! `decode_graph_edge` enumerate edges), the emission order is fully
//! deterministic: successors are pushed onto the DFS stack in reverse decoded
//! order so they are popped — and thus explored and emitted — in decoded order.
//! Within one source row this yields a pre-order DFS: a node at depth `d` is
//! emitted before the deeper paths that extend through it.

// The GQL v2 MATCH operators are constructed only by the gql-gated
// planner (`Expr::Match` is `#[cfg(feature = "gql")]`), so they are dead
// code when the feature is off — suppress the lint there only, keeping
// dead-code detection active in the default (gql-on) build.
#![cfg_attr(not(feature = "gql"), allow(dead_code))]

use std::sync::Arc;

use common::future::stream::{self, Yielder};
use futures::StreamExt;
use tracing::debug;

use super::expand::ExpandDir;
use crate::exec::operators::scan::fetch::{FetchFieldStateCache, resolve_with_field_state};
use crate::exec::operators::scan::graph_keys::{
	EdgeTableSpec, compute_graph_ranges, decode_graph_edge,
};
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, ControlFlowExt, ExecOperator, ExecutionContext,
	FlowResult, OperatorMetrics, ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::expr::{ControlFlow, Dir};
use crate::idx::planner::ScanDirection;
use crate::val::{Array, Object, RecordId, TableName, Value};

/// The storage-layer [`Dir`] scanned from a source vertex for an expand
/// direction. `ExpandDir`'s own mapping is private to `expand.rs`, so
/// `PathExpand` keeps its own copy of the two trivial mappings.
pub(crate) fn scan_dir(direction: ExpandDir) -> Dir {
	match direction {
		ExpandDir::Out => Dir::Out,
		ExpandDir::In => Dir::In,
	}
}

/// The edge-record field naming the far endpoint for an expand direction:
/// following `->` (Out) the target is the edge's `out` vertex; following `<-`
/// (In) it is the edge's `in` vertex.
pub(crate) fn target_field(direction: ExpandDir) -> &'static str {
	match direction {
		ExpandDir::Out => "out",
		ExpandDir::In => "in",
	}
}

/// The ISO path mode of a quantified traversal, as the operators execute it.
///
/// SurrealDB's MATCH mode is fixed to DIFFERENT EDGES (V2_DESIGN R2: no edge
/// record binds twice within a path), so edge-uniqueness is always enforced by
/// the adjacency scan regardless of mode — `Walk` and `Trail` are therefore
/// identical here. `Acyclic` additionally forbids any repeated node; `Simple`
/// forbids repeated nodes except a single close back onto the path's start.
/// Shared by [`PathExpand`] (DFS) and `ShortestPathExpand` (BFS).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathMode {
	Walk,
	Trail,
	Simple,
	Acyclic,
}

impl PathMode {
	/// The EXPLAIN label, or `None` for the default (`Walk`) which renders
	/// nothing — keeping plans without an explicit mode byte-identical.
	pub(crate) fn attr(self) -> Option<&'static str> {
		match self {
			PathMode::Walk => None,
			PathMode::Trail => Some("trail"),
			PathMode::Simple => Some("simple"),
			PathMode::Acyclic => Some("acyclic"),
		}
	}
}

/// What a candidate hop to a target node is allowed to do under a path mode,
/// once edge-uniqueness has already been satisfied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StepKind {
	/// The hop revisits a node it may not — drop the branch entirely.
	Reject,
	/// The hop may be emitted (a valid terminal) but the branch must not extend
	/// past it — a `Simple` path closing back onto its start.
	EmitOnly,
	/// The hop may be both emitted and extended.
	EmitAndExtend,
}

/// Decide whether a hop landing on `target` is admissible for `mode`, given the
/// nodes already on `path`. Edge-uniqueness is enforced elsewhere (the adjacency
/// scan skips edges already on the path), so this only governs node repetition.
pub(crate) fn step_kind(mode: PathMode, path_nodes: &[Value], target: &RecordId) -> StepKind {
	match mode {
		PathMode::Walk | PathMode::Trail => StepKind::EmitAndExtend,
		PathMode::Acyclic => {
			if nodes_contain(path_nodes, target) {
				StepKind::Reject
			} else {
				StepKind::EmitAndExtend
			}
		}
		PathMode::Simple => {
			let is_start = path_nodes.first().and_then(node_id).as_ref() == Some(target);
			if is_start {
				// A single close back onto the start is the one repeat SIMPLE
				// permits; the closed path is a valid terminal but cannot extend.
				StepKind::EmitOnly
			} else if nodes_contain(path_nodes, target) {
				StepKind::Reject
			} else {
				StepKind::EmitAndExtend
			}
		}
	}
}

/// Whether any node object in `nodes` has the record id `target`.
fn nodes_contain(nodes: &[Value], target: &RecordId) -> bool {
	nodes.iter().filter_map(node_id).any(|id| &id == target)
}

/// One partial path in the DFS: a tip vertex plus the edges and nodes traversed
/// to reach it. `nodes.len() == edges.len() + 1` always holds — every edge is
/// flanked by the node before it and the node after it, and `nodes[0]` is the
/// source.
pub(crate) struct PartialPath {
	/// The vertex the next extension expands from.
	pub(crate) tip: RecordId,
	/// `(edge id, edge object)` per hop, in path order. The id is kept alongside
	/// the fetched object so the edge-uniqueness check (and the group value)
	/// need not re-read it from the object.
	pub(crate) edges: Vec<(RecordId, Value)>,
	/// Full node objects in path order, including the source at index 0.
	pub(crate) nodes: Vec<Value>,
}

impl PartialPath {
	/// Depth = number of edges traversed.
	pub(crate) fn depth(&self) -> u32 {
		self.edges.len() as u32
	}

	/// `true` if `id` is already an edge of this path (DIFFERENT-EDGES).
	pub(crate) fn contains_edge(&self, id: &RecordId) -> bool {
		self.edges.iter().any(|(eid, _)| eid == id)
	}
}

/// Variable-length quantified expansion from a bound source node.
///
/// See the module documentation for the full semantics. Constructed by the GQL
/// streaming planner from the quantified `EdgeStep` of a `PatternPlan`.
#[derive(Debug, Clone)]
pub struct PathExpand {
	/// Child operator providing the input binding rows.
	pub(crate) input: Arc<dyn ExecOperator>,
	/// Name of the bound source-node binding in each input row.
	pub(crate) source: String,
	/// Direction of every hop in the quantified step.
	pub(crate) direction: ExpandDir,
	/// Edge table(s) the quantified step may traverse; empty ⇒ all edge tables
	/// in `direction`.
	pub(crate) edge_tables: Vec<TableName>,
	/// Minimum path length (number of edges). `0` enables the zero-length path.
	pub(crate) min: u32,
	/// Maximum path length, or `None` for unbounded (`*`, `+`, `{n,}`).
	pub(crate) max: Option<u32>,
	/// Binding name the terminal node is written under.
	pub(crate) target_binding: String,
	/// Optional table the terminal node must belong to (label filter).
	pub(crate) target_label: Option<TableName>,
	/// Binding name for the edge-group list (kind `EdgeGroup`), when present.
	pub(crate) group_binding: Option<String>,
	/// Binding name for the whole-path array (kind `Path`), when present.
	pub(crate) path_binding: Option<String>,
	/// The path mode (node/edge repetition discipline). `Walk`/`Trail` keep the
	/// default edge-unique traversal; `Simple`/`Acyclic` add node-uniqueness.
	pub(crate) mode: PathMode,
	/// `true` when the pattern was anchored on its far node, so the traversal runs
	/// the segment backwards (`source` is the pattern's *end*); the emitted group
	/// and path arrays are reversed to read in the pattern's written order.
	pub(crate) reversed: bool,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl PathExpand {
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		source: String,
		direction: ExpandDir,
		edge_tables: Vec<TableName>,
		min: u32,
		max: Option<u32>,
		target_binding: String,
		target_label: Option<TableName>,
		group_binding: Option<String>,
		path_binding: Option<String>,
		mode: PathMode,
		reversed: bool,
	) -> Self {
		Self {
			input,
			source,
			direction,
			edge_tables,
			min,
			max,
			target_binding,
			target_label,
			group_binding,
			path_binding,
			mode,
			reversed,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	/// Build the `EdgeTableSpec` list (unbounded ranges) for the configured edge
	/// tables. An empty list yields the all-edges wildcard scan. Mirrors
	/// `Expand::edge_specs`.
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

	/// `true` if depth `d` is within the emit window `[min, max]` (max unbounded
	/// when `None`).
	///
	/// The streaming `execute` body cannot borrow `self` inside its
	/// `async_stream`, so it rebuilds this as a local closure over the captured
	/// `min`/`max` (the production path); this method is the unit-tested form of
	/// the same window logic, hence `#[cfg(test)]`.
	#[cfg(test)]
	fn emits_at(&self, d: u32) -> bool {
		d >= self.min && self.max.is_none_or(|m| d <= m)
	}

	/// `true` if the DFS may extend a path that has already reached depth `d`.
	/// Unit-tested twin of `execute`'s local closure (see [`Self::emits_at`]).
	#[cfg(test)]
	fn extends_past(&self, d: u32) -> bool {
		self.max.is_none_or(|m| d < m)
	}
}

/// Extract the source `RecordId` from a binding row's source slot.
///
/// Thin wrapper over the shared [`crate::exec::operators::binding_record_id`]
/// (the slot holds a full node object whose `id` field is the record id —
/// V2_DESIGN §3 — or, defensively, a bare `RecordId`; anything else yields
/// `None` and the operator drops the row).
pub(crate) fn source_record_id(row: &Value, source: &str) -> Option<RecordId> {
	crate::exec::operators::binding_record_id(row, source)
}

/// Read the far-endpoint `RecordId` from a fetched edge object's `out`/`in`
/// field.
pub(crate) fn edge_target(edge_obj: &Value, field: &str) -> Option<RecordId> {
	let Value::Object(obj) = edge_obj else {
		return None;
	};
	match obj.get(field) {
		Some(Value::RecordId(rid)) => Some(rid.clone()),
		_ => None,
	}
}

/// The `id` `RecordId` of a node object, if present.
pub(crate) fn node_id(node_obj: &Value) -> Option<RecordId> {
	let Value::Object(obj) = node_obj else {
		return None;
	};
	match obj.get("id") {
		Some(Value::RecordId(rid)) => Some(rid.clone()),
		_ => None,
	}
}

/// Does this node object's table match the `target_label` filter? A `None`
/// filter always passes; a non-object never matches a label.
pub(crate) fn node_passes_label(node_obj: &Value, target_label: Option<&TableName>) -> bool {
	let Some(label) = target_label else {
		return true;
	};
	node_id(node_obj).map(|rid| &rid.table == label).unwrap_or(false)
}

impl ExecOperator for PathExpand {
	fn name(&self) -> &'static str {
		"PathExpand"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let dir = match self.direction {
			ExpandDir::Out => "->",
			ExpandDir::In => "<-",
		};
		let tables = if self.edge_tables.is_empty() {
			"*".to_string()
		} else {
			self.edge_tables.iter().map(|t| t.as_str()).collect::<Vec<_>>().join(", ")
		};
		let max = self.max.map(|m| m.to_string()).unwrap_or_else(|| "∞".to_string());
		let mut attrs = vec![
			("source".to_string(), self.source.clone()),
			("direction".to_string(), dir.to_string()),
			("tables".to_string(), tables),
			("min".to_string(), self.min.to_string()),
			("max".to_string(), max),
			("target_binding".to_string(), self.target_binding.clone()),
		];
		if let Some(label) = self.target_label.as_ref() {
			attrs.push(("target_label".to_string(), label.as_str().to_string()));
		}
		if let Some(group) = self.group_binding.as_ref() {
			attrs.push(("group".to_string(), group.clone()));
		}
		if let Some(path) = self.path_binding.as_ref() {
			attrs.push(("path".to_string(), path.clone()));
		}
		if let Some(mode) = self.mode.attr() {
			attrs.push(("mode".to_string(), mode.to_string()));
		}
		if self.reversed {
			attrs.push(("reversed".to_string(), "true".to_string()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		// Needs a transaction + catalog for adjacency scans and FieldState
		// fetches, combined with the child's requirement.
		self.input.required_context().max(ContextLevel::Database)
	}

	fn access_mode(&self) -> AccessMode {
		// Read-only itself (graph reads + record fetches); inherits the child's
		// mode. There are no embedded predicate exprs — the planner emits a
		// separate Filter above PathExpand for clause predicates.
		self.input.access_mode()
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		// One input row can fan out to many paths.
		CardinalityHint::Unbounded
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let db_ctx = ctx.database()?.clone();
		let mut input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);

		let source = self.source.clone();
		let direction = self.direction;
		let edge_specs = self.edge_specs();
		let min = self.min;
		let max = self.max;
		let target_binding = self.target_binding.clone();
		let target_label = self.target_label.clone();
		let group_binding = self.group_binding.clone();
		let path_binding = self.path_binding.clone();
		let mode = self.mode;
		let reversed = self.reversed;
		// Window predicates captured as closures so the stream body stays terse.
		let emits_at = move |d: u32| d >= min && max.is_none_or(|m| d <= m);
		let extends_past = move |d: u32| max.is_none_or(|m| d < m);
		let scan_batch_size = ctx.root().ctx.config.exec.scan_batch_size;
		let path_row_limit = ctx.root().ctx.config.exec.gql_max_path_rows;
		let dir = scan_dir(direction);
		let tgt_field = target_field(direction);
		let ctx = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			let txn = ctx.txn();
			let ns_id = db_ctx.ns_ctx.ns.namespace_id;
			let db_id = db_ctx.db.database_id;
			let version = ctx.version_stamp();

			// FieldState caches shared across every fetch this operator makes,
			// for every input row and batch — each table's state is built at
			// most once. Edges and nodes live in different tables, so separate
			// caches avoid churn (mirrors `Expand`'s edge/target split).
			let mut edge_cache = FetchFieldStateCache::new();
			let mut node_cache = FetchFieldStateCache::new();

			// Output batch, flushed when it reaches the configured batch size.
			let mut out: Vec<Value> = Vec::with_capacity(scan_batch_size);

			while let Some(batch_result) = input_stream.next().await {
				crate::exec::operators::check_cancelled(&ctx)?;
				let batch = batch_result?;
				for row in batch.values {
					// Drop rows whose source is absent / not a record id.
					let Some(source_rid) = source_record_id(&row, &source) else {
						debug!(
							target: "surrealdb::exec::path_expand",
							"PathExpand source binding {source} is not a bound node; dropping row",
						);
						continue;
					};

					// The source node object as it appears in the input row
					// (already permission-filtered upstream by the anchor scan).
					let source_obj = match &row {
						Value::Object(obj) => obj.get(&source).cloned().unwrap_or(Value::Null),
						_ => Value::Null,
					};

					// Per-source path budget: counts every path pushed on the
					// DFS stack plus every emitted path for this source row.
					let mut path_count: usize = 0;

					// --- Zero-length path (R6): target == source, [] group,
					// [source] path. Emitted iff min == 0 and the source node
					// itself passes the target-label filter.
					if min == 0 && node_passes_label(&source_obj, target_label.as_ref()) {
						path_count += 1;
						if path_count > path_row_limit {
							Err(path_rows_exceeded(path_row_limit))?;
						}
						let assembled = assemble_row(
							&row,
							&target_binding,
							source_obj.clone(),
							group_binding.as_deref(),
							&[],
							path_binding.as_deref(),
							std::slice::from_ref(&source_obj),
						);
						out.push(assembled);
						if out.len() >= scan_batch_size {
							yielder
								.emit(ValueBatch {
									values: std::mem::take(&mut out),
								})
								.await;
							out = Vec::with_capacity(scan_batch_size);
						}
					}

					// --- DFS over partial paths. The stack holds paths whose
					// tip is yet to be expanded; a path is only pushed when the
					// branch may still extend (depth < max).
					let mut stack: Vec<PartialPath> = Vec::new();
					if extends_past(0) {
						path_count += 1;
						if path_count > path_row_limit {
							Err(path_rows_exceeded(path_row_limit))?;
						}
						stack.push(PartialPath {
							tip: source_rid.clone(),
							edges: Vec::new(),
							nodes: vec![source_obj.clone()],
						});
					}

					while let Some(path) = stack.pop() {
						// The DFS can run very long (up to the per-source path
						// budget) emitting nothing; poll cancellation each step.
						crate::exec::operators::check_cancelled(&ctx)?;
						let depth = path.depth();

						// Enumerate the tip's adjacency for this direction,
						// collecting (edge id, decoded target) pairs and
						// skipping edges already used on this path
						// (DIFFERENT-EDGES). The cursor must be dropped before
						// the FieldState fetch can borrow `txn` again.
						let ranges =
							compute_graph_ranges(ns_id, db_id, &path.tip, dir, &edge_specs, &ctx)
								.await?;

						let mut edge_ids: Vec<RecordId> = Vec::new();
						let mut decoded_targets: Vec<Option<RecordId>> = Vec::new();
						for r in ranges {
							let mut cursor = txn
								.open_keys_cursor_raw(r, ScanDirection::Forward, 0, version)
								.await
								.context("Failed to open PathExpand graph cursor")?;
							loop {
								crate::exec::operators::check_cancelled(&ctx)?;
								let keys = cursor
									.next_batch(crate::kvs::NORMAL_BATCH_SIZE)
									.await
									.context("Failed to scan PathExpand graph edge")?;
								if keys.is_empty() {
									break;
								}
								for key in &keys {
									let decoded = decode_graph_edge(key)?;
									if !path.contains_edge(&decoded.edge) {
										edge_ids.push(decoded.edge);
										decoded_targets.push(decoded.target);
									}
								}
							}
							drop(cursor);
						}

						if edge_ids.is_empty() {
							continue;
						}

						// Batch FieldState-fetch every candidate edge (always —
						// the edge is part of the group/path value). A None
						// (missing / permission-denied) prunes that branch.
						let edge_objs =
							resolve_with_field_state(&ctx, &mut edge_cache, &edge_ids).await?;

						// Pair each surviving edge with its far-endpoint node id
						// (decoded target ⇒ fetched edge `out`/`in`), then
						// batch-fetch those nodes.
						let mut step_edges: Vec<(RecordId, Value)> = Vec::new();
						let mut node_ids: Vec<RecordId> = Vec::new();
						for ((edge_id, decoded_target), edge_obj) in
							edge_ids.into_iter().zip(decoded_targets).zip(edge_objs)
						{
							let Some(edge_obj) = edge_obj else {
								continue;
							};
							let next_id =
								decoded_target.or_else(|| edge_target(&edge_obj, tgt_field));
							let Some(next_id) = next_id else {
								// No usable endpoint ⇒ this edge can't extend the
								// path; prune the branch.
								continue;
							};
							step_edges.push((edge_id, edge_obj));
							node_ids.push(next_id);
						}

						if node_ids.is_empty() {
							continue;
						}

						let node_objs =
							resolve_with_field_state(&ctx, &mut node_cache, &node_ids).await?;

						// Build successors. Push them in reverse so the DFS pops
						// them in decoded (KV) order — deterministic pre-order
						// emission given a fixed adjacency order.
						let new_depth = depth + 1;
						let mut successors: Vec<PartialPath> = Vec::new();
						for ((edge_id, edge_obj), node_obj) in step_edges.into_iter().zip(node_objs)
						{
							let Some(node_obj) = node_obj else {
								// Permission-denied / missing next node prunes the
								// branch (permission-prune semantics).
								continue;
							};
							let target_id = node_id(&node_obj);

							// Path-mode admissibility (edge-uniqueness is already
							// enforced by the adjacency scan): drop a disallowed node
							// repeat, or allow a SIMPLE close that may emit but not
							// extend. A node without an id cannot be deduplicated or
							// extended from, so it is a leaf candidate (emit only).
							let kind = match target_id.as_ref() {
								Some(tid) => step_kind(mode, &path.nodes, tid),
								None => StepKind::EmitOnly,
							};
							if matches!(kind, StepKind::Reject) {
								continue;
							}

							// Extend the path by this hop.
							let mut edges = path.edges.clone();
							edges.push((edge_id, edge_obj));
							let mut nodes = path.nodes.clone();
							nodes.push(node_obj.clone());

							// Emit at this depth when in window and the terminal node
							// passes the label filter.
							if emits_at(new_depth)
								&& node_passes_label(&node_obj, target_label.as_ref())
							{
								path_count += 1;
								if path_count > path_row_limit {
									Err(path_rows_exceeded(path_row_limit))?;
								}
								let (group, path_arr) =
									build_group_and_path(&nodes, &edges, reversed);
								let assembled = assemble_row(
									&row,
									&target_binding,
									node_obj.clone(),
									group_binding.as_deref(),
									&group,
									path_binding.as_deref(),
									&path_arr,
								);
								out.push(assembled);
								if out.len() >= scan_batch_size {
									yielder
										.emit(ValueBatch {
											values: std::mem::take(&mut out),
										})
										.await;
									out = Vec::with_capacity(scan_batch_size);
								}
							}

							// Keep extending only when the mode permits it (a SIMPLE
							// close does not) and another hop is allowed.
							if matches!(kind, StepKind::EmitAndExtend)
								&& extends_past(new_depth)
								&& let Some(tip) = target_id
							{
								path_count += 1;
								if path_count > path_row_limit {
									Err(path_rows_exceeded(path_row_limit))?;
								}
								successors.push(PartialPath {
									tip,
									edges,
									nodes,
								});
							}
						}

						for succ in successors.into_iter().rev() {
							stack.push(succ);
						}
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
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "PathExpand", &self.metrics))
	}
}

/// The `SURREAL_GQL_MAX_PATH_ROWS` guard error, naming the knob.
pub(crate) fn path_rows_exceeded(limit: usize) -> ControlFlow {
	ControlFlow::Err(anyhow::anyhow!(
		"GQL MATCH path traversal exceeded SURREAL_GQL_MAX_PATH_ROWS ({limit}); the quantified \
		 pattern produced too many paths from a single source row"
	))
}

/// Build the R5 alternating path array `[n0, e1, n1, … , ed, nd]`.
///
/// `nodes.len() == edges.len() + 1`. A single-node path is `[n0]`.
pub(crate) fn build_path_array(nodes: &[Value], edges: &[(RecordId, Value)]) -> Vec<Value> {
	let mut arr = Vec::with_capacity(nodes.len() + edges.len());
	for (i, node) in nodes.iter().enumerate() {
		arr.push(node.clone());
		if let Some((_, edge_obj)) = edges.get(i) {
			arr.push(edge_obj.clone());
		}
	}
	arr
}

/// Build the edge-group list (R4) and the R5 path array for an emitted path,
/// both in traversal order (`source → terminal`).
///
/// When `reversed` the pattern was anchored on its far node and the traversal
/// ran the segment backwards (`source` is the pattern's *end*), so the arrays
/// are reversed to read in the pattern's written order (`start → end`).
/// Reversing the alternating `[n0, e1, … , nd]` array yields the
/// correctly-alternating `[nd, ed, … , n0]`; a single-node / empty group is a
/// no-op.
pub(crate) fn build_group_and_path(
	nodes: &[Value],
	edges: &[(RecordId, Value)],
	reversed: bool,
) -> (Vec<Value>, Vec<Value>) {
	let mut group: Vec<Value> = edges.iter().map(|(_, o)| o.clone()).collect();
	let mut path_arr = build_path_array(nodes, edges);
	if reversed {
		group.reverse();
		path_arr.reverse();
	}
	(group, path_arr)
}

/// Assemble an output binding row: the input row extended with the target node
/// and the optional group / path bindings.
pub(crate) fn assemble_row(
	input_row: &Value,
	target_binding: &str,
	target_obj: Value,
	group_binding: Option<&str>,
	group: &[Value],
	path_binding: Option<&str>,
	path_arr: &[Value],
) -> Value {
	let mut obj: Object = match input_row {
		Value::Object(o) => o.clone(),
		// The binding-row convention guarantees objects; an unexpected shape
		// degrades to an empty row rather than panicking.
		_ => Object::default(),
	};
	obj.insert(target_binding.to_string(), target_obj);
	if let Some(group_name) = group_binding {
		obj.insert(group_name.to_string(), Value::Array(Array::from(group.to_vec())));
	}
	if let Some(path_name) = path_binding {
		obj.insert(path_name.to_string(), Value::Array(Array::from(path_arr.to_vec())));
	}
	Value::Object(obj)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::CurrentValueSource;
	use crate::val::RecordIdKey;

	fn rid(table: &str, key: &str) -> RecordId {
		RecordId {
			table: TableName::new(table.to_string()),
			key: RecordIdKey::from(key.to_string()),
		}
	}

	fn node(table: &str, key: &str) -> Value {
		let mut obj = Object::default();
		obj.insert("id", Value::RecordId(rid(table, key)));
		Value::Object(obj)
	}

	/// `table:key` ⇒ `RecordId`, for the `in`/`out` endpoints written
	/// `"person:alice"` in test fixtures.
	fn rid_str(s: &str) -> RecordId {
		let (table, key) = s.split_once(':').expect("test record id missing ':'");
		rid(table, key)
	}

	fn edge(table: &str, key: &str, in_node: &str, out_node: &str) -> Value {
		let mut obj = Object::default();
		obj.insert("id", Value::RecordId(rid(table, key)));
		obj.insert("in", Value::RecordId(rid_str(in_node)));
		obj.insert("out", Value::RecordId(rid_str(out_node)));
		Value::Object(obj)
	}

	fn edge_id(table: &str, key: &str) -> RecordId {
		rid(table, key)
	}

	fn sample(min: u32, max: Option<u32>) -> PathExpand {
		PathExpand::new(
			Arc::new(CurrentValueSource::new()),
			"a".to_string(),
			ExpandDir::Out,
			vec![TableName::new("knows")],
			min,
			max,
			"b".to_string(),
			None,
			None,
			None,
			PathMode::Walk,
			false,
		)
	}

	#[test]
	fn source_id_from_full_object_row() {
		let mut row = Object::default();
		row.insert("a", node("person", "alice"));
		let rid = source_record_id(&Value::Object(row), "a").unwrap();
		assert_eq!(rid.table, TableName::new("person"));
	}

	#[test]
	fn source_id_missing_or_null_drops() {
		// Missing binding.
		assert!(source_record_id(&Value::Object(Object::default()), "a").is_none());
		// Null slot (optional miss; PR-C interplay).
		let mut row = Object::default();
		row.insert("a", Value::Null);
		assert!(source_record_id(&Value::Object(row), "a").is_none());
		// Non-object row.
		assert!(source_record_id(&Value::None, "a").is_none());
	}

	#[test]
	fn direction_maps_dir_and_field() {
		assert_eq!(target_field(ExpandDir::Out), "out");
		assert_eq!(target_field(ExpandDir::In), "in");
		assert_eq!(scan_dir(ExpandDir::Out), Dir::Out);
		assert_eq!(scan_dir(ExpandDir::In), Dir::In);
	}

	#[test]
	fn edge_target_reads_endpoint() {
		let e = edge("knows", "1", "person:a", "person:b");
		assert_eq!(edge_target(&e, "out"), Some(edge_id("person", "b")));
		assert_eq!(edge_target(&e, "in"), Some(edge_id("person", "a")));
	}

	#[test]
	fn label_filter_matches_table() {
		let n = node("person", "alice");
		assert!(node_passes_label(&n, Some(&TableName::new("person"))));
		assert!(!node_passes_label(&n, Some(&TableName::new("company"))));
		// No label always passes.
		assert!(node_passes_label(&n, None));
		// Non-object never passes a label.
		assert!(!node_passes_label(&Value::None, Some(&TableName::new("person"))));
	}

	#[test]
	fn build_path_array_alternates() {
		let nodes = vec![node("person", "a"), node("person", "b"), node("person", "c")];
		let edges = vec![
			(edge_id("knows", "1"), edge("knows", "1", "person:a", "person:b")),
			(edge_id("knows", "2"), edge("knows", "2", "person:b", "person:c")),
		];
		let arr = build_path_array(&nodes, &edges);
		// [n0, e1, n1, e2, n2] => 5 elements, node/edge alternating.
		assert_eq!(arr.len(), 5);
		assert!(matches!(&arr[0], Value::Object(_)));
		assert!(matches!(&arr[1], Value::Object(_)));

		// Single-node path => [n0].
		assert_eq!(build_path_array(&nodes[..1], &[]).len(), 1);
	}

	#[test]
	fn assemble_row_carries_input_and_bindings() {
		let mut input = Object::default();
		input.insert("a", node("person", "a"));
		let input = Value::Object(input);

		let group = vec![edge("knows", "1", "person:a", "person:b")];
		let path = vec![node("person", "a"), group[0].clone(), node("person", "b")];
		let row =
			assemble_row(&input, "b", node("person", "b"), Some("g"), &group, Some("p"), &path);
		let Value::Object(obj) = row else {
			panic!("expected object row");
		};
		assert!(obj.contains_key("a")); // input binding preserved
		assert!(obj.contains_key("b")); // target node bound
		match obj.get("g") {
			Some(Value::Array(a)) => assert_eq!(a.len(), 1),
			other => panic!("expected group array, got {other:?}"),
		}
		match obj.get("p") {
			Some(Value::Array(a)) => assert_eq!(a.len(), 3),
			other => panic!("expected path array, got {other:?}"),
		}
	}

	#[test]
	fn zero_length_group_is_empty_array() {
		let input = {
			let mut o = Object::default();
			o.insert("a", node("person", "a"));
			Value::Object(o)
		};
		let src = node("person", "a");
		let row = assemble_row(&input, "a", src.clone(), Some("g"), &[], Some("p"), &[src]);
		let Value::Object(obj) = row else {
			panic!("expected object row");
		};
		match obj.get("g") {
			Some(Value::Array(a)) => assert!(a.is_empty()),
			other => panic!("expected empty group array, got {other:?}"),
		}
		match obj.get("p") {
			Some(Value::Array(a)) => assert_eq!(a.len(), 1),
			other => panic!("expected single-node path, got {other:?}"),
		}
	}

	#[test]
	fn emit_and_extend_windows_bounded() {
		// {1,3}: depth 0 doesn't emit but extends; 1..=3 emit; depth 3 doesn't
		// extend further.
		let pe = sample(1, Some(3));
		assert!(!pe.emits_at(0));
		assert!(pe.emits_at(1));
		assert!(pe.emits_at(3));
		assert!(!pe.emits_at(4));
		assert!(pe.extends_past(0));
		assert!(pe.extends_past(2));
		assert!(!pe.extends_past(3));
	}

	#[test]
	fn unbounded_max_always_extends() {
		let pe = sample(0, None);
		assert!(pe.emits_at(0));
		assert!(pe.emits_at(100));
		assert!(pe.extends_past(0));
		assert!(pe.extends_past(1000));
	}

	#[test]
	fn exact_quantifier_window() {
		// {2}: only depth 2 emits; extends through depth 0 and 1, stops at 2.
		let pe = sample(2, Some(2));
		assert!(!pe.emits_at(1));
		assert!(pe.emits_at(2));
		assert!(!pe.emits_at(3));
		assert!(pe.extends_past(1));
		assert!(!pe.extends_past(2));
	}

	#[test]
	fn attrs_render_min_max_group_path() {
		let pe = PathExpand::new(
			Arc::new(CurrentValueSource::new()),
			"a".to_string(),
			ExpandDir::Out,
			vec![TableName::new("knows")],
			1,
			None,
			"b".to_string(),
			Some(TableName::new("person")),
			Some("g".to_string()),
			Some("p".to_string()),
			PathMode::Trail,
			false,
		);
		assert_eq!(pe.name(), "PathExpand");
		let attrs = pe.attrs();
		assert!(attrs.iter().any(|(k, v)| k == "min" && v == "1"));
		// Unbounded max renders as ∞.
		assert!(attrs.iter().any(|(k, v)| k == "max" && v == "∞"));
		assert!(attrs.iter().any(|(k, v)| k == "tables" && v.contains("knows")));
		assert!(attrs.iter().any(|(k, v)| k == "group" && v == "g"));
		assert!(attrs.iter().any(|(k, v)| k == "path" && v == "p"));
		assert!(attrs.iter().any(|(k, v)| k == "target_label" && v == "person"));
		assert!(attrs.iter().any(|(k, v)| k == "mode" && v == "trail"));
	}

	#[test]
	fn step_kind_enforces_path_modes() {
		// nodes a -> b; consider a hop landing on various targets.
		let nodes = vec![node("hub", "a"), node("hub", "b")];
		let start = rid("hub", "a");
		let other = rid("hub", "c");
		let mid = rid("hub", "b");
		// WALK/TRAIL: every hop extends (edge-uniqueness handles termination).
		assert_eq!(step_kind(PathMode::Walk, &nodes, &start), StepKind::EmitAndExtend);
		assert_eq!(step_kind(PathMode::Trail, &nodes, &mid), StepKind::EmitAndExtend);
		// ACYCLIC: any revisited node (including the start) is rejected.
		assert_eq!(step_kind(PathMode::Acyclic, &nodes, &start), StepKind::Reject);
		assert_eq!(step_kind(PathMode::Acyclic, &nodes, &mid), StepKind::Reject);
		assert_eq!(step_kind(PathMode::Acyclic, &nodes, &other), StepKind::EmitAndExtend);
		// SIMPLE: only a close back onto the start is allowed (emit, don't extend).
		assert_eq!(step_kind(PathMode::Simple, &nodes, &start), StepKind::EmitOnly);
		assert_eq!(step_kind(PathMode::Simple, &nodes, &mid), StepKind::Reject);
		assert_eq!(step_kind(PathMode::Simple, &nodes, &other), StepKind::EmitAndExtend);
	}

	#[test]
	fn edge_specs_unbounded_per_table() {
		let pe = sample(1, Some(2));
		let specs = pe.edge_specs();
		assert_eq!(specs.len(), 1);
		assert_eq!(specs[0].table, TableName::new("knows"));
		assert!(matches!(specs[0].range_start, std::ops::Bound::Unbounded));
	}

	#[test]
	fn partial_path_edge_uniqueness() {
		let path = PartialPath {
			tip: edge_id("person", "b"),
			edges: vec![(edge_id("knows", "1"), edge("knows", "1", "person:a", "person:b"))],
			nodes: vec![node("person", "a"), node("person", "b")],
		};
		assert_eq!(path.depth(), 1);
		assert!(path.contains_edge(&edge_id("knows", "1")));
		assert!(!path.contains_edge(&edge_id("knows", "2")));
	}
}
