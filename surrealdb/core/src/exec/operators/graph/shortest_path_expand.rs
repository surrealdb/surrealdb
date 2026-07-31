//! The `ShortestPathExpand` operator — shortest-path search for GQL v2
//! `MATCH` (the `ALL SHORTEST` / `ANY SHORTEST` / `SHORTEST k` / `SHORTEST k
//! GROUP(S)` path-search prefixes).
//!
//! Like [`PathExpand`](super::path_expand::PathExpand) it expands a single
//! quantified segment from a bound *source* node, per input binding row, and
//! emits one row per selected path with the terminal node, the edge group (R4)
//! and the alternating path array (R5). It differs in *which* paths it keeps:
//! the planner routes a pattern carrying a `SHORTEST` (or `ANY`) selector here.
//! Grouping is per-source-row by terminal node id, which equals the GQL endpoint
//! partition `(start, end)`. A pattern bound on its far node expands backwards
//! (`source` is the pattern's end); grouping and path length are symmetric so
//! selection is unaffected, and `reversed` flips the emitted path/group back to
//! the pattern's written order.
//!
//! ## Algorithm: BFS over partial paths
//!
//! Per source row the operator runs a breadth-first traversal whose frontier is
//! a FIFO queue of partial paths `{ tip, edges, nodes }` (the same struct
//! `PathExpand` uses). Because successors are enqueued at the back, paths are
//! processed in non-decreasing depth (hop count), so a terminal is *reached* in
//! non-decreasing length order — the property every selector relies on.
//! Edge-uniqueness within a path (DIFFERENT EDGES, R2) is enforced by the
//! adjacency scan exactly as in `PathExpand`, which also bounds the queue on
//! cyclic graphs; the optional path mode adds node-uniqueness
//! ([`step_kind`]). Shortest is by **hop count** (unweighted).
//!
//! Per distinct terminal node id (the GQL endpoint), a [`TerminalSelector`]
//! decides emission:
//! - **ANY SHORTEST** — the first in-window arrival; the rest suppressed.
//! - **ALL SHORTEST** — every arrival at the minimum in-window depth.
//! - **SHORTEST k** — the first `k` arrivals (i.e. the `k` shortest, ties in KV order).
//! - **SHORTEST k GROUP(S)** — every arrival whose length is among the `k` smallest distinct
//!   lengths.
//!
//! ## Extension pruning & termination
//!
//! With `min <= 1` (the common case, where the in-window shortest equals the
//! global shortest) every selector caps how many times each node is extended,
//! keeping the frontier O(k·E) instead of enumerating all paths:
//! - `ANY SHORTEST` / `ALL SHORTEST` are the `k == 1` cases — visit each node once (count cap 1),
//!   resp. extend every arrival at its minimum depth (depth cap 1, the shortest-path DAG). Exact:
//!   shortest paths are simple, so the cap never drops one. Cheap even on unbounded `*`.
//! - `SHORTEST k` extends each node from at most `k` paths; `SHORTEST k GROUP(S)` extends each node
//!   across its `k` smallest distinct depths (all ties). This is the standard per-node k-shortest
//!   relaxation — exact whenever the k-shortest paths are simple (the common case), and otherwise a
//!   sound bound on work (it never *adds* spurious paths, only bounds exploration).
//! - `ANY [k]` reuses the counted cap and is **always** exact: returning `k` valid paths per
//!   terminal is a valid arbitrary answer.
//!
//! With `min > 1` the in-window shortest can sit above a node's globally
//! smallest depths, so the caps are disabled (full enumeration); termination
//! then rests on edge-uniqueness (each path has at most `|E|` edges) and the
//! per-source `SURREAL_GQL_MAX_PATH_ROWS` budget, which counts every frontier
//! push and every emit.

#![cfg_attr(not(feature = "gql"), allow(dead_code))]

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use common::future::stream::{self, Yielder};
use futures::StreamExt;
use tracing::debug;

use super::expand::ExpandDir;
use super::path_expand::{
	PartialPath, PathMode, StepKind, assemble_row, build_group_and_path, edge_target, node_id,
	node_passes_label, path_rows_exceeded, scan_dir, source_record_id, step_kind, target_field,
};
use crate::exec::operators::scan::fetch::{FetchFieldStateCache, resolve_with_field_state};
use crate::exec::operators::scan::graph_keys::{
	EdgeTableSpec, compute_graph_ranges, decode_graph_edge,
};
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, ControlFlowExt, ExecOperator, ExecutionContext,
	FlowResult, OperatorMetrics, ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::idx::planner::ScanDirection;
use crate::val::{RecordId, TableName, Value};

/// Which of the paths to each endpoint a path search keeps. Named for the
/// shortest family (its main job), it also serves the non-shortest `ANY [k]`
/// selector ([`ShortestSelector::Any`]) — a shortest representative is a valid
/// arbitrary path, so `ANY` reuses the same cheap visit-once / `k`-bounded
/// traversal rather than the full `PathExpand` DFS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShortestSelector {
	/// `k` arbitrary paths per endpoint (`ANY [k]`); implemented as the `k`
	/// shortest (a valid arbitrary choice) for O(k·E) cost.
	Any {
		count: u32,
	},
	/// One minimum-length path per endpoint (`ANY SHORTEST`).
	AnyShortest,
	/// Every minimum-length path per endpoint (`ALL SHORTEST`).
	AllShortest,
	/// The `k` shortest paths per endpoint (`SHORTEST k`).
	Counted(u32),
	/// Every path in the `k` smallest distinct lengths per endpoint
	/// (`SHORTEST [k] GROUP(S)`).
	CountedGroups(u32),
}

impl ShortestSelector {
	/// The EXPLAIN label for this selector.
	fn attr(self) -> String {
		match self {
			ShortestSelector::Any {
				count: 1,
			} => "any".to_string(),
			ShortestSelector::Any {
				count,
			} => format!("any {count}"),
			ShortestSelector::AnyShortest => "any shortest".to_string(),
			ShortestSelector::AllShortest => "all shortest".to_string(),
			ShortestSelector::Counted(k) => format!("shortest {k}"),
			ShortestSelector::CountedGroups(k) => format!("shortest {k} groups"),
		}
	}
}

/// Per-source-row emission/extension bookkeeping for a [`ShortestSelector`].
///
/// All state is keyed by record id. The emission maps decide which arrivals at a
/// terminal are surfaced; the extension maps prune the frontier for the
/// shortest selectors (see the module docs).
struct TerminalSelector {
	selector: ShortestSelector,
	/// Whether to prune the frontier with per-node extension caps. Sound only
	/// when the window starts at the natural shortest (`min <= 1`); for `min > 1`
	/// the in-window shortest may sit above a node's globally-smallest depths, so
	/// pruning could miss in-window terminals and the search falls back to full
	/// enumeration.
	prune: bool,
	/// `ANY SHORTEST`: terminals already emitted.
	any_emitted: HashSet<RecordId>,
	/// `ALL SHORTEST`: the minimum in-window depth emitted per terminal.
	best_emit_depth: HashMap<RecordId, u32>,
	/// `ANY [k]` / `SHORTEST k`: paths emitted per terminal.
	counts: HashMap<RecordId, u32>,
	/// `SHORTEST k GROUP(S)`: `(distinct lengths emitted, last length)` per
	/// terminal. `u32::MAX` marks "no length yet".
	groups: HashMap<RecordId, (u32, u32)>,
	/// Count-based extension cap: paths already extended from each node (drives
	/// the visit-once `ANY SHORTEST` and the `≤ k` `ANY [k]` / `SHORTEST k` bound).
	extend_count: HashMap<RecordId, u32>,
	/// Depth-based extension cap: `(distinct depths extended, last depth)` per
	/// node (drives the min-depth-DAG `ALL SHORTEST` and the `≤ k` smallest
	/// distinct depths `SHORTEST k GROUP(S)` bound).
	extend_groups: HashMap<RecordId, (u32, u32)>,
}

impl TerminalSelector {
	fn new(selector: ShortestSelector, min: u32) -> Self {
		Self {
			selector,
			prune: min <= 1,
			any_emitted: HashSet::new(),
			best_emit_depth: HashMap::new(),
			counts: HashMap::new(),
			groups: HashMap::new(),
			extend_count: HashMap::new(),
			extend_groups: HashMap::new(),
		}
	}

	/// The per-node extension cap for this selector: how many paths (count-based)
	/// or how many distinct depths (depth-based) each node may be extended from.
	/// `ANY SHORTEST`/`ALL SHORTEST` are the `k == 1` cases.
	fn extend_cap(&self) -> u32 {
		match self.selector {
			ShortestSelector::AnyShortest | ShortestSelector::AllShortest => 1,
			ShortestSelector::Any {
				count: k,
			}
			| ShortestSelector::Counted(k)
			| ShortestSelector::CountedGroups(k) => k,
		}
	}

	/// Whether this selector caps extension by distinct *depth* (the all-shortest
	/// / group family) rather than by path *count* (the any / counted family).
	fn depth_based(&self) -> bool {
		matches!(self.selector, ShortestSelector::AllShortest | ShortestSelector::CountedGroups(_))
	}

	/// Record the source as already extended at depth 0 so a later cycle back
	/// onto it is bounded by the cap (only when pruning is active).
	fn mark_source(&mut self, source: &RecordId) {
		if !self.prune {
			return;
		}
		if self.depth_based() {
			self.extend_groups.insert(source.clone(), (1, 0));
		} else {
			self.extend_count.insert(source.clone(), 1);
		}
	}

	/// Whether an in-window arrival at `terminal` at `depth` should be emitted,
	/// recording it.
	fn should_emit(&mut self, terminal: &RecordId, depth: u32) -> bool {
		match self.selector {
			ShortestSelector::AnyShortest => self.any_emitted.insert(terminal.clone()),
			ShortestSelector::AllShortest => match self.best_emit_depth.get(terminal).copied() {
				None => {
					self.best_emit_depth.insert(terminal.clone(), depth);
					true
				}
				Some(best) => best == depth,
			},
			ShortestSelector::Counted(k)
			| ShortestSelector::Any {
				count: k,
			} => {
				let count = self.counts.entry(terminal.clone()).or_insert(0);
				if *count < k {
					*count += 1;
					true
				} else {
					false
				}
			}
			ShortestSelector::CountedGroups(k) => {
				let entry = self.groups.entry(terminal.clone()).or_insert((0, u32::MAX));
				let (groups_seen, last_len) = *entry;
				if last_len == depth {
					// Another path of a length already counted for this terminal.
					true
				} else if groups_seen < k {
					*entry = (groups_seen + 1, depth);
					true
				} else {
					false
				}
			}
		}
	}

	/// Whether a path reaching `tip` at `depth` should be extended (enqueued).
	///
	/// With pruning active (`min <= 1`), each node is extended from at most `k`
	/// paths (count-based selectors) or across its `k` smallest distinct depths
	/// (depth-based selectors). For `k == 1` this is the exact visit-once /
	/// shortest-path-DAG traversal (shortest paths are simple, so the bound never
	/// drops one). For `k > 1` it is the standard per-node k-shortest relaxation:
	/// exact on graphs whose k-shortest paths are simple (the common case) and a
	/// sound upper bound on work otherwise. For an arbitrary-path `ANY [k]` it is
	/// always exact (k valid paths per terminal is a valid answer). Without
	/// pruning (`min > 1`) every path is kept; edge-uniqueness + the per-source
	/// row budget terminate the search.
	fn should_extend(&mut self, tip: &RecordId, depth: u32) -> bool {
		if !self.prune {
			return true;
		}
		let cap = self.extend_cap();
		if self.depth_based() {
			let entry = self.extend_groups.entry(tip.clone()).or_insert((0, u32::MAX));
			let (seen, last) = *entry;
			if last == depth {
				// Another sub-path at a depth already opened — needed for the ties.
				true
			} else if seen < cap {
				*entry = (seen + 1, depth);
				true
			} else {
				false
			}
		} else {
			let count = self.extend_count.entry(tip.clone()).or_insert(0);
			if *count < cap {
				*count += 1;
				true
			} else {
				false
			}
		}
	}
}

/// Shortest-path variable-length expansion from a bound source node.
///
/// See the module documentation for the full semantics. Constructed by the GQL
/// streaming planner from a quantified `EdgeStep` whose pattern carries a
/// `SHORTEST` path-search selector.
#[derive(Debug, Clone)]
pub struct ShortestPathExpand {
	/// Child operator providing the input binding rows.
	pub(crate) input: Arc<dyn ExecOperator>,
	/// Name of the bound source-node binding in each input row.
	pub(crate) source: String,
	/// Direction of every hop in the quantified step.
	pub(crate) direction: ExpandDir,
	/// Edge table(s) the step may traverse; empty ⇒ all edge tables in `direction`.
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
	/// The path mode (node/edge repetition discipline).
	pub(crate) mode: PathMode,
	/// Which paths to each endpoint survive.
	pub(crate) selector: ShortestSelector,
	/// `true` when the pattern was anchored on its far node, so the traversal
	/// runs the segment backwards (`source` is the pattern's *end*); the emitted
	/// group and path arrays are reversed to read in the pattern's written order.
	/// Path length is symmetric, so the shortest selection is unaffected.
	pub(crate) reversed: bool,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl ShortestPathExpand {
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
		selector: ShortestSelector,
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
			selector,
			reversed,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	/// Build the `EdgeTableSpec` list (unbounded ranges) for the configured edge
	/// tables. Mirrors `PathExpand::edge_specs`.
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

impl ExecOperator for ShortestPathExpand {
	fn name(&self) -> &'static str {
		"ShortestPathExpand"
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
			("search".to_string(), self.selector.attr()),
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
		self.input.required_context().max(ContextLevel::Database)
	}

	fn access_mode(&self) -> AccessMode {
		self.input.access_mode()
	}

	fn cardinality_hint(&self) -> CardinalityHint {
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
		let selector = self.selector;
		let reversed = self.reversed;
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

			let mut edge_cache = FetchFieldStateCache::new();
			let mut node_cache = FetchFieldStateCache::new();

			let mut out: Vec<Value> = Vec::with_capacity(scan_batch_size);

			while let Some(batch_result) = input_stream.next().await {
				crate::exec::operators::check_cancelled(&ctx)?;
				let batch = batch_result?;
				for row in batch.values {
					let Some(source_rid) = source_record_id(&row, &source) else {
						debug!(
							target: "surrealdb::exec::shortest_path_expand",
							"ShortestPathExpand source binding {source} is not a bound node; dropping row",
						);
						continue;
					};
					let source_obj = match &row {
						Value::Object(obj) => obj.get(&source).cloned().unwrap_or(Value::Null),
						_ => Value::Null,
					};

					// Per-source budget (frontier pushes + emits) and selector state.
					let mut path_count: usize = 0;
					let mut state = TerminalSelector::new(selector, min);
					state.mark_source(&source_rid);

					// --- Zero-length path (R6): the source is its own terminal at
					// depth 0. Emitted iff min == 0, the source passes the label,
					// and the selector admits it.
					if min == 0
						&& node_passes_label(&source_obj, target_label.as_ref())
						&& state.should_emit(&source_rid, 0)
					{
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

					// --- BFS frontier (FIFO ⇒ non-decreasing depth).
					let mut queue: VecDeque<PartialPath> = VecDeque::new();
					if extends_past(0) {
						path_count += 1;
						if path_count > path_row_limit {
							Err(path_rows_exceeded(path_row_limit))?;
						}
						queue.push_back(PartialPath {
							tip: source_rid.clone(),
							edges: Vec::new(),
							nodes: vec![source_obj.clone()],
						});
					}

					while let Some(path) = queue.pop_front() {
						crate::exec::operators::check_cancelled(&ctx)?;
						let depth = path.depth();

						// Enumerate the tip's adjacency, skipping edges already on
						// the path (DIFFERENT EDGES). Identical to `PathExpand`.
						let ranges =
							compute_graph_ranges(ns_id, db_id, &path.tip, dir, &edge_specs, &ctx)
								.await?;

						let mut edge_ids: Vec<RecordId> = Vec::new();
						let mut decoded_targets: Vec<Option<RecordId>> = Vec::new();
						for r in ranges {
							let mut cursor = txn
								.open_keys_cursor_raw(r, ScanDirection::Forward, 0, version)
								.await
								.context("Failed to open ShortestPathExpand graph cursor")?;
							loop {
								crate::exec::operators::check_cancelled(&ctx)?;
								let keys =
									cursor
										.next_batch(crate::kvs::NORMAL_BATCH_SIZE)
										.await
										.context("Failed to scan ShortestPathExpand graph edge")?;
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

						let edge_objs =
							resolve_with_field_state(&ctx, &mut edge_cache, &edge_ids).await?;

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

						let new_depth = depth + 1;
						for ((edge_id, edge_obj), node_obj) in step_edges.into_iter().zip(node_objs)
						{
							let Some(node_obj) = node_obj else {
								continue;
							};
							let Some(target_id) = node_id(&node_obj) else {
								// A node without an id cannot be an endpoint or be
								// deduplicated; skip it.
								continue;
							};

							let kind = step_kind(mode, &path.nodes, &target_id);
							if matches!(kind, StepKind::Reject) {
								continue;
							}

							let mut edges = path.edges.clone();
							edges.push((edge_id, edge_obj));
							let mut nodes = path.nodes.clone();
							nodes.push(node_obj.clone());

							// Emit when in window, label-passing, and the selector
							// admits this arrival at `target_id`.
							if emits_at(new_depth)
								&& node_passes_label(&node_obj, target_label.as_ref())
								&& state.should_emit(&target_id, new_depth)
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

							// Extend when the mode permits (a SIMPLE close does not)
							// another hop is allowed, and the selector's frontier
							// pruning admits it.
							if matches!(kind, StepKind::EmitAndExtend)
								&& extends_past(new_depth)
								&& state.should_extend(&target_id, new_depth)
							{
								path_count += 1;
								if path_count > path_row_limit {
									Err(path_rows_exceeded(path_row_limit))?;
								}
								queue.push_back(PartialPath {
									tip: target_id,
									edges,
									nodes,
								});
							}
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

		Ok(monitor_stream(Box::pin(stream), "ShortestPathExpand", &self.metrics))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn rid(key: &str) -> RecordId {
		RecordId {
			table: TableName::new("hub".to_string()),
			key: crate::val::RecordIdKey::from(key.to_string()),
		}
	}

	#[test]
	fn any_shortest_emits_first_and_visits_once() {
		let mut s = TerminalSelector::new(ShortestSelector::AnyShortest, 1);
		let t = rid("t");
		assert!(s.should_emit(&t, 2)); // first arrival emits
		assert!(!s.should_emit(&t, 2)); // second suppressed
		assert!(!s.should_emit(&t, 3));
		// Extension: each node visited once.
		let x = rid("x");
		assert!(s.should_extend(&x, 1));
		assert!(!s.should_extend(&x, 1));
		assert!(!s.should_extend(&x, 2));
	}

	#[test]
	fn all_shortest_emits_min_depth_ties_only() {
		let mut s = TerminalSelector::new(ShortestSelector::AllShortest, 1);
		let t = rid("t");
		assert!(s.should_emit(&t, 2)); // sets best = 2
		assert!(s.should_emit(&t, 2)); // tie at 2 emits
		assert!(!s.should_emit(&t, 3)); // deeper suppressed
		// Extension allows ties at min depth, prunes deeper.
		let x = rid("x");
		assert!(s.should_extend(&x, 1));
		assert!(s.should_extend(&x, 1));
		assert!(!s.should_extend(&x, 2));
	}

	#[test]
	fn counted_emits_k_and_extends_up_to_k() {
		let mut s = TerminalSelector::new(ShortestSelector::Counted(2), 1);
		let t = rid("t");
		assert!(s.should_emit(&t, 2));
		assert!(s.should_emit(&t, 3));
		assert!(!s.should_emit(&t, 4)); // k == 2 reached
		// Count-based extension cap: each node extends at most k = 2 paths.
		let x = rid("x");
		assert!(s.should_extend(&x, 1));
		assert!(s.should_extend(&x, 1));
		assert!(!s.should_extend(&x, 5)); // 3rd extension pruned
	}

	#[test]
	fn counted_groups_extends_k_distinct_depths() {
		let mut s = TerminalSelector::new(ShortestSelector::CountedGroups(2), 1);
		let x = rid("x");
		// Depth-based cap: extend across the k = 2 smallest distinct depths, all
		// ties at each; the 3rd distinct depth is pruned.
		assert!(s.should_extend(&x, 1)); // depth 1, group 1
		assert!(s.should_extend(&x, 1)); // tie at depth 1
		assert!(s.should_extend(&x, 2)); // depth 2, group 2
		assert!(s.should_extend(&x, 2)); // tie at depth 2
		assert!(!s.should_extend(&x, 3)); // 3rd distinct depth pruned
	}

	#[test]
	fn counted_groups_emits_k_distinct_lengths() {
		let mut s = TerminalSelector::new(ShortestSelector::CountedGroups(2), 1);
		let t = rid("t");
		assert!(s.should_emit(&t, 2)); // group 1 (len 2)
		assert!(s.should_emit(&t, 2)); // same group
		assert!(s.should_emit(&t, 3)); // group 2 (len 3)
		assert!(s.should_emit(&t, 3)); // same group
		assert!(!s.should_emit(&t, 4)); // would be a 3rd group
	}

	#[test]
	fn min_above_one_disables_pruning() {
		let mut s = TerminalSelector::new(ShortestSelector::AnyShortest, 2);
		let x = rid("x");
		// With min > 1 the in-window shortest may differ from the global
		// shortest, so frontier pruning is disabled (full enumeration).
		assert!(s.should_extend(&x, 1));
		assert!(s.should_extend(&x, 1));
	}
}
