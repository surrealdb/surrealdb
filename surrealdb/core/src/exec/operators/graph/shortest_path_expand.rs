//! Shortest-path search for GQL v2 `MATCH` (the `ALL SHORTEST` / `ANY SHORTEST`
//! / `SHORTEST k` / `SHORTEST k GROUP(S)` path-search prefixes).
//!
//! Like [`expand_paths`](super::path_expand::expand_paths) it expands a single
//! quantified segment from a bound *source* node, per input binding row, and
//! emits one row per selected path with the terminal node, the edge group
//! and the alternating path array. It differs in *which* paths it keeps:
//! a pattern carrying a `SHORTEST` (or `ANY`) selector is routed here.
//! Grouping is per-source-row by terminal node id, which equals the GQL endpoint
//! partition `(start, end)`. A pattern bound on its far node expands backwards
//! (`source` is the pattern's end); grouping and path length are symmetric so
//! selection is unaffected, and `reversed` flips the emitted path/group back to
//! the pattern's written order.
//!
//! ## Algorithm: BFS over partial paths
//!
//! Per source row the search runs a breadth-first traversal whose frontier is
//! a FIFO queue of partial paths `{ tip, edges, nodes }` (the same struct
//! path expansion uses). Because successors are enqueued at the back, paths are
//! processed in non-decreasing depth (hop count), so a terminal is *reached* in
//! non-decreasing length order — the property every selector relies on.
//! Edge-uniqueness within a path (DIFFERENT EDGES) is enforced by the
//! adjacency scan exactly as in path expansion, which also bounds the queue on
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

use tracing::debug;

use super::path_expand::{
	PartialPath, PathCaches, PathFlush, PathParams, StepKind, assemble_row, build_group_and_path,
	edge_target, node_id, node_passes_label, path_rows_exceeded, push_row, scan_dir,
	source_record_id, step_kind, target_field,
};
use crate::exec::operators::scan::fetch::resolve_with_field_state;
use crate::exec::operators::scan::graph_keys::collect_adjacency;
use crate::exec::{ExecutionContext, FlowResult};
use crate::val::{RecordId, Value};

/// Which of the paths to each endpoint a path search keeps. Named for the
/// shortest family (its main job), it also serves the non-shortest `ANY [k]`
/// selector ([`ShortestSelector::Any`]) — a shortest representative is a valid
/// arbitrary path, so `ANY` reuses the same cheap visit-once / `k`-bounded
/// traversal rather than the full path-expansion DFS.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
	pub(crate) fn attr(self) -> String {
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

/// The paths from the node bound at `params.source` in `row` that `selector`
/// keeps, as the rows of `out`: breadth-first, so a terminal is reached in
/// non-decreasing length, with a full batch handed to `flush` as it forms.
/// Selection and the per-source budget (frontier pushes and emits) are per
/// source row.
///
/// `false` once `flush` refused a batch, which ends the search.
pub(crate) async fn expand_shortest(
	ctx: &ExecutionContext,
	params: &PathParams,
	selector: ShortestSelector,
	caches: &mut PathCaches,
	row: &Value,
	out: &mut Vec<Value>,
	flush: &mut dyn PathFlush,
) -> FlowResult<bool> {
	let db_ctx = ctx.database()?;
	let txn = ctx.txn();
	let ns_id = db_ctx.ns_ctx.ns.namespace_id;
	let db_id = db_ctx.db.database_id;
	let version = ctx.version_stamp();
	let dir = scan_dir(params.direction);
	let tgt_field = target_field(params.direction);
	let path_row_limit = params.path_row_limit;

	let Some(source_rid) = source_record_id(row, &params.source) else {
		debug!(
			target: "surrealdb::exec::shortest_path_expand",
			"ShortestPathExpand source binding {} is not a bound node; dropping row",
			params.source,
		);
		return Ok(true);
	};
	let source_obj = if let Value::Object(obj) = row {
		obj.get(&params.source).cloned().unwrap_or(Value::Null)
	} else {
		Value::Null
	};

	// Per-source budget (frontier pushes + emits) and selector state.
	let mut path_count: usize = 0;
	let mut state = TerminalSelector::new(selector, params.min);
	state.mark_source(&source_rid);

	// --- Zero-length path: the source is its own terminal at depth 0.
	// Emitted iff min == 0, the source passes the label, and the selector
	// admits it.
	if params.min == 0
		&& node_passes_label(&source_obj, params.target_label.as_ref())
		&& state.should_emit(&source_rid, 0)
	{
		path_count += 1;
		if path_count > path_row_limit {
			return Err(path_rows_exceeded(path_row_limit));
		}
		let assembled = assemble_row(
			row,
			&params.target_binding,
			source_obj.clone(),
			params.group_binding.as_deref(),
			&[],
			params.path_binding.as_deref(),
			std::slice::from_ref(&source_obj),
		);
		if !push_row(params, out, assembled, flush).await? {
			return Ok(false);
		}
	}

	// --- BFS frontier (FIFO => non-decreasing depth).
	let mut queue: VecDeque<PartialPath> = VecDeque::new();
	if params.extends_past(0) {
		path_count += 1;
		if path_count > path_row_limit {
			return Err(path_rows_exceeded(path_row_limit));
		}
		queue.push_back(PartialPath {
			tip: source_rid.clone(),
			edges: Vec::new(),
			nodes: vec![source_obj.clone()],
		});
	}

	while let Some(path) = queue.pop_front() {
		crate::exec::vm::fuel::tick(ctx, 1).await?;
		let depth = path.depth();

		// Enumerate the tip's adjacency, retaining only edges not already on
		// the path (DIFFERENT EDGES), filtered per streamed item. Same scan as
		// path expansion.
		let mut edge_ids: Vec<RecordId> = Vec::new();
		let mut decoded_targets: Vec<Option<RecordId>> = Vec::new();
		collect_adjacency(
			&txn,
			ns_id,
			db_id,
			&path.tip,
			dir,
			&params.edge_specs,
			ctx,
			version,
			&mut caches.adjacency,
			|item| {
				if !path.contains_edge(&item.edge) {
					edge_ids.push(item.edge);
					decoded_targets.push(item.target);
				}
				Ok(())
			},
		)
		.await?;

		if edge_ids.is_empty() {
			continue;
		}

		let edge_objs = resolve_with_field_state(ctx, &mut caches.edges, &edge_ids).await?;

		let mut step_edges: Vec<(RecordId, Value)> = Vec::new();
		let mut node_ids: Vec<RecordId> = Vec::new();
		for ((edge_id, decoded_target), edge_obj) in
			edge_ids.into_iter().zip(decoded_targets).zip(edge_objs)
		{
			let Some(edge_obj) = edge_obj else {
				continue;
			};
			let next_id = decoded_target.or_else(|| edge_target(&edge_obj, tgt_field));
			let Some(next_id) = next_id else {
				continue;
			};
			step_edges.push((edge_id, edge_obj));
			node_ids.push(next_id);
		}

		if node_ids.is_empty() {
			continue;
		}

		let node_objs = resolve_with_field_state(ctx, &mut caches.nodes, &node_ids).await?;

		let new_depth = depth + 1;
		for ((edge_id, edge_obj), node_obj) in step_edges.into_iter().zip(node_objs) {
			let Some(node_obj) = node_obj else {
				continue;
			};
			let Some(target_id) = node_id(&node_obj) else {
				// A node without an id cannot be an endpoint or be
				// deduplicated; skip it.
				continue;
			};

			let kind = step_kind(params.mode, &path.nodes, &target_id);
			if matches!(kind, StepKind::Reject) {
				continue;
			}

			let mut edges = path.edges.clone();
			edges.push((edge_id, edge_obj));
			let mut nodes = path.nodes.clone();
			nodes.push(node_obj.clone());

			// Emit when in window, label-passing, and the selector admits this
			// arrival at `target_id`.
			if params.emits_at(new_depth)
				&& node_passes_label(&node_obj, params.target_label.as_ref())
				&& state.should_emit(&target_id, new_depth)
			{
				path_count += 1;
				if path_count > path_row_limit {
					return Err(path_rows_exceeded(path_row_limit));
				}
				let (group, path_arr) = build_group_and_path(&nodes, &edges, params.reversed);
				let assembled = assemble_row(
					row,
					&params.target_binding,
					node_obj.clone(),
					params.group_binding.as_deref(),
					&group,
					params.path_binding.as_deref(),
					&path_arr,
				);
				if !push_row(params, out, assembled, flush).await? {
					return Ok(false);
				}
			}

			// Extend when the mode permits (a SIMPLE close does not) another
			// hop is allowed, and the selector's frontier pruning admits it.
			if matches!(kind, StepKind::EmitAndExtend)
				&& params.extends_past(new_depth)
				&& state.should_extend(&target_id, new_depth)
			{
				path_count += 1;
				if path_count > path_row_limit {
					return Err(path_rows_exceeded(path_row_limit));
				}
				queue.push_back(PartialPath {
					tip: target_id,
					edges,
					nodes,
				});
			}
		}
	}
	Ok(true)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::val::TableName;

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
