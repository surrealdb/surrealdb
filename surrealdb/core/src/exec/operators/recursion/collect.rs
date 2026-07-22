//! Collect recursion strategy: gather all unique nodes during BFS traversal.
//!
//! Uses breadth-first search to collect all reachable nodes, respecting
//! depth bounds and avoiding cycles via hash-based deduplication.
//! Fully iterative — frontier-based BFS loop.
//!
//! # Example data and query
//!
//! Using a hierarchy of record links (e.g. planet → country → state/province → city):
//!
//! ```text
//! planet:earth  (contains: [country:us, country:canada])
//! ├── country:us     → contains: [state:california, state:texas]
//! │   └── state:california → contains: [city:los_angeles, city:san_francisco]
//! └── country:canada  → contains: [province:ontario, province:bc]
//!     └── province:ontario → contains: [city:toronto, city:ottawa]
//! ```
//!
//! Example SurrealQL:
//!
//! ```surql
//! planet:earth.{..+collect}.contains
//! -- or: planet:earth.{2..4+collect+inclusive}->contains->?
//! ```
//!
//! With `min_depth=1`, `max_depth=3`, `inclusive=false`: "collect every unique node
//! reached at depth 1, 2, or 3 (do not include the start node)."
//!
//! # How the loop runs (step-by-step)
//!
//! Internal state: `collected` (output list), `seen` (hashes of nodes already collected),
//! `expanded` (hashes of nodes already expanded at depth >= min_depth),
//! `frontier` (nodes to expand at current depth), `depth` (current level).
//!
//! 1. **Initial:** `frontier = [planet:earth]`, `collected = []`, `seen = {}`, `depth = 0`. If
//!    `inclusive`: push start into `collected` and `seen`.
//!
//! 2. **Iteration 1 (depth 0):** For each value in `frontier` (planet:earth), evaluate path → e.g.
//!    `[country:us, country:canada]`. For each `v` (discovered at `depth + 1 = 1 >= min_depth`): if
//!    `v` not in `seen`, insert hash into `seen` and push `v` into `collected`; if `v` not in
//!    `expanded`, insert hash and push `v` into `next_frontier`. Then `frontier = next_frontier` =
//!    [country:us, country:canada], `depth = 1`. (At depths below `min_depth`, discovered nodes are
//!    instead deduplicated per level only and pushed to `next_frontier` without collection.)
//!
//! 3. **Iteration 2 (depth 1):** Expand country:us → states; country:canada → provinces. Each new
//!    node (state:california, state:texas, province:ontario, province:bc) is added to `seen`, to
//!    `collected` (2 >= 1), and to `next_frontier`. `frontier` = those four, `depth = 2`.
//!
//! 4. **Iteration 3 (depth 2):** Expand each state/province to cities. New nodes (cities) go into
//!    `seen`, `collected` (3 >= 1), and `next_frontier`. `depth = 3`.
//!
//! 5. **Loop exit:** `depth (3) < max_depth (3)` is false → exit. Return `Value::Array(collected)`.
//!
//! Result: a flat array of all unique nodes at depths 1..max_depth (e.g. countries, then
//! states/provinces, then cities), with no duplicates even if the graph has cycles.

use std::collections::HashSet;
use std::sync::Arc;

use surrealdb_types::ToSql;

use super::common::{RecursionBounds, eval_buffered, is_recursion_target};
use crate::exec::FlowResult;
use crate::exec::parts::recurse::value_hash;
use crate::exec::parts::{evaluate_physical_path, is_final};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::val::Value;

/// Collect recursion: gather all unique nodes encountered during BFS traversal.
///
/// Collects every distinct node reachable by a walk whose length falls in
/// `[min_depth, max_depth]`, matching the legacy compute engine. Walks may
/// revisit nodes, so cycle pruning must not lose nodes whose only in-range
/// walk passes through an already-visited node:
///
/// - Below `min_depth`, the frontier is deduplicated per level only. A node visited here is not
///   collected, so it must remain collectable (and expandable) when re-reached at a depth within
///   range via a cycle.
/// - At or beyond `min_depth`, a node is collected once and expanded once (`expanded`): any walk
///   through a later occurrence reaches the same nodes at shallower, still-in-range depths via the
///   first occurrence. This also bounds unbounded recursion on cyclic graphs.
///
/// Fully iterative -- frontier-based BFS loop.
pub(crate) async fn evaluate_recurse_collect(
	start: &Value,
	path: &[Arc<dyn PhysicalExpr>],
	bounds: RecursionBounds,
	inclusive: bool,
	ctx: EvalContext<'_>,
) -> FlowResult<Value> {
	let min_depth = bounds.min;
	let max_depth = bounds.cap();
	let mut collected = Vec::new();
	// Nodes already collected (output dedup). The inclusive start is seeded
	// here so it is not collected again if re-reached through a cycle.
	let mut seen: HashSet<u64> = HashSet::new();
	// Nodes already expanded at a depth >= min_depth. Deliberately separate
	// from `seen`: the inclusive start sits in `seen` from depth 0 but must
	// still be expanded when re-reached at a depth within range.
	let mut expanded: HashSet<u64> = HashSet::new();
	let mut frontier = vec![start.clone()];

	if inclusive {
		collected.push(start.clone());
		seen.insert(value_hash(start));
	}

	let mut depth = 0u32;

	while depth < max_depth && !frontier.is_empty() {
		let mut next_frontier = Vec::new();
		// Nodes discovered in this iteration sit at depth + 1.
		let collecting = depth + 1 >= min_depth;
		// Per-level frontier dedup for the below-min phase.
		let mut level_seen: HashSet<u64> = HashSet::new();

		// Phase 1: Evaluate all frontier values concurrently (bounded).
		let futures: Vec<_> = frontier
			.iter()
			.map(|value| evaluate_physical_path(value, path, ctx.with_value(value)))
			.collect();
		let eval_results = eval_buffered(futures).await?;

		// Phase 2: Aggregate results sequentially (fast, no I/O).
		for result in eval_results {
			// Destructure directly into the inner Vec to avoid
			// iterator + collect overhead.
			let values = match result {
				Value::Array(arr) => arr.0,
				Value::None | Value::Null => continue,
				other => vec![other],
			};

			for v in values {
				// Dead ends (None, Null, empty arrays) silently terminate this branch.
				if is_final(&v) {
					continue;
				}

				// Non-RecordId values during recursion are an error --
				// recursion is intended purely for record graph traversal.
				if !is_recursion_target(&v) {
					return Err(crate::err::Error::InvalidRecursionTarget {
						value: v.to_sql(),
					}
					.into());
				}

				let hash = value_hash(&v);
				if collecting {
					if seen.insert(hash) {
						collected.push(v.clone());
					}
					if expanded.insert(hash) {
						next_frontier.push(v);
					}
				} else {
					// Below min_depth: dedupe within this level only, so the
					// node stays collectable when re-reached within range.
					if level_seen.insert(hash) {
						next_frontier.push(v);
					}
				}
			}
		}

		frontier = next_frontier;
		depth += 1;
	}

	// Unbounded recursion truncated at the system limit with the frontier still
	// non-empty: hard error, matching legacy (see `RecursionBounds`).
	if bounds.errors_on_limit() && !frontier.is_empty() {
		return Err(crate::err::Error::IdiomRecursionLimitExceeded {
			limit: bounds.system_limit,
		}
		.into());
	}

	Ok(Value::Array(collected.into()))
}
