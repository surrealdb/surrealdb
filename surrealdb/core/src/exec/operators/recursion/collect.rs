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
					return Err(crate::exec::Error::InvalidRecursionTarget {
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
		return Err(crate::exec::Error::IdiomRecursionLimitExceeded {
			limit: bounds.system_limit,
		}
		.into());
	}

	Ok(Value::Array(collected.into()))
}

#[cfg(test)]
mod tests {
	use super::super::tests::{
		FIXTURES, Raise, SYSTEM_LIMIT, body_path, bounds, exec_error, raise_path,
	};
	use super::*;
	use crate::exec::ExecutionContext;
	use crate::exec::operators::test_util::{TestDb, val};
	use crate::expr::ControlFlow;

	/// Run the collect strategy over the record-link body `src` from `start`.
	async fn run(
		start: &str,
		src: &str,
		min: u32,
		max: Option<u32>,
		inclusive: bool,
		system_limit: u32,
		ctx: &ExecutionContext,
	) -> FlowResult<Value> {
		let start = val(start).await;
		let path = body_path(src, ctx).await;
		let base = crate::exec::physical_expr::EvalContext::from_exec_ctx(ctx);
		evaluate_recurse_collect(
			&start,
			&path,
			bounds(min, max, system_limit),
			inclusive,
			base.with_value(&start),
		)
		.await
	}

	/// Run over the `next` record-link body with the default system limit.
	async fn links(
		start: &str,
		min: u32,
		max: Option<u32>,
		inclusive: bool,
		ctx: &ExecutionContext,
	) -> FlowResult<Value> {
		run(start, "link:a.next", min, max, inclusive, SYSTEM_LIMIT, ctx).await
	}

	#[tokio::test]
	async fn every_reachable_node_is_collected_once_in_breadth_first_order() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// A single level collects the immediate successors in order.
		assert_eq!(
			links("link:x", 1, Some(1), false, &ctx).await.unwrap(),
			val("[link:y, link:z]").await
		);
		// link:w sits at the bottom of both diamond branches; it is collected
		// once, and the output is ordered level by level.
		assert_eq!(
			links("link:x", 1, Some(2), false, &ctx).await.unwrap(),
			val("[link:y, link:z, link:w]").await
		);
	}

	#[tokio::test]
	async fn inclusive_collects_the_start_and_never_collects_it_again() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// link:p is re-reached at depth 2 through the cycle but is already in the
		// output-dedup set from depth 0.
		assert_eq!(
			links("link:p", 1, Some(3), true, &ctx).await.unwrap(),
			val("[link:p, link:q]").await
		);
	}

	#[tokio::test]
	async fn the_inclusive_start_is_still_expanded_when_a_cycle_returns_to_it() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// `seen` and `expanded` are separate sets. link:e is seeded into `seen`
		// by `inclusive`, and its successors are only reachable within
		// `{2..3}` by expanding it again when the cycle comes back through it.
		assert_eq!(
			links("link:e", 2, Some(3), true, &ctx).await.unwrap(),
			val("[link:e, link:f, link:g]").await
		);
	}

	#[tokio::test]
	async fn nodes_below_min_depth_are_not_collected_but_are_still_expanded() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// link:b sits at depth 1, below the minimum, so it is expanded without
		// being collected.
		assert_eq!(
			links("link:a", 2, Some(3), false, &ctx).await.unwrap(),
			val("[link:c, link:d]").await
		);
	}

	#[tokio::test]
	async fn a_node_seen_below_min_depth_is_still_collected_when_re_reached_in_range() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// link:q is reached at depth 1 (below the minimum, so not collected) and
		// again at depth 3 through the cycle, where it must be collected. That
		// is why the below-min phase deduplicates per level only.
		assert_eq!(
			links("link:p", 2, Some(3), false, &ctx).await.unwrap(),
			val("[link:p, link:q]").await
		);
	}

	#[tokio::test]
	async fn a_cycle_is_closed_by_the_visited_set_so_an_unbounded_walk_terminates() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// `max: None` would raise the limit error if the frontier were still
		// advancing at the cap; `expanded` empties it first.
		assert_eq!(
			run("link:p", "link:a.next", 1, None, false, 8, &ctx).await.unwrap(),
			val("[link:q, link:p]").await
		);
	}

	#[tokio::test]
	async fn an_unbounded_walk_still_advancing_at_the_cap_raises_the_limit() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// The chain is longer than the cap, so the frontier is non-empty when the
		// loop stops.
		let err = run("link:a", "link:a.next", 1, None, false, 2, &ctx).await.unwrap_err();
		assert!(matches!(
			exec_error(err),
			crate::exec::Error::IdiomRecursionLimitExceeded {
				limit: 2
			}
		));
	}

	#[tokio::test]
	async fn an_explicit_bound_truncates_silently_instead_of_raising() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// Same truncation as above, but the user asked for it.
		assert_eq!(
			run("link:a", "link:a.next", 1, Some(2), false, 2, &ctx).await.unwrap(),
			val("[link:b, link:c]").await
		);
	}

	#[tokio::test]
	async fn a_dead_end_ends_its_own_branch_only() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// link:o is a dead end reached at depth 1 alongside link:n, which keeps
		// expanding. Neither the dead end nor the duplicate discovery of link:o
		// at depth 2 changes the output.
		assert_eq!(
			links("link:m", 1, Some(3), false, &ctx).await.unwrap(),
			val("[link:n, link:o]").await
		);
	}

	#[tokio::test]
	async fn a_dead_end_start_collects_nothing_unless_it_is_inclusive() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// The result is always an array, empty rather than NONE.
		assert_eq!(links("link:d", 1, Some(3), false, &ctx).await.unwrap(), val("[]").await);
		// `inclusive` seeds the start unconditionally, dead end or not.
		assert_eq!(links("link:d", 1, Some(3), true, &ctx).await.unwrap(), val("[link:d]").await);
	}

	#[tokio::test]
	async fn dedup_keys_on_the_value_hash_alone_so_colliding_array_targets_merge() {
		let db = TestDb::new(FIXTURES).await;
		// A nested array is itself a recursion target, and `value_hash` summarises
		// an array as its length plus its first eight elements. These two
		// ten-element arrays agree on both, so they share a hash.
		db.run(
			"UPSERT link:h SET next = [
				[link:a, link:b, link:c, link:d, link:w, link:x, link:y, link:z, link:m, link:n],
				[link:a, link:b, link:c, link:d, link:w, link:x, link:y, link:z, link:o, link:p]
			];",
		)
		.await;
		let ctx = db.exec_ctx().await;

		// `seen` and `expanded` hold hashes, not values, so the second target is
		// taken for a repeat of the first and only one of the two is collected.
		assert_eq!(
			links("link:h", 1, Some(1), false, &ctx).await.unwrap(),
			val(
				"[[link:a, link:b, link:c, link:d, link:w, link:x, link:y, link:z, link:m, link:n]]"
			)
			.await
		);
	}

	#[tokio::test]
	async fn a_non_record_value_is_rejected_because_recursion_is_record_only() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		let err =
			run("link:a", "link:a.name", 1, Some(2), false, SYSTEM_LIMIT, &ctx).await.unwrap_err();
		match exec_error(err) {
			crate::exec::Error::InvalidRecursionTarget {
				value,
			} => assert_eq!(value, "'A'"),
			other => panic!("expected InvalidRecursionTarget, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn control_flow_out_of_the_body_aborts_the_traversal() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		let start = val("link:a").await;
		let base = crate::exec::physical_expr::EvalContext::from_exec_ctx(&ctx);

		let path = raise_path(Raise::Break);
		let err = evaluate_recurse_collect(
			&start,
			&path,
			bounds(1, Some(3), SYSTEM_LIMIT),
			false,
			base.with_value(&start),
		)
		.await
		.unwrap_err();
		assert!(matches!(err, ControlFlow::Break));
	}
}
