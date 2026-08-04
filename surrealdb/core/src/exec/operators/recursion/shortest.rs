//! Shortest path recursion strategy: find shortest path to a target using BFS.
//!
//! Returns the first (shortest) path found to the target, or None if not
//! reachable within max_depth. Fully iterative — level-based BFS loop.
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
//! Example SurrealQL (target = a specific city):
//!
//! ```surql
//! planet:earth.{..+shortest=city:vancouver}.contains
//! -- or: planet:earth.{1..+shortest=city:vancouver+inclusive}->contains->?
//! ```
//!
//! With `min_depth=1`, `max_depth=10`, `inclusive=true`: "find the shortest path from
//! planet:earth to city:vancouver, including the start node in the path, and only
//! consider paths of length >= 1."
//!
//! # How the loop runs (step-by-step)
//!
//! Internal state: `queue` (FIFO of `(node, path_so_far)`), `seen` (hashes of visited
//! nodes so we do not re-enqueue the same node), `depth` (current BFS level).
//!
//! 1. **Initial:** `queue = [(planet:earth, [planet:earth])]` (if inclusive), `seen =
//!    {planet:earth}`, `depth = 0`.
//!
//! 2. **Iteration 1:** Process all nodes at this level (`level_size = queue.len()`). Pop
//!    (planet:earth, path). Evaluate path(planet:earth) → [country:us, country:canada]. For each
//!    successor: if it equals `target` (city:vancouver), we're not at min_depth yet (1 >= 1 but
//!    we're still at depth 0 before increment), so we only check target when `depth + 1 >=
//!    min_depth`. Neither country is the target. Add (country:us, [planet:earth, country:us]) and
//!    (country:canada, [planet:earth, country:canada]) to queue if not in `seen`. Then `depth = 1`.
//!
//! 3. **Iteration 2:** Process level: expand country:us and country:canada to states/provinces.
//!    None is city:vancouver. Enqueue (state:california, path), (state:texas, path),
//!    (province:ontario, path), (province:bc, path). `depth = 2`.
//!
//! 4. **Iteration 3:** Expand states/provinces to cities. When we expand province:bc we get
//!    city:vancouver. Check: `depth + 1 (3) >= min_depth (1)` and `v == target` → found. Build
//!    `final_path = current_path + city:vancouver`, return `Ok(Value::Array(final_path))`
//!    immediately.
//!
//! Result: e.g. `[planet:earth, country:canada, province:bc, city:vancouver]`. If the target
//! is never found before `max_depth`, we return `None` (or an array of remaining paths for
//! compatibility).

use std::collections::VecDeque;
use std::sync::Arc;

use surrealdb_types::ToSql;

use super::common::{RecursionBounds, eval_buffered, is_recursion_target};
use crate::exec::FlowResult;
use crate::exec::parts::recurse::value_hash;
use crate::exec::parts::{evaluate_physical_path, is_final};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::val::Value;

/// Shortest path recursion: find the shortest path to a target node using BFS.
///
/// Returns the first (shortest) path found to the target, or None if the
/// target is not reachable within max_depth.
///
/// Fully iterative -- level-based BFS loop.
pub(crate) async fn evaluate_recurse_shortest(
	start: &Value,
	target: &Value,
	path: &[Arc<dyn PhysicalExpr>],
	bounds: RecursionBounds,
	inclusive: bool,
	ctx: EvalContext<'_>,
) -> FlowResult<Value> {
	let min_depth = bounds.min;
	let max_depth = bounds.cap();
	let mut seen = std::collections::HashSet::new();

	let initial_path = if inclusive {
		vec![start.clone()]
	} else {
		vec![]
	};
	let mut queue: VecDeque<(Value, Vec<Value>)> = VecDeque::new();
	queue.push_back((start.clone(), initial_path));
	seen.insert(value_hash(start));

	let mut depth = 0u32;

	while depth < max_depth && !queue.is_empty() {
		// Drain this depth level from the queue into a vec.
		let level: Vec<(Value, Vec<Value>)> = queue.drain(..).collect();

		// Phase 1: Evaluate all level values concurrently (bounded).
		// Uses `buffered` (ordered) so results align with `level` for zip.
		let futures: Vec<_> = level
			.iter()
			.map(|(current, _)| evaluate_physical_path(current, path, ctx.with_value(current)))
			.collect();
		let eval_results = eval_buffered(futures).await?;

		// Phase 2: Process results sequentially (target check, dedup, enqueue).
		for ((_, current_path), result) in level.into_iter().zip(eval_results) {
			// Destructure directly into the inner Vec.
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

				// Check if we found the target (only if min_depth reached)
				if depth + 1 >= min_depth && &v == target {
					let mut final_path = current_path;
					final_path.push(v);
					return Ok(Value::Array(final_path.into()));
				}

				let hash = value_hash(&v);
				if seen.insert(hash) {
					let mut new_path = current_path.clone();
					new_path.push(v.clone());
					queue.push_back((v, new_path));
				}
			}
		}

		depth += 1;
	}

	// Unbounded recursion truncated at the system limit with the queue still
	// non-empty: hard error, matching legacy (see `RecursionBounds`).
	if bounds.errors_on_limit() && !queue.is_empty() {
		return Err(crate::exec::Error::IdiomRecursionLimitExceeded {
			limit: bounds.system_limit,
		}
		.into());
	}

	// Target not found within max_depth.
	let remaining_paths: Vec<Value> = queue
		.into_iter()
		.filter(|(_, p)| !p.is_empty())
		.map(|(_, p)| Value::Array(p.into()))
		.collect();

	if remaining_paths.is_empty() {
		Ok(Value::None)
	} else {
		Ok(Value::Array(remaining_paths.into()))
	}
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

	/// Search the `next` record-link graph from `start` for `target`.
	#[allow(clippy::too_many_arguments)]
	async fn run(
		start: &str,
		target: &str,
		src: &str,
		min: u32,
		max: Option<u32>,
		inclusive: bool,
		system_limit: u32,
		ctx: &ExecutionContext,
	) -> FlowResult<Value> {
		let start = val(start).await;
		let target = val(target).await;
		let path = body_path(src, ctx).await;
		let base = crate::exec::physical_expr::EvalContext::from_exec_ctx(ctx);
		evaluate_recurse_shortest(
			&start,
			&target,
			&path,
			bounds(min, max, system_limit),
			inclusive,
			base.with_value(&start),
		)
		.await
	}

	async fn links(
		start: &str,
		target: &str,
		min: u32,
		max: Option<u32>,
		inclusive: bool,
		ctx: &ExecutionContext,
	) -> FlowResult<Value> {
		run(start, target, "link:a.next", min, max, inclusive, SYSTEM_LIMIT, ctx).await
	}

	#[tokio::test]
	async fn the_first_walk_that_reaches_the_target_is_returned() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// link:w closes both diamond branches; BFS reaches it first through
		// link:y and returns immediately.
		assert_eq!(
			links("link:x", "link:w", 1, Some(9), false, &ctx).await.unwrap(),
			val("[link:y, link:w]").await
		);
		assert_eq!(
			links("link:x", "link:w", 1, Some(9), true, &ctx).await.unwrap(),
			val("[link:x, link:y, link:w]").await
		);
	}

	#[tokio::test]
	async fn the_shorter_of_two_routes_to_the_target_wins() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// link:o is reachable from link:m directly and via link:n. The level-based
		// search returns the one-step route.
		assert_eq!(
			links("link:m", "link:o", 1, Some(9), false, &ctx).await.unwrap(),
			val("[link:o]").await
		);
	}

	#[tokio::test]
	async fn the_target_is_only_matched_when_discovered_so_the_start_is_not_a_hit() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// Searching for the start node itself does not return a zero-length walk:
		// the match is tested against discovered successors only, so link:p is
		// found by going round the cycle.
		assert_eq!(
			links("link:p", "link:p", 1, Some(4), false, &ctx).await.unwrap(),
			val("[link:q, link:p]").await
		);
	}

	#[tokio::test]
	async fn a_target_reached_below_min_depth_is_not_matched() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// link:y sits one step from link:x, below the minimum of two, and nothing
		// in the diamond leads back to it — so the search finds nothing.
		assert_eq!(links("link:x", "link:y", 2, Some(3), false, &ctx).await.unwrap(), Value::None);
	}

	#[tokio::test]
	async fn an_exhausted_search_returns_none() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// The chain runs out before the bound, leaving nothing queued.
		assert_eq!(links("link:a", "link:zz", 1, Some(9), false, &ctx).await.unwrap(), Value::None);
	}

	#[tokio::test]
	async fn a_search_cut_off_with_the_frontier_still_live_returns_the_frontier_walks() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// Stopping at the bound with the queue non-empty returns the walks that
		// were still in flight rather than NONE — they are not paths to the
		// target, only the state the search stopped in.
		assert_eq!(
			links("link:a", "link:zz", 1, Some(2), false, &ctx).await.unwrap(),
			val("[[link:b, link:c]]").await
		);
	}

	#[tokio::test]
	async fn a_cycle_is_closed_by_the_visited_set_so_an_unbounded_search_terminates() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// `max: None` would raise the limit error if the queue were still live at
		// the cap; the visited set empties it after one lap.
		assert_eq!(
			run("link:p", "link:zz", "link:a.next", 1, None, false, 8, &ctx).await.unwrap(),
			Value::None
		);
	}

	#[tokio::test]
	async fn an_unbounded_search_with_a_live_queue_at_the_cap_raises_the_limit() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		let err =
			run("link:a", "link:zz", "link:a.next", 1, None, false, 2, &ctx).await.unwrap_err();
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
		assert_eq!(
			run("link:a", "link:zz", "link:a.next", 1, Some(2), false, 2, &ctx).await.unwrap(),
			val("[[link:b, link:c]]").await
		);
	}

	#[tokio::test]
	async fn a_non_record_value_is_rejected_because_recursion_is_record_only() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		let err = run("link:a", "link:zz", "link:a.name", 1, Some(2), false, SYSTEM_LIMIT, &ctx)
			.await
			.unwrap_err();
		match exec_error(err) {
			crate::exec::Error::InvalidRecursionTarget {
				value,
			} => assert_eq!(value, "'A'"),
			other => panic!("expected InvalidRecursionTarget, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn control_flow_out_of_the_body_aborts_the_search() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		let start = val("link:a").await;
		let target = val("link:d").await;
		let base = crate::exec::physical_expr::EvalContext::from_exec_ctx(&ctx);

		let path = raise_path(Raise::Error);
		let err = evaluate_recurse_shortest(
			&start,
			&target,
			&path,
			bounds(1, Some(3), SYSTEM_LIMIT),
			false,
			base.with_value(&start),
		)
		.await
		.unwrap_err();
		match err {
			ControlFlow::Err(e) => assert_eq!(e.to_string(), "body blew up"),
			other => panic!("expected an error, got {other}"),
		}
	}
}
