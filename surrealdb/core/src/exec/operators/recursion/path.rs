//! Path recursion strategy: return all paths as arrays of arrays.
//!
//! Tracks all possible paths through the graph, returning each complete
//! path as an array. Paths terminate at dead ends or max depth.
//! Fully iterative — BFS loop over active paths.
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
//! planet:earth.{..+path}.contains
//! -- or: planet:earth.{2..3+path+inclusive}->contains->?
//! ```
//!
//! With `min_depth=2`, `max_depth=3`, `inclusive=true`: "return every path from the start
//! that has length between 2 and 3 steps, with the start node included in each path."
//!
//! # How the loop runs (step-by-step)
//!
//! Internal state: `completed_paths` (finished paths to return), `active_paths` (paths we are
//! still extending; each element is a `Vec<Value>` = one path), `depth` (current step index).
//!
//! 1. **Initial:** If `inclusive`: `active_paths = [[planet:earth]]`; else `active_paths = [[]]`.
//!    `completed_paths = []`, `depth = 0`.
//!
//! 2. **Iteration 1:** For each path in `active_paths`, take the last value (e.g. planet:earth),
//!    evaluate path → [country:us, country:canada]. We get two successors. For each successor we
//!    push a new path: [planet:earth, country:us] and [planet:earth, country:canada]. So
//!    `next_paths` has 2 paths. `active_paths = next_paths`, `depth = 1`.
//!
//! 3. **Iteration 2:** Expand [planet:earth, country:us] from country:us → state:california,
//!    state:texas; expand [planet:earth, country:canada] → province:ontario, province:bc. Each
//!    yields two new paths (clone path prefix + one successor, or move for the last). So we get 4
//!    paths of length 3. If `depth >= min_depth` and we hit a dead end on some branch, that path is
//!    pushed to `completed_paths`. `depth = 2`.
//!
//! 4. **Iteration 3:** Expand the 4 paths from their leaf nodes (states/provinces) to cities. Each
//!    state/province may have 2 cities, so we get many new paths. Paths that reach a dead end (city
//!    with no contains) are completed and pushed to `completed_paths`. `depth = 3`.
//!
//! 5. **Loop exit:** `depth (3) < max_depth (3)` is false → exit. Any remaining `active_paths` that
//!    reached max_depth without a dead end are appended to `completed_paths`. Return
//!    `Value::Array(completed_paths)` — each element is `Value::Array(path)`.
//!
//! Result: e.g. `[[planet:earth, country:us, state:california], [planet:earth, country:us,
//! state:texas], [planet:earth, country:canada, province:ontario], [planet:earth, country:canada,
//! province:bc], ...]`.

use std::sync::Arc;

use surrealdb_types::ToSql;

use super::common::{RecursionBounds, eval_buffered, is_recursion_target};
use crate::exec::FlowResult;
use crate::exec::parts::{evaluate_physical_path, is_final};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::val::Value;

/// Path recursion: return all paths as arrays of arrays.
///
/// Tracks all possible paths through the graph, returning each complete
/// path as an array. Paths terminate at dead ends or max depth.
///
/// Fully iterative -- BFS loop over active paths.
pub(crate) async fn evaluate_recurse_path(
	start: &Value,
	path: &[Arc<dyn PhysicalExpr>],
	bounds: RecursionBounds,
	inclusive: bool,
	ctx: EvalContext<'_>,
) -> FlowResult<Value> {
	let min_depth = bounds.min;
	let max_depth = bounds.cap();
	let mut completed_paths: Vec<Value> = Vec::new();
	let mut active_paths: Vec<Vec<Value>> = if inclusive {
		vec![vec![start.clone()]]
	} else {
		vec![vec![]]
	};

	let mut depth = 0u32;

	while depth < max_depth && !active_paths.is_empty() {
		let mut next_paths = Vec::new();

		// Phase 1: Evaluate all active path tips concurrently (bounded).
		// Uses `buffered` (ordered) so results align with `active_paths` for zip.
		let futures: Vec<_> = active_paths
			.iter()
			.map(|current_path| {
				let current_value = current_path.last().unwrap_or(start);
				evaluate_physical_path(current_value, path, ctx.with_value(current_value))
			})
			.collect();
		let eval_results = eval_buffered(futures).await?;

		// Phase 2: Pair results with path prefixes and aggregate sequentially.
		for (mut current_path, result) in active_paths.into_iter().zip(eval_results) {
			// Destructure directly into the inner Vec.
			let values = match result {
				Value::Array(arr) => arr.0,
				Value::None | Value::Null => {
					if depth >= min_depth && !current_path.is_empty() {
						completed_paths.push(Value::Array(current_path.into()));
					}
					continue;
				}
				other => vec![other],
			};

			// Single pass: extend paths for valid recursion targets, detect dead ends.
			// On the last valid value we move current_path instead of cloning
			// to save one allocation per branch point.
			let mut valid_targets = Vec::new();
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

				valid_targets.push(v);
			}

			if valid_targets.is_empty() {
				// All values were dead ends
				if depth >= min_depth && !current_path.is_empty() {
					completed_paths.push(Value::Array(current_path.into()));
				}
			} else {
				let mut iter = valid_targets.into_iter().peekable();
				while let Some(v) = iter.next() {
					if iter.peek().is_some() {
						// More successors to come -- clone the path prefix
						let mut new_path = current_path.clone();
						new_path.push(v);
						next_paths.push(new_path);
					} else {
						// Last successor -- move the path prefix (saves a clone)
						current_path.push(v);
						next_paths.push(current_path);
						break;
					}
				}
			}
		}

		active_paths = next_paths;
		depth += 1;
	}

	// Unbounded recursion truncated at the system limit with paths still
	// active: hard error, matching legacy (see `RecursionBounds`).
	if bounds.errors_on_limit() && !active_paths.is_empty() {
		return Err(crate::exec::Error::IdiomRecursionLimitExceeded {
			limit: bounds.system_limit,
		}
		.into());
	}

	// Add remaining active paths that reached max depth
	for p in active_paths {
		if !p.is_empty() && depth >= min_depth {
			completed_paths.push(Value::Array(p.into()));
		}
	}

	Ok(Value::Array(completed_paths.into()))
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

	/// Run the path strategy over the body `src` from the value `start`.
	async fn run_from(
		start: &Value,
		src: &str,
		min: u32,
		max: Option<u32>,
		inclusive: bool,
		system_limit: u32,
		ctx: &ExecutionContext,
	) -> FlowResult<Value> {
		let path = body_path(src, ctx).await;
		let base = crate::exec::physical_expr::EvalContext::from_exec_ctx(ctx);
		evaluate_recurse_path(
			start,
			&path,
			bounds(min, max, system_limit),
			inclusive,
			base.with_value(start),
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
		let start = val(start).await;
		run_from(&start, "link:a.next", min, max, inclusive, SYSTEM_LIMIT, ctx).await
	}

	#[tokio::test]
	async fn each_walk_is_returned_as_its_own_array() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// Non-inclusive walks start from an empty prefix, so the first step is
		// still evaluated against the start node but the start is not in the
		// output.
		assert_eq!(
			links("link:x", 1, Some(1), false, &ctx).await.unwrap(),
			val("[[link:y], [link:z]]").await
		);
		assert_eq!(
			links("link:x", 1, Some(2), false, &ctx).await.unwrap(),
			val("[[link:y, link:w], [link:z, link:w]]").await
		);
		assert_eq!(
			links("link:x", 1, Some(2), true, &ctx).await.unwrap(),
			val("[[link:x, link:y, link:w], [link:x, link:z, link:w]]").await
		);
	}

	#[tokio::test]
	async fn a_walk_that_dead_ends_at_or_past_min_depth_is_completed() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// The diamond bottoms out at link:w after two steps, well short of the
		// upper bound, and both walks are returned as they stood.
		assert_eq!(
			links("link:x", 1, Some(9), false, &ctx).await.unwrap(),
			val("[[link:y, link:w], [link:z, link:w]]").await
		);
	}

	#[tokio::test]
	async fn a_walk_that_dies_before_min_depth_is_dropped_not_truncated() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// Every diamond walk is two steps long, so a minimum of three leaves
		// nothing — the two-step walks are discarded rather than returned short.
		assert_eq!(links("link:x", 3, Some(4), false, &ctx).await.unwrap(), val("[]").await);
	}

	#[tokio::test]
	async fn walks_still_active_at_the_upper_bound_are_returned() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		assert_eq!(
			links("link:a", 1, Some(2), false, &ctx).await.unwrap(),
			val("[[link:b, link:c]]").await
		);
	}

	#[tokio::test]
	async fn a_dead_end_start_yields_no_walks_at_all() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// Zero steps is below the minimum, so even the inclusive prefix — which
		// is a non-empty path — is discarded.
		assert_eq!(links("link:d", 1, Some(3), false, &ctx).await.unwrap(), val("[]").await);
		assert_eq!(links("link:d", 1, Some(3), true, &ctx).await.unwrap(), val("[]").await);
	}

	#[tokio::test]
	async fn dead_end_elements_inside_a_step_result_drop_only_their_own_branch() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// `.next` over [link:d, link:c] yields [NONE, link:d]: the NONE is
		// skipped and only the live successor extends the walk.
		let start = val("[link:d, link:c]").await;
		assert_eq!(
			run_from(&start, "link:a.next", 1, Some(3), false, SYSTEM_LIMIT, &ctx).await.unwrap(),
			val("[[link:d]]").await
		);
	}

	#[tokio::test]
	async fn a_cycle_is_walked_to_the_bound_because_walks_carry_no_visited_set() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// The 2-cycle produces one walk that revisits both nodes; only the depth
		// bound stops it.
		assert_eq!(
			links("link:p", 1, Some(4), false, &ctx).await.unwrap(),
			val("[[link:q, link:p, link:q, link:p]]").await
		);
	}

	#[tokio::test]
	async fn a_self_loop_repeats_inside_the_walk_rather_than_closing_it() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// link:s links to itself. There is no equality check here (unlike the
		// default strategy), so the node is appended once per step until the
		// bound is reached.
		assert_eq!(
			links("link:s", 1, Some(3), false, &ctx).await.unwrap(),
			val("[[link:s, link:s, link:s]]").await
		);
	}

	#[tokio::test]
	async fn an_unbounded_walk_still_active_at_the_cap_raises_the_limit() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		let start = val("link:a").await;
		let err = run_from(&start, "link:a.next", 1, None, false, 2, &ctx).await.unwrap_err();
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
		let start = val("link:a").await;
		assert_eq!(
			run_from(&start, "link:a.next", 1, Some(2), false, 2, &ctx).await.unwrap(),
			val("[[link:b, link:c]]").await
		);
	}

	#[tokio::test]
	async fn a_non_record_value_is_rejected_because_recursion_is_record_only() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		let start = val("link:a").await;
		let err = run_from(&start, "link:a.name", 1, Some(2), false, SYSTEM_LIMIT, &ctx)
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
	async fn control_flow_out_of_the_body_aborts_the_traversal() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		let start = val("link:a").await;
		let base = crate::exec::physical_expr::EvalContext::from_exec_ctx(&ctx);

		let path = raise_path(Raise::Continue);
		let err = evaluate_recurse_path(
			&start,
			&path,
			bounds(1, Some(3), SYSTEM_LIMIT),
			false,
			base.with_value(&start),
		)
		.await
		.unwrap_err();
		assert!(matches!(err, ControlFlow::Continue));
	}
}
