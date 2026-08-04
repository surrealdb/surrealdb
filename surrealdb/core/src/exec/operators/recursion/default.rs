//! Default recursion strategy: follow path until bounds or dead end.
//!
//! Returns the final value after traversing the path up to max_depth times.
//! Fully iterative — uses a while loop with no recursive calls.
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
//! Example SurrealQL (default instruction = no `+collect` / `+path` / `+shortest`):
//!
//! ```surql
//! planet:earth.{3}.contains
//! -- or with graph edges: planet:earth.{1..4}->contains->?
//! ```
//!
//! With `min_depth=1`, `max_depth=3` this means: "follow the path up to 3 steps and return
//! the value we have after the last step (or when we hit a dead end)."
//!
//! # How the loop runs (step-by-step)
//!
//! Internal state: `current` (the value at the current depth), `depth` (steps taken).
//!
//! 1. **Initial:** `current = planet:earth`, `depth = 0`.
//!
//! 2. **Iteration 1:** `next = evaluate_physical_path(current, path)` → e.g. `[country:us,
//!    country:canada]`.
//!    - `depth` becomes 1.
//!    - `clean_iteration(next)` leaves an array (not a dead end).
//!    - Not final, not a cycle, and contains valid RecordIds, so we do not return.
//!    - `current = next` → `current` is now the countries array.
//!
//! 3. **Iteration 2:** `next = evaluate_physical_path(current, path)` → e.g. `[state:california,
//!    state:texas, province:ontario, province:bc]`.
//!    - `depth` becomes 2.
//!    - Not final, not equal to current; `current = next` (states/provinces).
//!
//! 4. **Iteration 3:** `next = evaluate_physical_path(current, path)` → e.g. array of cities.
//!    - `depth` becomes 3.
//!    - Not final; `current = next` (cities).
//!
//! 5. **Loop condition:** `depth (3) < max_depth (3)` is false → exit loop.
//!
//! 6. **After loop:** `depth >= min_depth` → return `Ok(current)` (the cities array).
//!
//! If at any step the path returns a dead end (`None`/`Null` or empty after cleaning) or we
//! detect a cycle (`next == current`), we exit early: we return the previous `current` if
//! `depth > min_depth`, otherwise the final value from the dead end.

use std::sync::Arc;

use surrealdb_types::ToSql;

use super::common::{RecursionBounds, is_recursion_target};
use crate::exec::FlowResult;
use crate::exec::parts::{clean_iteration, evaluate_physical_path, get_final, is_final};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::val::Value;

/// Default recursion: keep following the path until bounds or dead end.
///
/// Returns the final value after traversing the path up to max_depth times,
/// or None if min_depth is not reached before termination.
///
/// Fully iterative -- uses a while loop with no recursive calls.
pub(crate) async fn evaluate_recurse_default(
	start: &Value,
	path: &[Arc<dyn PhysicalExpr>],
	bounds: RecursionBounds,
	ctx: EvalContext<'_>,
) -> FlowResult<Value> {
	let min_depth = bounds.min;
	let max_depth = bounds.cap();
	let mut current = start.clone();
	let mut depth = 0u32;

	while depth < max_depth {
		let next = evaluate_physical_path(&current, path, ctx.with_value(&current)).await?;

		depth += 1;

		// Clean up dead ends from array results
		let next = clean_iteration(next);

		// Check termination conditions.
		if is_final(&next) || next == current {
			// Reached a dead end or cycle.
			// Use `depth > min_depth` (not `>=`) because the current iteration
			// produced a dead end, so we've only completed (depth - 1) successful
			// traversals.
			return if depth > min_depth {
				Ok(current)
			} else {
				Ok(get_final(&next))
			};
		}

		// Non-RecordId values during recursion are an error --
		// recursion is intended purely for record graph traversal.
		if !is_recursion_target(&next) {
			return Err(crate::exec::Error::InvalidRecursionTarget {
				value: next.to_sql(),
			}
			.into());
		}

		current = next;
	}

	// Exhausted depth limit without resolving
	if bounds.errors_on_limit() && depth >= bounds.system_limit {
		return Err(crate::exec::Error::IdiomRecursionLimitExceeded {
			limit: bounds.system_limit,
		}
		.into());
	}

	if depth >= min_depth {
		Ok(current)
	} else {
		Ok(Value::None)
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

	/// Run the default strategy over the body compiled from `src` (see
	/// `body_path`), starting at `start`.
	async fn run(
		start: &Value,
		src: &str,
		min: u32,
		max: Option<u32>,
		system_limit: u32,
		ctx: &ExecutionContext,
	) -> FlowResult<Value> {
		let path = body_path(src, ctx).await;
		let base = crate::exec::physical_expr::EvalContext::from_exec_ctx(ctx);
		evaluate_recurse_default(
			start,
			&path,
			bounds(min, max, system_limit),
			base.with_value(start),
		)
		.await
	}

	/// The common case: a `{min..max}` recursion over the record-link chain.
	async fn chain(
		start: &str,
		min: u32,
		max: Option<u32>,
		ctx: &ExecutionContext,
	) -> FlowResult<Value> {
		let start = val(start).await;
		run(&start, "link:a.next", min, max, SYSTEM_LIMIT, ctx).await
	}

	#[tokio::test]
	async fn an_exact_depth_returns_the_value_reached_at_that_depth() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;

		assert_eq!(chain("link:a", 1, Some(1), &ctx).await.unwrap(), val("link:b").await);
		assert_eq!(chain("link:a", 2, Some(2), &ctx).await.unwrap(), val("link:c").await);
		assert_eq!(chain("link:a", 3, Some(3), &ctx).await.unwrap(), val("link:d").await);
	}

	#[tokio::test]
	async fn a_range_terminates_at_the_upper_bound() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		assert_eq!(chain("link:a", 1, Some(2), &ctx).await.unwrap(), val("link:c").await);
	}

	#[tokio::test]
	async fn a_dead_end_short_of_the_upper_bound_returns_the_last_live_value() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// The chain dies at link:d, three steps in, so a `{1..9}` recursion
		// returns link:d rather than the dead end that followed it.
		assert_eq!(chain("link:a", 1, Some(9), &ctx).await.unwrap(), val("link:d").await);
	}

	#[tokio::test]
	async fn an_unbounded_recursion_that_dead_ends_never_reaches_the_limit() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// `max: None` makes exhausting the cap a hard error, but the dead end is
		// reached first and returns from inside the loop.
		let start = val("link:a").await;
		let result = run(&start, "link:a.next", 1, None, 8, &ctx).await.unwrap();
		assert_eq!(result, val("link:d").await);
	}

	#[tokio::test]
	async fn a_zero_depth_bound_returns_the_start_untouched() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// The parser rejects a minimum below 1, so this pins the loop's own
		// guard: a cap of 0 evaluates the body zero times.
		assert_eq!(chain("link:a", 0, Some(0), &ctx).await.unwrap(), val("link:a").await);
	}

	#[tokio::test]
	async fn a_branch_that_dies_before_min_depth_is_dropped_not_truncated() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;

		// link:c reaches link:d in one step and then dies. A recursion needing at
		// least three successful traversals must not return the truncated walk.
		assert_eq!(chain("link:c", 3, Some(5), &ctx).await.unwrap(), Value::None);
		// Same at an exact bound: the dead end lands on the boundary itself, so
		// only (depth - 1) traversals actually succeeded.
		assert_eq!(chain("link:c", 2, Some(2), &ctx).await.unwrap(), Value::None);
		// One step is enough, so the same walk is returned when min allows it.
		assert_eq!(chain("link:c", 1, Some(5), &ctx).await.unwrap(), val("link:d").await);
	}

	#[tokio::test]
	async fn the_dead_end_keeps_the_shape_of_the_value_that_produced_it() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;

		// A missing link field is NONE, so a dead end below min_depth is NONE.
		assert_eq!(chain("link:d", 1, Some(1), &ctx).await.unwrap(), Value::None);
		// An array input keeps producing arrays, so its dead end is the empty
		// array — this is what lets a following `.name` stay array-shaped.
		let start = val("[link:d]").await;
		assert_eq!(
			run(&start, "link:a.next", 1, Some(1), SYSTEM_LIMIT, &ctx).await.unwrap(),
			val("[]").await
		);
	}

	#[tokio::test]
	async fn a_self_loop_terminates_on_value_equality_with_the_previous_step() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;

		// link:s links to itself, so the very first traversal reproduces the
		// start value. That counts as a dead end: with min_depth 1 only zero
		// traversals are complete, so the final value is returned.
		assert_eq!(chain("link:s", 1, Some(4), &ctx).await.unwrap(), Value::None);
		// Below the minimum the same equality returns the value itself.
		assert_eq!(chain("link:s", 0, Some(4), &ctx).await.unwrap(), val("link:s").await);

		// A graph body wraps each step in an array, so equality is only reached
		// on the second traversal and the loop value survives.
		let start = val("node:s").await;
		assert_eq!(
			run(&start, "node:a->step->node", 1, Some(4), SYSTEM_LIMIT, &ctx).await.unwrap(),
			val("[node:s]").await
		);
	}

	#[tokio::test]
	async fn a_two_cycle_is_walked_to_the_bound_because_there_is_no_visited_set() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// link:p and link:q alternate, so successive steps never compare equal.
		// The bound is the only thing that stops the walk: six traversals of a
		// 2-cycle land back on the start node.
		assert_eq!(chain("link:p", 1, Some(6), &ctx).await.unwrap(), val("link:p").await);
		assert_eq!(chain("link:p", 1, Some(5), &ctx).await.unwrap(), val("link:q").await);
	}

	#[tokio::test]
	async fn an_unbounded_walk_of_a_cycle_raises_the_iteration_limit() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		let start = val("link:p").await;
		let err = run(&start, "link:a.next", 1, None, 8, &ctx).await.unwrap_err();
		assert!(matches!(
			exec_error(err),
			crate::exec::Error::IdiomRecursionLimitExceeded {
				limit: 8
			}
		));
	}

	#[tokio::test]
	async fn a_non_record_step_is_rejected_because_recursion_is_record_only() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		let start = val("link:a").await;
		let err = run(&start, "link:a.name", 1, Some(2), SYSTEM_LIMIT, &ctx).await.unwrap_err();
		match exec_error(err) {
			crate::exec::Error::InvalidRecursionTarget {
				value,
			} => assert_eq!(value, "'A'"),
			other => panic!("expected InvalidRecursionTarget, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn control_flow_out_of_the_body_aborts_the_walk() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		let start = val("link:a").await;
		let base = crate::exec::physical_expr::EvalContext::from_exec_ctx(&ctx);

		let path = raise_path(Raise::Return);
		let err = evaluate_recurse_default(
			&start,
			&path,
			bounds(1, Some(3), SYSTEM_LIMIT),
			base.with_value(&start),
		)
		.await
		.unwrap_err();
		match err {
			ControlFlow::Return(v) => assert_eq!(v, Value::from(7)),
			other => panic!("expected RETURN, got {other}"),
		}

		let path = raise_path(Raise::Error);
		let err = evaluate_recurse_default(
			&start,
			&path,
			bounds(1, Some(3), SYSTEM_LIMIT),
			base.with_value(&start),
		)
		.await
		.unwrap_err();
		assert!(matches!(err, ControlFlow::Err(_)));
	}
}
