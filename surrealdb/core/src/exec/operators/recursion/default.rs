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

use super::common::is_recursion_target;
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
	min_depth: u32,
	max_depth: u32,
	user_specified_max: bool,
	ctx: EvalContext<'_>,
) -> FlowResult<Value> {
	let system_limit = ctx.exec_ctx.ctx().config().limits.idiom_recursion_limit as u32;
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
			return Err(crate::err::Error::InvalidRecursionTarget {
				value: next.to_sql(),
			}
			.into());
		}

		current = next;
	}

	// Exhausted depth limit without resolving
	if !user_specified_max && depth >= system_limit {
		return Err(crate::err::Error::IdiomRecursionLimitExceeded {
			limit: system_limit,
		}
		.into());
	}

	if depth >= min_depth {
		Ok(current)
	} else {
		Ok(Value::None)
	}
}
