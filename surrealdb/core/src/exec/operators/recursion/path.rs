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
//!    state:texas; expand [planet:earth, country:canada] → province:ontario, province:bc.
//!    Each yields two new paths (clone path prefix + one successor, or move for the last).
//!    So we get 4 paths of length 3. If `depth >= min_depth` and we hit a dead end on some
//!    branch, that path is pushed to `completed_paths`. `depth = 2`.
//!
//! 4. **Iteration 3:** Expand the 4 paths from their leaf nodes (states/provinces) to cities.
//!    Each state/province may have 2 cities, so we get many new paths. Paths that reach a
//!    dead end (city with no contains) are completed and pushed to `completed_paths`.
//!    `depth = 3`.
//!
//! 5. **Loop exit:** `depth (3) < max_depth (3)` is false → exit. Any remaining `active_paths`
//!    that reached max_depth without a dead end are appended to `completed_paths`. Return
//!    `Value::Array(completed_paths)` — each element is `Value::Array(path)`.
//!
//! Result: e.g. `[[planet:earth, country:us, state:california], [planet:earth, country:us, state:texas],
//! [planet:earth, country:canada, province:ontario], [planet:earth, country:canada, province:bc], ...]`.

use std::sync::Arc;

use super::common::{eval_buffered, is_recursion_target};
use crate::exec::parts::{evaluate_physical_path, is_final};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::FlowResult;
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
	min_depth: u32,
	max_depth: u32,
	inclusive: bool,
	ctx: EvalContext<'_>,
) -> FlowResult<Value> {
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
			// Non-RecordId values are treated as terminal -- recursion is
			// intended purely for record graph traversal.
			// On the last valid value we move current_path instead of cloning
			// to save one allocation per branch point.
			let mut non_final =
				values.into_iter().filter(|v| !is_final(v) && is_recursion_target(v)).peekable();

			if non_final.peek().is_none() {
				// All values were final -- dead end
				if depth >= min_depth && !current_path.is_empty() {
					completed_paths.push(Value::Array(current_path.into()));
				}
			} else {
				while let Some(v) = non_final.next() {
					if non_final.peek().is_some() {
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

	// Add remaining active paths that reached max depth
	for p in active_paths {
		if !p.is_empty() && depth >= min_depth {
			completed_paths.push(Value::Array(p.into()));
		}
	}

	Ok(Value::Array(completed_paths.into()))
}
