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
//! 1. **Initial:** `queue = [(planet:earth, [planet:earth])]` (if inclusive), `seen = {planet:earth}`,
//!    `depth = 0`.
//!
//! 2. **Iteration 1:** Process all nodes at this level (`level_size = queue.len()`). Pop
//!    (planet:earth, path). Evaluate path(planet:earth) → [country:us, country:canada]. For
//!    each successor: if it equals `target` (city:vancouver), we're not at min_depth yet (1 >= 1
//!    but we're still at depth 0 before increment), so we only check target when `depth + 1 >= min_depth`.
//!    Neither country is the target. Add (country:us, [planet:earth, country:us]) and
//!    (country:canada, [planet:earth, country:canada]) to queue if not in `seen`. Then
//!    `depth = 1`.
//!
//! 3. **Iteration 2:** Process level: expand country:us and country:canada to states/provinces.
//!    None is city:vancouver. Enqueue (state:california, path), (state:texas, path),
//!    (province:ontario, path), (province:bc, path). `depth = 2`.
//!
//! 4. **Iteration 3:** Expand states/provinces to cities. When we expand province:bc we get
//!    city:vancouver. Check: `depth + 1 (3) >= min_depth (1)` and `v == target` → found.
//!    Build `final_path = current_path + city:vancouver`, return `Ok(Value::Array(final_path))`
//!    immediately.
//!
//! Result: e.g. `[planet:earth, country:canada, province:bc, city:vancouver]`. If the target
//! is never found before `max_depth`, we return `None` (or an array of remaining paths for
//! compatibility).

use std::collections::VecDeque;
use std::sync::Arc;

use super::common::{eval_buffered, is_recursion_target};
use crate::exec::parts::recurse::value_hash;
use crate::exec::parts::{evaluate_physical_path, is_final};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::FlowResult;
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
	min_depth: u32,
	max_depth: u32,
	inclusive: bool,
	ctx: EvalContext<'_>,
) -> FlowResult<Value> {
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
				// Non-RecordId values are treated as terminal --
				// recursion is intended purely for record graph traversal.
				if is_final(&v) || !is_recursion_target(&v) {
					continue;
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
