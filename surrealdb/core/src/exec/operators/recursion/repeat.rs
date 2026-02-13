//! RepeatRecurse (`@`) strategy: two-phase BFS discovery + backward assembly.
//!
//! Fully iterative — no stack recursion. Used when the path contains `@` markers
//! (e.g. in destructuring to build a tree of nested results).
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
//! Example SurrealQL (repeat recurse builds a nested tree per record):
//!
//! ```surql
//! SELECT VALUE @{..}.{ id, name, places: contains.@ } FROM planet;
//! -- For planet:earth this yields one object: { id, name, places: [ { id, name, places: [ ... ] }, ... ] }
//! ```
//!
//! The `@` means "recurse here and replace with the same shape"; the engine uses this
//! module to evaluate that recursion without stack recursion.
//!
//! # How the two phases run (step-by-step)
//!
//! **Phase 1 — Forward BFS Discovery**
//!
//! Internal state: `levels[d]` = list of values (record ids) discovered at depth `d`.
//!
//! 1. **Initial:** `levels[0] = [planet:earth]`.
//!
//! 2. **Depth 0:** For each value in `levels[0]`, evaluate the full path with `@` in *discovery*
//!    mode: the `@` writes its inputs (the children) into a shared `sink` instead of recursing. So
//!    path(planet:earth) pushes country:us, country:canada into the sink. Deduplicate within level
//!    → `next_level = [country:us, country:canada]`. `levels = [[planet:earth], [country:us,
//!    country:canada]]`.
//!
//! 3. **Depth 1:** Evaluate path for country:us and country:canada; `@` writes states and provinces
//!    into the sink. `levels` gains a third row: states and provinces.
//!
//! 4. **Depth 2:** Same for states/provinces → cities. Then no new nodes (cities have no contains),
//!    so `next_level` is empty and we break. We now have `levels[0..=3]` with no stack recursion.
//!
//! **Phase 2 — Backward Assembly**
//!
//! Internal state: `next_cache` = map from value_hash to the assembled result for that
//! value (the nested structure for the sub-tree rooted at that value). We iterate depths
//! from highest to 0.
//!
//! 1. **Depth 3 (cities):** At or beyond max_depth we store raw values: `current_cache[hash(city)]
//!    = city`.
//!
//! 2. **Depth 2 (states/provinces):** For each state/province, evaluate the path with `@` in
//!    *assembly* mode: `@` looks up each child in `next_cache` (the cities we just stored). We get
//!    e.g. `[city:la, city:sf]`. Store `current_cache[hash(state:california)] = [city:la,
//!    city:sf]`. Then `next_cache = current_cache` for the next (lower) depth.
//!
//! 3. **Depth 1 (countries):** Same: path(country:us) in assembly mode looks up states in
//!    `next_cache`, gets their assembled arrays. Store country → { places: [state:ca, state:tx] }
//!    etc.
//!
//! 4. **Depth 0 (start):** path(planet:earth) in assembly mode looks up countries in `next_cache`.
//!    We get the full nested tree. Return `next_cache.remove(hash(planet:earth))`.
//!
//! Result: a single value that is the nested tree for the start record (e.g. planet:earth
//! with places: [country:us with places: [...], country:canada with places: [...]], without
//! using any stack recursion.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use surrealdb_types::ToSql;

use super::common::{self, discover_body_targets, eval_buffered_all, is_recursion_target};
use crate::exec::parts::recurse::value_hash;
use crate::exec::parts::{clean_iteration, evaluate_physical_path, get_final, is_final};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr, RecursionCtx};
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::val::Value;

/// Sentinel error type used to signal path elimination during recursion.
///
/// When a RepeatRecurse (`@`) in assembly mode finds that all results are
/// dead ends and the current depth is below `min_depth`, it raises this
/// signal. The signal propagates through the Destructure (skipping
/// remaining field evaluation) and is caught by the assembly loop in
/// `evaluate_recurse_iterative`, which stores `Value::None` -- allowing
/// the parent level's `clean_iteration` to filter the eliminated sub-tree.
#[derive(Debug)]
pub(crate) struct PathEliminationSignal;

impl std::fmt::Display for PathEliminationSignal {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "path elimination signal")
	}
}

impl std::error::Error for PathEliminationSignal {}

/// Handle the RepeatRecurse (@) marker during path evaluation.
///
/// This reads the recursion context from EvalContext and dispatches to one
/// of two modes set by `evaluate_recurse_iterative`:
///
/// 1. **Discovery mode** (`discovery_sink` is `Some`): Write each non-final input value to the
///    shared sink. Returns the input as-is (the value won't be used -- only the sink contents
///    matter). No recursion.
///
/// 2. **Assembly mode** (`assembly_cache` is `Some`): Look up each element's pre-computed result
///    from the cache via `value_hash`. Apply `clean_iteration` and path-elimination checks. No
///    recursion.
///
/// Both modes are fully iterative -- no stack recursion occurs.
pub(crate) fn evaluate_repeat_recurse<'a>(
	value: &'a Value,
	ctx: EvalContext<'a>,
) -> crate::exec::BoxFut<'a, FlowResult<Value>> {
	Box::pin(async move {
		let rec_ctx = match &ctx.recursion_ctx {
			Some(rc) => rc.clone(),
			None => {
				// RepeatRecurse outside recursion context is an error
				return Err(crate::err::Error::UnsupportedRepeatRecurse.into());
			}
		};

		// ── Discovery mode ──────────────────────────────────────────────
		// Write non-final, valid recursion targets to the shared sink,
		// return input as-is. Only RecordIds (and arrays of them) are
		// valid recursion targets.
		if let Some(ref sink) = rec_ctx.discovery_sink {
			let values_to_write: Vec<Value> = match value {
				Value::Array(arr) => {
					let mut targets = Vec::new();
					for v in arr.iter() {
						if is_final(v) {
							continue;
						}
						if !is_recursion_target(v) {
							return Err(crate::err::Error::InvalidRecursionTarget {
								value: v.to_sql(),
							}
							.into());
						}
						targets.push(v.clone());
					}
					targets
				}
				v if is_final(v) => vec![],
				v if is_recursion_target(v) => vec![v.clone()],
				v => {
					return Err(crate::err::Error::InvalidRecursionTarget {
						value: v.to_sql(),
					}
					.into());
				}
			};
			if !values_to_write.is_empty() {
				let mut guard = sink.lock().map_err(|_| {
					ControlFlow::Err(anyhow::anyhow!("recursion discovery sink mutex poisoned"))
				})?;
				guard.extend(values_to_write);
			}
			// Return a placeholder -- the discovery phase discards results.
			return Ok(value.clone());
		}

		// ── Assembly mode ───────────────────────────────────────────────
		// Look up pre-computed results from the cache.
		// The depth check uses `(depth + 1) < min_depth` to match the
		// semantics where the recursive call would happen at `depth + 1`.
		if let Some(ref cache) = rec_ctx.assembly_cache {
			let next_depth = rec_ctx.depth + 1;
			return match value {
				Value::Array(arr) => {
					let mut results = Vec::with_capacity(arr.len());
					for elem in arr.iter() {
						if is_final(elem) {
							continue;
						}
						let hash = value_hash(elem);
						if let Some(cached) = cache.get(&hash) {
							results.push(cached.clone());
						}
						// If not in cache, the value was never discovered
						// (shouldn't happen), skip it.
					}
					let result = clean_iteration(Value::Array(results.into()));
					if is_final(&result) && next_depth < rec_ctx.min_depth {
						return Err(ControlFlow::Err(anyhow::Error::new(PathEliminationSignal)));
					}
					Ok(result)
				}
				v if !is_final(v) => {
					let hash = value_hash(v);
					let result = cache.get(&hash).cloned().unwrap_or(Value::None);
					if is_final(&result) && next_depth < rec_ctx.min_depth {
						return Err(ControlFlow::Err(anyhow::Error::new(PathEliminationSignal)));
					}
					Ok(result)
				}
				// Final values (None, Null) -- check path elimination.
				_ => {
					if next_depth < rec_ctx.min_depth {
						return Err(ControlFlow::Err(anyhow::Error::new(PathEliminationSignal)));
					}
					Ok(Value::None)
				}
			};
		}

		// Neither discovery_sink nor assembly_cache is set -- this should
		// not happen in normal execution since RecursionOp always uses the
		// iterative evaluator which sets one of these fields.
		Err(crate::err::Error::UnsupportedRepeatRecurse.into())
	})
}

/// Iterative evaluation of a RepeatRecurse (`@`) recursion.
///
/// Replaces the stack-recursive chain with a two-phase approach:
///
/// **Phase 1 -- Forward BFS Discovery:** Walk the graph level by level.
/// When a `body` operator is available (the fused lookup chain), it is
/// executed directly to discover target RecordIds -- this avoids fetching
/// documents for non-recursive destructure fields (e.g. `name`), which
/// would otherwise happen when evaluating the full path.
/// When `body` is `None` (record-link patterns without graph edges), falls
/// back to the original approach: evaluate the full path with `@` writing
/// its input values to a shared sink (discovery mode).
///
/// **Phase 2 -- Backward Assembly:** Process from the deepest level to 0.
/// At each depth, evaluate the full path with `@` doing a cache lookup for
/// the next depth's pre-computed results (assembly mode). Since deeper
/// levels are already resolved, no recursion is needed.
pub(crate) async fn evaluate_recurse_iterative(
	start: &Value,
	path: &[Arc<dyn PhysicalExpr>],
	min_depth: u32,
	max_depth: u32,
	body: &Option<Arc<dyn crate::exec::ExecOperator>>,
	exec_ctx: &ExecutionContext,
	ctx: EvalContext<'_>,
) -> FlowResult<Value> {
	// Early exit: if start is a dead end, return immediately.
	if is_final(start) {
		return Ok(get_final(start));
	}

	// ── Phase 1: Forward BFS Discovery ──────────────────────────────
	//
	// Build `levels[d]` = values discovered at depth d.
	//
	// When a body operator is available, we execute it directly for each
	// value -- this produces the target RecordIds without evaluating the
	// full destructure path. This eliminates record fetches for non-
	// recursive fields (e.g. `name` in `{ name, knows: ->knows->person.@ }`).
	//
	// When no body operator is available, we fall back to evaluating the
	// full path with `@` in discovery mode: it writes its inputs to a
	// shared sink instead of recursing.
	//
	// Values are deduplicated WITHIN each level (same value at same depth
	// is redundant) but NOT across levels. The same value CAN appear at
	// different depths -- this is required for DAGs where a node is
	// reachable via multiple paths of different lengths.
	let mut levels: Vec<Vec<Value>> = vec![vec![start.clone()]];

	for d in 0..(max_depth as usize) {
		let current_level = &levels[d];
		if current_level.is_empty() {
			break;
		}

		let raw_discovered: Vec<Value> = if let Some(body_op) = body {
			// ── Fast path: use the body operator to discover targets ──
			// Execute the fused lookup chain directly. This only performs
			// the graph scan / record-link resolution without evaluating
			// destructure fields that are irrelevant for discovery.
			let futs: Vec<_> = current_level
				.iter()
				.filter(|val| !is_final(val) && is_recursion_target(val))
				.map(|val| {
					let body_ctx = exec_ctx.with_current_value(val.clone());
					discover_body_targets(body_op, body_ctx)
				})
				.collect();
			let all_discovered = common::eval_buffered(futs).await?;
			all_discovered.into_iter().flatten().collect()
		} else {
			// ── Fallback: full-path evaluation with discovery sink ──
			// Create a shared sink for this depth's discoveries.
			let sink: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));

			// Build a RecursionCtx in discovery mode.
			let discovery_ctx = RecursionCtx {
				min_depth,
				depth: d as u32,
				discovery_sink: Some(Arc::clone(&sink)),
				assembly_cache: None,
			};

			// Evaluate the path for each value at this depth.
			// The `@` part will write discovered values to the shared sink.
			let futures: Vec<_> = current_level
				.iter()
				.filter(|val| !is_final(val))
				.map(|val| {
					let eval = ctx.with_value(val).with_recursion_ctx(discovery_ctx.clone());
					evaluate_physical_path(val, path, eval)
				})
				.collect();
			let eval_results = eval_buffered_all(futures).await;

			// Check for hard errors (PathEliminationSignal is OK).
			for result in eval_results {
				match result {
					Ok(_) => {}
					Err(ControlFlow::Err(ref e))
						if e.downcast_ref::<PathEliminationSignal>().is_some() => {}
					Err(other) => return Err(other),
				}
			}

			// Extract discovered values from the sink.
			// We use lock+take instead of Arc::try_unwrap because the cloned
			// RecursionCtx instances may still hold Arc references to the sink.
			std::mem::take(&mut *sink.lock().map_err(|_| {
				ControlFlow::Err(anyhow::anyhow!("recursion discovery sink mutex poisoned"))
			})?)
		};

		// Per-level deduplication.
		let mut seen_level: HashSet<u64> = HashSet::new();
		let mut next_level = Vec::new();
		for v in raw_discovered {
			let hash = value_hash(&v);
			if seen_level.insert(hash) {
				next_level.push(v);
			}
		}

		if next_level.is_empty() {
			break;
		}
		levels.push(next_level);
	}

	let num_levels = levels.len();

	// ── Phase 2: Backward Assembly ──────────────────────────────────
	//
	// Start from the deepest level and work backward to depth 0.
	// At each depth, evaluate the full path with `@` in assembly mode:
	// it looks up the NEXT depth's pre-computed results from a cache.
	//
	// At levels >= max_depth, store raw values as base cases (matching
	// the original recursive behavior where `depth >= max_depth` returns
	// the value without evaluating the path).
	let mut next_cache: HashMap<u64, Value> = HashMap::new();

	for d in (0..num_levels).rev() {
		let mut current_cache: HashMap<u64, Value> = HashMap::new();

		if d as u32 >= max_depth {
			// Base case: at or beyond max_depth, store raw values.
			// The `@` marker at this depth would not recurse further.
			for val in &levels[d] {
				current_cache.insert(value_hash(val), val.clone());
			}
		} else {
			let cache_arc = Arc::new(next_cache);

			let assembly_ctx = RecursionCtx {
				min_depth,
				depth: d as u32,
				discovery_sink: None,
				assembly_cache: Some(Arc::clone(&cache_arc)),
			};

			// Handle final values directly (no I/O needed).
			// Collect non-final values for concurrent evaluation.
			let eval_values: Vec<&Value> = levels[d]
				.iter()
				.filter(|val| {
					if is_final(val) {
						current_cache.insert(value_hash(val), get_final(val));
						false
					} else {
						true
					}
				})
				.collect();

			// Evaluate non-final values concurrently (bounded).
			// Uses `buffered` (ordered) so results align with `eval_values` for zip.
			let futures: Vec<_> = eval_values
				.iter()
				.map(|val| {
					let eval = ctx.with_value(val).with_recursion_ctx(assembly_ctx.clone());
					evaluate_physical_path(val, path, eval)
				})
				.collect();
			let eval_results = eval_buffered_all(futures).await;

			// Process results: PathEliminationSignal -> Value::None, others propagate.
			for (val, result) in eval_values.iter().zip(eval_results) {
				let result = match result {
					Ok(v) => v,
					// PathEliminationSignal during assembly means the sub-tree
					// was eliminated. Store Value::None so the parent can filter.
					Err(ControlFlow::Err(ref e))
						if e.downcast_ref::<PathEliminationSignal>().is_some() =>
					{
						Value::None
					}
					Err(other) => return Err(other),
				};
				current_cache.insert(value_hash(val), result);
			}
		}

		next_cache = current_cache;
	}

	// The depth-0 result for `start` is the final assembled tree.
	let start_hash = value_hash(start);
	Ok(next_cache.remove(&start_hash).unwrap_or(Value::None))
}
