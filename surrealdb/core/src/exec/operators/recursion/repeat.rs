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
use std::sync::Arc;

use surrealdb_types::ToSql;

use super::common::{
	self, RecursionBounds, discover_body_targets, eval_buffered_all, is_recursion_target,
};
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
				return Err(crate::exec::Error::UnsupportedRepeatRecurse.into());
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
							return Err(crate::exec::Error::InvalidRecursionTarget {
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
					return Err(crate::exec::Error::InvalidRecursionTarget {
						value: v.to_sql(),
					}
					.into());
				}
			};
			if !values_to_write.is_empty() {
				sink.lock().extend(values_to_write);
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
		Err(crate::exec::Error::UnsupportedRepeatRecurse.into())
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
	bounds: RecursionBounds,
	body: &Option<Arc<dyn crate::exec::ExecOperator>>,
	exec_ctx: &ExecutionContext,
	ctx: EvalContext<'_>,
) -> FlowResult<Value> {
	let min_depth = bounds.min;
	let max_depth = bounds.cap();
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
			let sink: Arc<parking_lot::Mutex<Vec<Value>>> =
				Arc::new(parking_lot::Mutex::new(Vec::new()));

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
			std::mem::take(&mut *sink.lock())
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

	// Unbounded recursion with a non-empty level still discovered at the system
	// limit (one level pushed per iteration): hard error, matching legacy (see
	// `RecursionBounds`).
	if bounds.errors_on_limit() && num_levels > max_depth as usize {
		return Err(crate::exec::Error::IdiomRecursionLimitExceeded {
			limit: bounds.system_limit,
		}
		.into());
	}

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

#[cfg(test)]
mod tests {
	use super::super::tests::{
		FIXTURES, Raise, SYSTEM_LIMIT, body_operator, body_path, bounds, exec_error, raise_path,
	};
	use super::*;
	use crate::exec::ExecutionContext;
	use crate::exec::operators::test_util::{TestDb, root_ctx, val};

	// =========================================================================
	// evaluate_repeat_recurse — the two phases of the `@` protocol
	// =========================================================================

	/// A recursion context in discovery mode, sharing `sink`.
	fn discovery(
		depth: u32,
		min_depth: u32,
		sink: &Arc<parking_lot::Mutex<Vec<Value>>>,
	) -> RecursionCtx {
		RecursionCtx {
			min_depth,
			depth,
			discovery_sink: Some(Arc::clone(sink)),
			assembly_cache: None,
		}
	}

	/// A recursion context in assembly mode over `entries`, keyed the way the
	/// assembly loop keys them.
	async fn assembly(depth: u32, min_depth: u32, entries: &[(&str, Value)]) -> RecursionCtx {
		let mut cache = HashMap::new();
		for (key, value) in entries {
			cache.insert(value_hash(&val(key).await), value.clone());
		}
		RecursionCtx {
			min_depth,
			depth,
			discovery_sink: None,
			assembly_cache: Some(Arc::new(cache)),
		}
	}

	/// Evaluate a `@` marker on `value` under `rec_ctx`.
	async fn repeat_on(value: &Value, rec_ctx: RecursionCtx) -> FlowResult<Value> {
		let exec = root_ctx();
		let base = EvalContext::from_exec_ctx(&exec);
		evaluate_repeat_recurse(value, base.with_value(value).with_recursion_ctx(rec_ctx)).await
	}

	/// Whether a failed `FlowResult` carries the path-elimination signal.
	fn is_elimination(flow: &ControlFlow) -> bool {
		match flow {
			ControlFlow::Err(e) => e.downcast_ref::<PathEliminationSignal>().is_some(),
			_ => false,
		}
	}

	#[tokio::test]
	async fn a_repeat_recurse_outside_a_recursion_context_is_rejected() {
		let exec = root_ctx();
		let base = EvalContext::from_exec_ctx(&exec);
		let value = val("link:a").await;
		let err = evaluate_repeat_recurse(&value, base.with_value(&value)).await.unwrap_err();
		assert!(matches!(exec_error(err), crate::exec::Error::UnsupportedRepeatRecurse));
	}

	#[tokio::test]
	async fn a_recursion_context_with_neither_phase_set_is_rejected() {
		// Only the iterative evaluator builds these, and it always sets exactly
		// one of the two fields.
		let neither = RecursionCtx {
			min_depth: 1,
			depth: 0,
			discovery_sink: None,
			assembly_cache: None,
		};
		let err = repeat_on(&val("link:a").await, neither).await.unwrap_err();
		assert!(matches!(exec_error(err), crate::exec::Error::UnsupportedRepeatRecurse));
	}

	#[tokio::test]
	async fn discovery_writes_its_inputs_to_the_sink_and_returns_without_recursing() {
		let sink = Arc::new(parking_lot::Mutex::new(Vec::new()));
		let input = val("[link:a, NONE, link:b, NULL, []]").await;

		// The return value is the input, untouched: the discovery phase discards
		// results and reads the sink instead.
		let out = repeat_on(&input, discovery(0, 1, &sink)).await.unwrap();
		assert_eq!(out, input);
		let expected = vec![val("link:a").await, val("link:b").await];
		assert_eq!(&*sink.lock(), &expected);
	}

	#[tokio::test]
	async fn discovery_of_a_scalar_writes_one_target_and_a_dead_end_writes_nothing() {
		let sink = Arc::new(parking_lot::Mutex::new(Vec::new()));
		assert_eq!(
			repeat_on(&val("link:a").await, discovery(0, 1, &sink)).await.unwrap(),
			val("link:a").await
		);
		assert_eq!(repeat_on(&Value::None, discovery(0, 1, &sink)).await.unwrap(), Value::None);
		assert_eq!(repeat_on(&Value::Null, discovery(0, 1, &sink)).await.unwrap(), Value::Null);
		let expected = vec![val("link:a").await];
		assert_eq!(&*sink.lock(), &expected);
	}

	#[tokio::test]
	async fn discovery_rejects_a_non_record_value_bare_or_inside_an_array() {
		let sink = Arc::new(parking_lot::Mutex::new(Vec::new()));

		let bare = repeat_on(&val("'sample'").await, discovery(0, 1, &sink)).await.unwrap_err();
		assert!(matches!(exec_error(bare), crate::exec::Error::InvalidRecursionTarget { .. }));

		let inside =
			repeat_on(&val("[link:a, 'sample']").await, discovery(0, 1, &sink)).await.unwrap_err();
		assert!(matches!(exec_error(inside), crate::exec::Error::InvalidRecursionTarget { .. }));
	}

	#[tokio::test]
	async fn assembly_reads_the_pre_computed_result_out_of_the_cache() {
		let cache = assembly(0, 1, &[("link:b", val("{ name: 'B' }").await)]).await;
		// Scalar input: the cached sub-tree replaces the record id.
		assert_eq!(
			repeat_on(&val("link:b").await, cache.clone()).await.unwrap(),
			val("{ name: 'B' }").await
		);
		// Array input: one cache lookup per element.
		assert_eq!(
			repeat_on(&val("[link:b]").await, cache).await.unwrap(),
			val("[{ name: 'B' }]").await
		);
	}

	#[tokio::test]
	async fn assembly_skips_dead_ends_and_elements_that_were_never_discovered() {
		let cache =
			assembly(0, 1, &[("link:b", val("{ name: 'B' }").await), ("link:c", Value::None)])
				.await;
		// link:a was never discovered so it has no cache entry; NONE is a dead
		// end; link:c assembled to a dead end and `clean_iteration` drops it.
		assert_eq!(
			repeat_on(&val("[link:a, NONE, link:b, link:c]").await, cache).await.unwrap(),
			val("[{ name: 'B' }]").await
		);
	}

	#[tokio::test]
	async fn assembly_flattens_one_level_of_the_values_it_splices_in() {
		// Each child assembled to an array, and the spliced result is flattened
		// so a `contains.@` chain stays a flat list of nodes.
		let cache =
			assembly(0, 1, &[("link:b", val("[link:c]").await), ("link:d", val("[link:w]").await)])
				.await;
		assert_eq!(
			repeat_on(&val("[link:b, link:d]").await, cache).await.unwrap(),
			val("[link:c, link:w]").await
		);
	}

	#[tokio::test]
	async fn assembly_below_min_depth_eliminates_the_path_for_every_input_shape() {
		// `depth + 1` is the depth the recursive call would have happened at, so
		// a sub-tree that resolves to a dead end there is eliminated rather than
		// returned empty. Depth 0 with a minimum of 2 is below the bound.
		let below = || async { assembly(0, 2, &[("link:c", Value::None)]).await };

		// Array input whose lookups all resolve to dead ends.
		let arr = repeat_on(&val("[link:c]").await, below().await).await.unwrap_err();
		assert!(is_elimination(&arr), "array arm should eliminate, got {arr}");

		// Scalar input with no cache entry.
		let scalar = repeat_on(&val("link:zz").await, below().await).await.unwrap_err();
		assert!(is_elimination(&scalar), "scalar arm should eliminate, got {scalar}");

		// A final input value.
		let final_value = repeat_on(&Value::None, below().await).await.unwrap_err();
		assert!(is_elimination(&final_value), "final arm should eliminate, got {final_value}");
	}

	#[tokio::test]
	async fn assembly_at_or_above_min_depth_returns_the_dead_end_instead_of_eliminating() {
		// Same three shapes one depth deeper, where `depth + 1` meets the minimum.
		let at = || async { assembly(1, 2, &[("link:c", Value::None)]).await };

		assert_eq!(repeat_on(&val("[link:c]").await, at().await).await.unwrap(), val("[]").await);
		assert_eq!(repeat_on(&val("link:zz").await, at().await).await.unwrap(), Value::None);
		assert_eq!(repeat_on(&Value::None, at().await).await.unwrap(), Value::None);
	}

	// =========================================================================
	// evaluate_recurse_iterative — discovery + assembly end to end
	// =========================================================================

	/// Run the iterative evaluator over the body compiled from `src`, choosing
	/// whether the discovery phase uses the body operator or the sink fallback.
	async fn run(
		start: &Value,
		src: &str,
		min: u32,
		max: Option<u32>,
		system_limit: u32,
		use_body: bool,
		ctx: &ExecutionContext,
	) -> FlowResult<Value> {
		let path = body_path(src, ctx).await;
		let body = if use_body {
			body_operator(&path)
		} else {
			None
		};
		let base = EvalContext::from_exec_ctx(ctx);
		evaluate_recurse_iterative(
			start,
			&path,
			bounds(min, max, system_limit),
			&body,
			ctx,
			base.with_value(start),
		)
		.await
	}

	/// Run the `{ name, next: next.@ }` destructure body over the record links.
	async fn tree(
		start: &str,
		min: u32,
		max: Option<u32>,
		ctx: &ExecutionContext,
	) -> FlowResult<Value> {
		let start = val(start).await;
		run(&start, "link:a.{ name, next: next.@ }", min, max, SYSTEM_LIMIT, false, ctx).await
	}

	#[tokio::test]
	async fn a_final_start_returns_its_own_final_value_without_evaluating_the_body() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// The body here always raises, so reaching it at all would fail the test.
		let path = raise_path(Raise::Error);
		let base = EvalContext::from_exec_ctx(&ctx);
		for (start, expected) in [
			(Value::None, Value::None),
			(Value::Null, Value::Null),
			(val("[]").await, val("[]").await),
		] {
			let out = evaluate_recurse_iterative(
				&start,
				&path,
				bounds(1, Some(3), SYSTEM_LIMIT),
				&None,
				&ctx,
				base.with_value(&start),
			)
			.await
			.unwrap();
			assert_eq!(out, expected);
		}
	}

	#[tokio::test]
	async fn discovery_then_assembly_builds_the_nested_tree_one_level_per_depth() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;

		// Each depth adds one level of nesting; the level at the bound is stored
		// raw, which is why the innermost value is a record id.
		assert_eq!(
			tree("link:a", 1, Some(1), &ctx).await.unwrap(),
			val("{ name: 'A', next: link:b }").await
		);
		assert_eq!(
			tree("link:a", 2, Some(2), &ctx).await.unwrap(),
			val("{ name: 'A', next: { name: 'B', next: link:c } }").await
		);
		assert_eq!(
			tree("link:a", 3, Some(3), &ctx).await.unwrap(),
			val("{ name: 'A', next: { name: 'B', next: { name: 'C', next: link:d } } }").await
		);
	}

	#[tokio::test]
	async fn a_branching_body_assembles_every_child_of_every_level() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		assert_eq!(
			tree("link:x", 2, Some(2), &ctx).await.unwrap(),
			val(
				"{ name: 'X', next: [{ name: 'Y', next: [link:w] }, { name: 'Z', next: [link:w] }] }"
			)
			.await
		);
	}

	#[tokio::test]
	async fn a_node_reachable_at_two_depths_is_assembled_at_each_of_them() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// link:o sits at depth 1 (directly under link:m) and at depth 2 (under
		// link:n). Levels are deduplicated within a level only, so both
		// occurrences are assembled — at depth 1 as a sub-tree, at the bound as
		// a raw record id.
		assert_eq!(
			tree("link:m", 2, Some(2), &ctx).await.unwrap(),
			val("{ name: 'M', next: [{ name: 'N', next: [link:o] }, { name: 'O', next: NONE }] }")
				.await
		);
	}

	#[tokio::test]
	async fn a_sub_tree_that_cannot_reach_min_depth_is_eliminated() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		// The diamond is two levels deep, so a minimum of four eliminates every
		// sub-tree bottom-up and the whole result collapses to NONE rather than a
		// truncated tree.
		assert_eq!(tree("link:x", 4, Some(4), &ctx).await.unwrap(), Value::None);
		// A start that cannot take a single step is eliminated the same way.
		assert_eq!(tree("link:d", 2, Some(2), &ctx).await.unwrap(), Value::None);
	}

	#[tokio::test]
	async fn the_body_operator_and_the_sink_fallback_discover_the_same_levels() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		let start = val("node:a").await;
		let src = "node:a->step->node.@";

		let via_body = run(&start, src, 2, Some(2), SYSTEM_LIMIT, true, &ctx).await.unwrap();
		let via_sink = run(&start, src, 2, Some(2), SYSTEM_LIMIT, false, &ctx).await.unwrap();
		assert_eq!(via_body, val("[node:c]").await);
		assert_eq!(via_body, via_sink, "both discovery mechanisms must agree");
	}

	#[tokio::test]
	async fn a_deep_chain_is_assembled_without_stack_recursion() {
		let db = TestDb::new(FIXTURES).await;
		// A chain far deeper than a recursive implementation would tolerate per
		// frame: neither phase may descend the native stack.
		const DEPTH: u32 = 100;
		db.run(
			"FOR $i IN 1..=99 {
				UPSERT type::record('deep', $i) SET name = 'n', next = type::record('deep', $i + 1)
			};
			UPSERT deep:100 SET name = 'end';",
		)
		.await;
		let ctx = db.exec_ctx().await;

		let start = val("deep:1").await;
		let out =
			run(&start, "link:a.{ name, next: next.@ }", 1, Some(DEPTH), SYSTEM_LIMIT, false, &ctx)
				.await
				.unwrap();

		// One object per level, nested through `next`, ending on the record with
		// no outgoing link.
		let mut levels = 0;
		let mut node = out;
		while let Value::Object(obj) = node {
			levels += 1;
			node = obj.get("next").cloned().unwrap_or(Value::None);
		}
		assert_eq!(levels, DEPTH, "one assembled object per depth");
		assert_eq!(node, Value::None, "the deepest record has no link to follow");
	}

	#[tokio::test]
	async fn an_unbounded_recursion_still_discovering_at_the_cap_raises_the_limit() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		let start = val("link:a").await;
		// One level is pushed per iteration, so a level count above the cap means
		// the walk had not finished.
		let err = run(&start, "link:a.{ name, next: next.@ }", 1, None, 2, false, &ctx)
			.await
			.unwrap_err();
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
			run(&start, "link:a.{ name, next: next.@ }", 1, Some(2), 2, false, &ctx).await.unwrap(),
			val("{ name: 'A', next: { name: 'B', next: link:c } }").await
		);
	}

	#[tokio::test]
	async fn a_non_record_value_reaching_the_marker_is_rejected_during_discovery() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		let start = val("link:a").await;
		// `name.@` feeds a string into the marker, which discovery rejects rather
		// than treating as a node.
		let err =
			run(&start, "link:a.{ name, next: name.@ }", 1, Some(2), SYSTEM_LIMIT, false, &ctx)
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
	async fn control_flow_out_of_the_body_aborts_the_discovery_phase() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		let start = val("link:a").await;
		let base = EvalContext::from_exec_ctx(&ctx);

		// Only the path-elimination signal is swallowed by the discovery loop;
		// every other signal is returned as-is.
		for raise in [Raise::Break, Raise::Continue, Raise::Return, Raise::Error] {
			let path = raise_path(raise);
			let err = evaluate_recurse_iterative(
				&start,
				&path,
				bounds(1, Some(3), SYSTEM_LIMIT),
				&None,
				&ctx,
				base.with_value(&start),
			)
			.await
			.unwrap_err();
			assert!(!is_elimination(&err), "{raise:?} must not be mistaken for path elimination");
			match (raise, &err) {
				(Raise::Break, ControlFlow::Break)
				| (Raise::Continue, ControlFlow::Continue)
				| (Raise::Return, ControlFlow::Return(_))
				| (Raise::Error, ControlFlow::Err(_)) => {}
				(raise, err) => panic!("{raise:?} propagated as {err}"),
			}
		}
	}
}
