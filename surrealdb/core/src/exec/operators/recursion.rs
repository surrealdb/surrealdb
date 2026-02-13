//! Recursion operator for the streaming execution engine.
//!
//! This operator implements bounded/unbounded recursive graph traversal with
//! various collection strategies. It wraps the inner operator chain (typically
//! a fused lookup chain like `GraphEdgeScan(->person, GraphEdgeScan(->knows, CVS))`)
//! and repeatedly evaluates the body path until depth bounds, dead ends, or
//! cycles are reached.
//!
//! ## RecordId enforcement
//!
//! Recursion is intended purely for RecordId graph traversal. The
//! `is_recursion_target` helper enforces this: only `RecordId` values (and
//! arrays containing them) are valid recursion targets. All other types
//! (String, Number, Object, Uuid, etc.) are treated as terminal and stop
//! recursion at that branch.
//!
//! ## Body-operator optimization
//!
//! When a `body` operator is available (the fused lookup chain extracted for
//! EXPLAIN display), the RepeatRecurse discovery phase executes it directly
//! to discover target RecordIds. This avoids fetching full documents for
//! non-recursive destructure fields (e.g., `name` in
//! `{ name, knows: ->knows->person.@ }`), eliminating redundant I/O.
//!
//! ## Stack safety
//!
//! All strategies are fully iterative and use no stack recursion:
//!
//! - **Default, Collect, Path, Shortest**: Loop-based, safe at any depth.
//! - **RepeatRecurse (`@`) tree-building**: Uses a two-phase iterative approach (forward BFS
//!   discovery + backward bottom-up assembly). In the discovery phase, `@` writes its inputs to a
//!   shared sink and returns immediately (or the body operator is executed directly when
//!   available). In the assembly phase, `@` does a cache lookup for pre-computed results. Neither
//!   phase uses stack recursion.
//!
//! ## EXPLAIN output
//!
//! ```text
//! Recurse [ctx: Db] [depth: 3, instruction: default]
//! └────> GraphEdgeScan [ctx: Db] [direction: ->, tables: person, output: TargetId]
//!        └────> GraphEdgeScan [ctx: Db] [direction: ->, tables: knows, output: TargetId]
//!               └────> CurrentValueSource [ctx: Rt]
//! ```

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, stream};

use crate::cnf::IDIOM_RECURSION_LIMIT;
use crate::exec::parts::recurse::{PhysicalRecurseInstruction, value_hash};
use crate::exec::parts::{clean_iteration, evaluate_physical_path, get_final, is_final};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr, RecursionCtx};
use crate::exec::{
	AccessMode, BoxFut, CombineAccessModes, ContextLevel, ExecOperator, ExecutionContext,
	FlowResult, OperatorMetrics, ValueBatch, ValueBatchStream, monitor_stream,
};
use crate::expr::ControlFlow;
use crate::val::Value;

/// Maximum number of concurrent path evaluations per depth level.
/// Limits parallelism to avoid overwhelming the KV layer while still
/// allowing progress when individual evaluations block on I/O.
const RECURSION_CONCURRENCY: usize = 16;

/// Sentinel error type used to signal path elimination during recursion.
///
/// When a RepeatRecurse (`@`) in assembly mode finds that all results are
/// dead ends and the current depth is below `min_depth`, it raises this
/// signal. The signal propagates through the Destructure (skipping
/// remaining field evaluation) and is caught by the assembly loop in
/// `evaluate_recurse_iterative`, which stores `Value::None` -- allowing
/// the parent level's `clean_iteration` to filter the eliminated sub-tree.
#[derive(Debug)]
struct PathEliminationSignal;

impl std::fmt::Display for PathEliminationSignal {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "path elimination signal")
	}
}

impl std::error::Error for PathEliminationSignal {}

/// Recursion operator -- bounded/unbounded recursive graph traversal.
///
/// Implements four collection strategies:
/// - Default: Follow path until bounds or dead end, return final value
/// - Collect: Gather all unique nodes encountered during BFS traversal
/// - Path: Return all paths as arrays of arrays
/// - Shortest: Find shortest path to a target node using BFS
///
/// The operator holds both:
/// - An optional body operator chain (for EXPLAIN display as `children()`)
/// - The full PhysicalExpr path (for execution via `evaluate_physical_path`)
#[derive(Debug, Clone)]
pub struct RecursionOp {
	/// The inner operator chain for the recursion body.
	/// For non-@ paths, this is the fused lookup chain extracted from
	/// the path parts' `embedded_operators()`.
	/// Used by `children()` to display the operator tree in EXPLAIN.
	pub(crate) body: Option<Arc<dyn ExecOperator>>,

	/// The full PhysicalExpr path for execution.
	/// Used by the iteration loop to evaluate each recursion step.
	pub(crate) path: Vec<Arc<dyn PhysicalExpr>>,

	/// Whether the path contains RepeatRecurse (@) markers.
	/// When true, uses single-step evaluation with callback-based tree building.
	pub(crate) has_repeat_recurse: bool,

	/// Minimum recursion depth (default 1)
	pub(crate) min_depth: u32,

	/// Maximum recursion depth (None = unbounded up to system limit)
	pub(crate) max_depth: Option<u32>,

	/// The recursion instruction (how to collect results)
	pub(crate) instruction: PhysicalRecurseInstruction,

	/// Whether to include the starting node in results
	pub(crate) inclusive: bool,

	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl RecursionOp {
	pub(crate) fn new(
		body: Option<Arc<dyn ExecOperator>>,
		path: Vec<Arc<dyn PhysicalExpr>>,
		min_depth: u32,
		max_depth: Option<u32>,
		instruction: PhysicalRecurseInstruction,
		inclusive: bool,
		has_repeat_recurse: bool,
	) -> Self {
		Self {
			body,
			path,
			has_repeat_recurse,
			min_depth,
			max_depth,
			instruction,
			inclusive,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	/// Format the depth range for EXPLAIN attrs.
	fn depth_display(&self) -> String {
		match (self.min_depth, self.max_depth) {
			(1, Some(1)) => "1".to_string(),
			(min, Some(max)) if min == max => format!("{}", min),
			(1, Some(max)) => format!("1..{}", max),
			(1, None) => "1..".to_string(),
			(min, Some(max)) => format!("{}..{}", min, max),
			(min, None) => format!("{}..", min),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for RecursionOp {
	fn name(&self) -> &'static str {
		"Recurse"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![("depth".to_string(), self.depth_display())];

		let instr_name = match &self.instruction {
			PhysicalRecurseInstruction::Default => "default",
			PhysicalRecurseInstruction::Collect => "collect",
			PhysicalRecurseInstruction::Path => "path",
			PhysicalRecurseInstruction::Shortest {
				..
			} => "shortest",
		};
		attrs.push(("instruction".to_string(), instr_name.to_string()));

		if self.has_repeat_recurse {
			attrs.push(("pattern".to_string(), "tree".to_string()));
		}

		attrs
	}

	fn required_context(&self) -> ContextLevel {
		let path_ctx =
			self.path.iter().map(|p| p.required_context()).max().unwrap_or(ContextLevel::Root);

		let instruction_ctx = match &self.instruction {
			PhysicalRecurseInstruction::Default
			| PhysicalRecurseInstruction::Collect
			| PhysicalRecurseInstruction::Path => ContextLevel::Root,
			PhysicalRecurseInstruction::Shortest {
				target,
			} => target.required_context(),
		};

		path_ctx.max(instruction_ctx)
	}

	fn access_mode(&self) -> AccessMode {
		let path_mode = self.path.iter().map(|p| p.access_mode()).combine_all();

		let instruction_mode = match &self.instruction {
			PhysicalRecurseInstruction::Default
			| PhysicalRecurseInstruction::Collect
			| PhysicalRecurseInstruction::Path => AccessMode::ReadOnly,
			PhysicalRecurseInstruction::Shortest {
				target,
			} => target.access_mode(),
		};

		path_mode.combine(instruction_mode)
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		match &self.body {
			Some(body) => vec![body],
			None => vec![],
		}
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		// Clone context and owned data into the async block.
		let ctx = ctx.clone();
		let value = ctx.current_value().cloned().unwrap_or(Value::None);

		// Resolve the effective max depth once.
		let system_limit = *IDIOM_RECURSION_LIMIT as u32;
		let max_depth = self.max_depth.unwrap_or(system_limit).min(system_limit);

		let path = self.path.clone();
		let body = self.body.clone();
		let min_depth = self.min_depth;
		let user_specified_max = self.max_depth.is_some();
		let inclusive = self.inclusive;
		let instruction = self.instruction.clone();
		let has_repeat_recurse = self.has_repeat_recurse;
		let metrics = Arc::clone(&self.metrics);

		// This operator always yields exactly one batch, so use
		// stream::once instead of a generator to avoid state-machine overhead.
		let fut = async move {
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);

			let result = if has_repeat_recurse {
				evaluate_recurse_iterative(
					&value,
					&path,
					min_depth,
					max_depth,
					&body,
					&ctx,
					eval_ctx.with_value(&value),
				)
				.await?
			} else {
				match &instruction {
					PhysicalRecurseInstruction::Default => {
						evaluate_recurse_default(
							&value,
							&path,
							min_depth,
							max_depth,
							user_specified_max,
							eval_ctx.with_value(&value),
						)
						.await?
					}
					PhysicalRecurseInstruction::Collect => {
						evaluate_recurse_collect(
							&value,
							&path,
							min_depth,
							max_depth,
							inclusive,
							eval_ctx.with_value(&value),
						)
						.await?
					}
					PhysicalRecurseInstruction::Path => {
						evaluate_recurse_path(
							&value,
							&path,
							min_depth,
							max_depth,
							inclusive,
							eval_ctx.with_value(&value),
						)
						.await?
					}
					PhysicalRecurseInstruction::Shortest {
						target,
					} => {
						let target_value = target.evaluate(eval_ctx.with_value(&value)).await?;
						evaluate_recurse_shortest(
							&value,
							&target_value,
							&path,
							min_depth,
							max_depth,
							inclusive,
							eval_ctx.with_value(&value),
						)
						.await?
					}
				}
			};

			Ok(ValueBatch {
				values: vec![result],
			})
		};

		Ok(monitor_stream(Box::pin(stream::once(fut)), "Recurse", &metrics))
	}
}

// ============================================================================
// RepeatRecurse evaluation function
// ============================================================================

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
					arr.iter().filter(|v| !is_final(v) && is_recursion_target(v)).cloned().collect()
				}
				v if !is_final(v) && is_recursion_target(v) => vec![v.clone()],
				_ => vec![],
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

// ============================================================================
// Iterative RepeatRecurse evaluation (two-phase BFS, no stack recursion)
// ============================================================================

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
async fn evaluate_recurse_iterative(
	start: &Value,
	path: &[Arc<dyn PhysicalExpr>],
	min_depth: u32,
	max_depth: u32,
	body: &Option<Arc<dyn ExecOperator>>,
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
			let all_discovered = eval_buffered(futs).await?;
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

// ============================================================================
// RecordId enforcement
// ============================================================================

/// Check if a value is a valid recursion target.
///
/// Recursion is intended purely for RecordId traversal. Only `RecordId`
/// values and arrays containing at least one `RecordId` are valid targets.
/// All other types (String, Number, Object, Uuid, etc.) are treated as
/// terminal and stop recursion at that branch.
fn is_recursion_target(value: &Value) -> bool {
	match value {
		Value::RecordId(_) => true,
		Value::Array(arr) => arr.iter().any(is_recursion_target),
		_ => false,
	}
}

// ============================================================================
// Concurrent evaluation helpers
// ============================================================================

/// Evaluate a batch of futures with bounded concurrency.
///
/// When fewer than 2 futures are provided, runs them sequentially to avoid
/// stream combinator overhead. Otherwise, uses `buffered(RECURSION_CONCURRENCY)`
/// to poll up to N futures concurrently -- when one blocks on I/O, others
/// make progress.
///
/// Short-circuits on the first error via `try_collect`.
async fn eval_buffered<'a, T: 'a>(futures: Vec<BoxFut<'a, FlowResult<T>>>) -> FlowResult<Vec<T>> {
	if futures.len() < 2 {
		let mut results = Vec::with_capacity(futures.len());
		for fut in futures {
			results.push(fut.await?);
		}
		Ok(results)
	} else {
		stream::iter(futures).buffered(RECURSION_CONCURRENCY).try_collect().await
	}
}

/// Like [`eval_buffered`], but collects all results without short-circuiting.
///
/// Used when callers need to inspect each result individually (e.g. to
/// handle [`PathEliminationSignal`] errors as non-fatal).
async fn eval_buffered_all<'a>(
	futures: Vec<BoxFut<'a, FlowResult<Value>>>,
) -> Vec<FlowResult<Value>> {
	if futures.len() < 2 {
		let mut results = Vec::with_capacity(futures.len());
		for fut in futures {
			results.push(fut.await);
		}
		results
	} else {
		stream::iter(futures).buffered(RECURSION_CONCURRENCY).collect().await
	}
}

/// Extract valid recursion target values from a single batch result value.
///
/// Flattens arrays and filters to only `RecordId` values (and arrays
/// containing them) that are valid for continued graph traversal.
fn collect_discovery_targets(v: Value, out: &mut Vec<Value>) {
	match v {
		Value::Array(arr) => {
			for inner in arr.0 {
				if !is_final(&inner) && is_recursion_target(&inner) {
					out.push(inner);
				}
			}
		}
		v if !is_final(&v) && is_recursion_target(&v) => {
			out.push(v);
		}
		_ => {}
	}
}

/// Discover recursion targets via the body operator for a single input value.
///
/// Executes the fused lookup chain and collects all valid `RecordId` targets
/// from the resulting stream. Returns a boxed future for use with
/// [`eval_buffered`].
fn discover_body_targets<'a>(
	body_op: &'a Arc<dyn ExecOperator>,
	body_ctx: ExecutionContext,
) -> BoxFut<'a, FlowResult<Vec<Value>>> {
	Box::pin(async move {
		let mut discovered = Vec::new();
		let mut body_stream = body_op.execute(&body_ctx)?;
		while let Some(batch_result) = body_stream.next().await {
			let batch = batch_result?;
			for v in batch.values {
				collect_discovery_targets(v, &mut discovered);
			}
		}
		Ok(discovered)
	})
}

// ============================================================================
// Iterative recursion strategies (loop-based, no stack recursion)
// ============================================================================

/// Default recursion: keep following the path until bounds or dead end.
///
/// Returns the final value after traversing the path up to max_depth times,
/// or None if min_depth is not reached before termination.
///
/// Fully iterative -- uses a while loop with no recursive calls.
async fn evaluate_recurse_default(
	start: &Value,
	path: &[Arc<dyn PhysicalExpr>],
	min_depth: u32,
	max_depth: u32,
	user_specified_max: bool,
	ctx: EvalContext<'_>,
) -> FlowResult<Value> {
	let system_limit = *IDIOM_RECURSION_LIMIT as u32;
	let mut current = start.clone();
	let mut depth = 0u32;

	while depth < max_depth {
		let next = evaluate_physical_path(&current, path, ctx.with_value(&current)).await?;

		depth += 1;

		// Clean up dead ends from array results
		let next = clean_iteration(next);

		// Check termination conditions.
		// Non-RecordId values are treated as terminal -- recursion is
		// intended purely for record graph traversal.
		if is_final(&next) || !is_recursion_target(&next) || next == current {
			// Reached a dead end, non-RecordId value, or cycle.
			// Use `depth > min_depth` (not `>=`) because the current iteration
			// produced a dead end, so we've only completed (depth - 1) successful
			// traversals.
			return if depth > min_depth {
				Ok(current)
			} else {
				Ok(get_final(&next))
			};
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

/// Collect recursion: gather all unique nodes encountered during BFS traversal.
///
/// Uses breadth-first search to collect all reachable nodes, respecting
/// depth bounds and avoiding cycles via hash-based deduplication.
///
/// Fully iterative -- frontier-based BFS loop.
async fn evaluate_recurse_collect(
	start: &Value,
	path: &[Arc<dyn PhysicalExpr>],
	min_depth: u32,
	max_depth: u32,
	inclusive: bool,
	ctx: EvalContext<'_>,
) -> FlowResult<Value> {
	let mut collected = Vec::new();
	let mut seen: HashSet<u64> = HashSet::new();
	let mut frontier = vec![start.clone()];

	if inclusive {
		collected.push(start.clone());
		seen.insert(value_hash(start));
	}

	let mut depth = 0u32;

	while depth < max_depth && !frontier.is_empty() {
		let mut next_frontier = Vec::new();

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
				// Non-RecordId values are treated as terminal --
				// recursion is intended purely for record graph traversal.
				if is_final(&v) || !is_recursion_target(&v) {
					continue;
				}

				let hash = value_hash(&v);
				if seen.insert(hash) {
					// Only collect nodes discovered at or beyond min_depth.
					// Nodes below the threshold are traversed but not emitted.
					if depth + 1 >= min_depth {
						collected.push(v.clone());
					}
					next_frontier.push(v);
				}
			}
		}

		frontier = next_frontier;
		depth += 1;
	}

	Ok(Value::Array(collected.into()))
}

/// Path recursion: return all paths as arrays of arrays.
///
/// Tracks all possible paths through the graph, returning each complete
/// path as an array. Paths terminate at dead ends or max depth.
///
/// Fully iterative -- BFS loop over active paths.
async fn evaluate_recurse_path(
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
	for path in active_paths {
		if !path.is_empty() && depth >= min_depth {
			completed_paths.push(Value::Array(path.into()));
		}
	}

	Ok(Value::Array(completed_paths.into()))
}

/// Shortest path recursion: find the shortest path to a target node using BFS.
///
/// Returns the first (shortest) path found to the target, or None if the
/// target is not reachable within max_depth.
///
/// Fully iterative -- level-based BFS loop.
async fn evaluate_recurse_shortest(
	start: &Value,
	target: &Value,
	path: &[Arc<dyn PhysicalExpr>],
	min_depth: u32,
	max_depth: u32,
	inclusive: bool,
	ctx: EvalContext<'_>,
) -> FlowResult<Value> {
	let mut seen: HashSet<u64> = HashSet::new();

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
