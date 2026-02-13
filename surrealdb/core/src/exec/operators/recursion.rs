//! Recursion operator for the streaming execution engine.
//!
//! This operator implements bounded/unbounded recursive graph traversal with
//! various collection strategies. It wraps the inner operator chain (typically
//! a fused lookup chain like `GraphEdgeScan(->person, GraphEdgeScan(->knows, CVS))`)
//! and repeatedly evaluates the body path until depth bounds, dead ends, or
//! cycles are reached.
//!
//! ## Stack safety
//!
//! The four iterative strategies (Default, Collect, Path, Shortest) are fully
//! loop-based and use no stack recursion. They are safe at any depth.
//!
//! The RepeatRecurse (`@`) tree-building strategy requires true recursion
//! because the `@` marker is embedded arbitrarily deep inside Destructure
//! fields. Converting it to a loop would require rewriting the entire
//! PhysicalExpr evaluation into a continuation-passing style. The recursive
//! calls use `Box::pin` to allocate each async state machine on the heap,
//! keeping per-level stack usage minimal. Depth is bounded by `max_depth`
//! (capped at the system `IDIOM_RECURSION_LIMIT`, default 256).
//!
//! ## EXPLAIN output
//!
//! ```text
//! Recurse [ctx: Db] [depth: 3, instruction: default]
//! └────> GraphEdgeScan [ctx: Db] [direction: ->, tables: person, output: TargetId]
//!        └────> GraphEdgeScan [ctx: Db] [direction: ->, tables: knows, output: TargetId]
//!               └────> CurrentValueSource [ctx: Rt]
//! ```

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;

use crate::cnf::IDIOM_RECURSION_LIMIT;
use crate::exec::parts::recurse::{PhysicalRecurseInstruction, value_hash};
use crate::exec::parts::{clean_iteration, evaluate_physical_path, get_final, is_final};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr, RecursionCtx};
use crate::exec::{
	AccessMode, CombineAccessModes, ContextLevel, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, ValueBatch, ValueBatchStream, monitor_stream,
};
use crate::expr::ControlFlow;
use crate::val::Value;

/// Sentinel error type used to signal path elimination during recursion.
///
/// When a RepeatRecurse (`@`) finds that all recursive results are dead ends
/// and the current depth is below `min_depth`, it raises this signal.
/// The signal propagates through the Destructure (skipping remaining field
/// evaluation) and is caught by `evaluate_recurse_with_plan`, which returns
/// `Value::None` -- allowing the parent's `clean_iteration` to filter
/// the eliminated sub-tree from the results.
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
			(1, Some(max)) if max == 1 => "1".to_string(),
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
				let rec_ctx = RecursionCtx {
					path: &path,
					max_depth: Some(max_depth),
					min_depth,
					depth: 0,
				};
				evaluate_recurse_with_plan(
					&value,
					&path,
					eval_ctx.with_value(&value).with_recursion_ctx(rec_ctx),
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
// RepeatRecurse evaluation functions
// ============================================================================

/// Evaluate a recursion that contains RepeatRecurse markers.
///
/// This performs a single evaluation of the path on the current value.
/// The actual recursion happens through RepeatRecurse callbacks within
/// the path evaluation (e.g., inside Destructure aliased fields).
///
/// The recursion context is set in EvalContext so that RepeatRecurse
/// handlers can re-invoke this function with incremented depth.
pub(crate) fn evaluate_recurse_with_plan<'a>(
	value: &'a Value,
	path: &'a [Arc<dyn PhysicalExpr>],
	ctx: EvalContext<'a>,
) -> crate::exec::BoxFut<'a, FlowResult<Value>> {
	Box::pin(async move {
		let rec_ctx = ctx.recursion_ctx.as_ref().expect("recursion context must be set");
		let max_depth = rec_ctx.max_depth.unwrap_or(256);

		// Check depth limit before evaluating
		if rec_ctx.depth >= max_depth {
			return Ok(value.clone());
		}

		// Check if the value is a dead end
		if is_final(value) {
			// get_final already returns the correct terminal value
			// ([] for arrays, Null for Null, None for everything else).
			return Ok(get_final(value));
		}

		// Evaluate the path once on the current value.
		// RepeatRecurse markers within the path will recursively call back
		// into evaluate_recurse_with_plan via evaluate_repeat_recurse.
		//
		// If a nested RepeatRecurse detects a dead-end sub-tree below
		// min_depth it raises PathEliminationSignal. Catch it here so
		// the parent clean_iteration can filter this branch out.
		match evaluate_physical_path(value, path, ctx.with_value(value)).await {
			Ok(v) => Ok(v),
			Err(ControlFlow::Err(ref e)) if e.downcast_ref::<PathEliminationSignal>().is_some() => {
				Ok(Value::None)
			}
			Err(other) => Err(other),
		}
	})
}

/// Handle the RepeatRecurse (@) marker during path evaluation.
///
/// This reads the recursion context from EvalContext and re-invokes
/// the recursion evaluator on the current value. For Array values,
/// each element is processed individually to build the recursive tree.
pub(crate) fn evaluate_repeat_recurse<'a>(
	value: &'a Value,
	ctx: EvalContext<'a>,
) -> crate::exec::BoxFut<'a, FlowResult<Value>> {
	Box::pin(async move {
		let rec_ctx = match &ctx.recursion_ctx {
			Some(rc) => *rc,
			None => {
				// RepeatRecurse outside recursion context is an error
				return Err(crate::err::Error::UnsupportedRepeatRecurse.into());
			}
		};

		// Increment depth for the recursive call
		let next_ctx = RecursionCtx {
			depth: rec_ctx.depth + 1,
			..rec_ctx
		};

		let next_depth = next_ctx.depth;

		match value {
			// For arrays, process each element individually and collect results.
			Value::Array(arr) => {
				let mut results = Vec::with_capacity(arr.len());
				for elem in arr.iter() {
					let elem_ctx = ctx.with_recursion_ctx(next_ctx);
					let result = evaluate_recurse_with_plan(elem, next_ctx.path, elem_ctx).await?;
					results.push(result);
				}

				// Apply clean_iteration: filter out dead-end values (None, Null,
				// all-None arrays) and flatten.
				let result = clean_iteration(Value::Array(results.into()));

				// Path elimination: when ALL recursive results are dead ends
				// (cleaned result is final) and we haven't reached min_depth,
				// signal elimination so the parent can prune this branch.
				if is_final(&result) && next_depth < rec_ctx.min_depth {
					return Err(ControlFlow::Err(anyhow::Error::new(PathEliminationSignal)));
				}

				Ok(result)
			}
			// For single values, recurse directly
			_ => {
				let elem_ctx = ctx.with_recursion_ctx(next_ctx);
				let result = evaluate_recurse_with_plan(value, next_ctx.path, elem_ctx).await?;

				// Path elimination for single-value dead ends
				if is_final(&result) && next_depth < rec_ctx.min_depth {
					return Err(ControlFlow::Err(anyhow::Error::new(PathEliminationSignal)));
				}

				Ok(result)
			}
		}
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

		// Check termination conditions
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

		for value in frontier {
			let result = evaluate_physical_path(&value, path, ctx.with_value(&value)).await?;

			// Destructure directly into the inner Vec to avoid
			// iterator + collect overhead.
			let values = match result {
				Value::Array(arr) => arr.0,
				Value::None | Value::Null => continue,
				other => vec![other],
			};

			for v in values {
				if is_final(&v) {
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

		for mut current_path in active_paths {
			let current_value = current_path.last().unwrap_or(start);
			let result =
				evaluate_physical_path(current_value, path, ctx.with_value(current_value)).await?;

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

			// Single pass: extend paths for non-final values, detect dead ends.
			// On the last non-final value we move current_path instead of cloning
			// to save one allocation per branch point.
			let mut non_final = values.into_iter().filter(|v| !is_final(v)).peekable();

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
		let level_size = queue.len();

		for _ in 0..level_size {
			let (current, current_path) = queue.pop_front().expect("queue checked non-empty");

			let result = evaluate_physical_path(&current, path, ctx.with_value(&current)).await?;

			// Destructure directly into the inner Vec.
			let values = match result {
				Value::Array(arr) => arr.0,
				Value::None | Value::Null => continue,
				other => vec![other],
			};

			for v in values {
				if is_final(&v) {
					continue;
				}

				// Check if we found the target (only if min_depth reached)
				if depth + 1 >= min_depth && &v == target {
					let mut final_path = current_path.clone();
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
