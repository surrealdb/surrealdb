//! Recursion parts -- `{min..max}`, `{..}`, and `@` (RepeatRecurse).

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql};

use super::{clean_iteration, evaluate_physical_path, get_final, is_final};
use crate::cnf::IDIOM_RECURSION_LIMIT;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr, RecursionCtx};
use crate::exec::{AccessMode, CombineAccessModes, ContextLevel};
use crate::expr::{ControlFlow, FlowResult};
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

// ============================================================================
// PhysicalRecurseInstruction -- shared enum
// ============================================================================

/// Instruction for how to handle recursion results.
#[derive(Debug, Clone)]
pub enum PhysicalRecurseInstruction {
	/// Default: return the final values after recursion
	Default,

	/// Collect all unique nodes encountered during traversal
	Collect,

	/// Return all paths as arrays of arrays
	Path,

	/// Find shortest path to a target node
	Shortest {
		/// Expression that evaluates to the target RecordId
		target: Arc<dyn PhysicalExpr>,
	},
}

// ============================================================================
// RecursePart
// ============================================================================

/// Recursive graph traversal - `{min..max}`.
///
/// Implements bounded/unbounded recursion with various collection strategies:
/// - Default: Follow path until bounds or dead end, return final value
/// - Collect: Gather all unique nodes encountered during BFS traversal
/// - Path: Return all paths as arrays of arrays
/// - Shortest: Find shortest path to a target node using BFS
#[derive(Debug, Clone)]
pub struct RecursePart {
	/// Minimum recursion depth (default 1)
	pub min_depth: u32,

	/// Maximum recursion depth (None = unbounded up to system limit)
	pub max_depth: Option<u32>,

	/// The path to traverse at each recursion step
	pub path: Vec<Arc<dyn PhysicalExpr>>,

	/// The recursion instruction (how to collect results)
	pub instruction: PhysicalRecurseInstruction,

	/// Whether to include the starting node in results
	pub inclusive: bool,

	/// Whether the inner path contains RepeatRecurse markers.
	/// When true, the recursion uses single-step evaluation where
	/// tree building is handled by the RepeatRecurse callbacks.
	pub has_repeat_recurse: bool,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for RecursePart {
	fn name(&self) -> &'static str {
		"Recurse"
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

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		let value = ctx.current_value.cloned().unwrap_or(Value::None);

		// Get the system recursion limit
		let system_limit = *IDIOM_RECURSION_LIMIT as u32;
		let max_depth = self.max_depth.unwrap_or(system_limit).min(system_limit);

		// When the path contains RepeatRecurse markers, use single-step evaluation.
		// The tree is built through RepeatRecurse callbacks within the path evaluation.
		if self.has_repeat_recurse {
			let rec_ctx = RecursionCtx {
				path: &self.path,
				max_depth: Some(max_depth),
				min_depth: self.min_depth,
				depth: 0,
			};
			return evaluate_recurse_with_plan(&value, &self.path, ctx.with_recursion_ctx(rec_ctx))
				.await;
		}

		match &self.instruction {
			PhysicalRecurseInstruction::Default => {
				evaluate_recurse_default(
					&value,
					&self.path,
					self.min_depth,
					max_depth,
					self.max_depth.is_some(),
					ctx,
				)
				.await
			}
			PhysicalRecurseInstruction::Collect => {
				evaluate_recurse_collect(
					&value,
					&self.path,
					self.min_depth,
					max_depth,
					self.inclusive,
					ctx,
				)
				.await
			}
			PhysicalRecurseInstruction::Path => {
				evaluate_recurse_path(
					&value,
					&self.path,
					self.min_depth,
					max_depth,
					self.inclusive,
					ctx,
				)
				.await
			}
			PhysicalRecurseInstruction::Shortest {
				target,
			} => {
				let target_value = target.evaluate(ctx.clone()).await?;
				evaluate_recurse_shortest(
					&value,
					&target_value,
					&self.path,
					self.min_depth,
					max_depth,
					self.inclusive,
					ctx,
				)
				.await
			}
		}
	}

	fn references_current_value(&self) -> bool {
		true
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
}

impl ToSql for RecursePart {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(".{");
		if self.min_depth > 1 {
			f.push_str(&self.min_depth.to_string());
		}
		f.push_str("..");
		if let Some(max) = self.max_depth {
			f.push_str(&max.to_string());
		}

		match &self.instruction {
			PhysicalRecurseInstruction::Default => {}
			PhysicalRecurseInstruction::Collect => f.push_str("+collect"),
			PhysicalRecurseInstruction::Path => f.push_str("+path"),
			PhysicalRecurseInstruction::Shortest {
				..
			} => f.push_str("+shortest=..."),
		}

		if self.inclusive {
			f.push_str("+inclusive");
		}

		f.push('}');
	}
}

// ============================================================================
// RepeatRecursePart -- `@`
// ============================================================================

/// RepeatRecurse marker - `@`.
///
/// When encountered during path evaluation inside a recursion context,
/// this part re-invokes the recursion evaluator on the current value
/// with incremented depth.
#[derive(Debug, Clone)]
pub struct RepeatRecursePart;

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for RepeatRecursePart {
	fn name(&self) -> &'static str {
		"RepeatRecurse"
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		let value = ctx.current_value.cloned().unwrap_or(Value::None);
		evaluate_repeat_recurse(&value, ctx).await
	}

	fn references_current_value(&self) -> bool {
		true
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}
}

impl ToSql for RepeatRecursePart {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push('@');
	}
}

// ============================================================================
// Recursion evaluation functions
// ============================================================================

/// Evaluate a recursion that contains RepeatRecurse markers.
///
/// This performs a single evaluation of the path on the current value.
/// The actual recursion happens through RepeatRecurse callbacks within
/// the path evaluation (e.g., inside Destructure aliased fields).
///
/// The recursion context is set in EvalContext so that RepeatRecurse
/// handlers can re-invoke this function with incremented depth.
fn evaluate_recurse_with_plan<'a>(
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

		// Check if the value is final (dead end)
		if is_final(value) {
			return Ok(clean_iteration(get_final(value)));
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
fn evaluate_repeat_recurse<'a>(
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
				// all-None arrays) and flatten.  This matches the old compute
				// path's `clean_iteration` call inside the Destructure plan.
				let result = clean_iteration(Value::Array(results.into()));

				// Path elimination: when ALL recursive results are dead ends
				// (cleaned result is final) and we haven't reached min_depth,
				// signal elimination so the parent can prune this branch.
				// Uses strict less-than (`<`) to match the old compute path's
				// check: `rec.iterated < rec.min`.
				if is_final(&result) && next_depth < rec_ctx.min_depth {
					return Err(ControlFlow::Err(anyhow::Error::new(PathEliminationSignal)));
				}

				Ok(result)
			}
			// For single values, recurse directly
			_ => {
				let elem_ctx = ctx.with_recursion_ctx(next_ctx);
				let result = evaluate_recurse_with_plan(value, next_ctx.path, elem_ctx).await?;

				// Path elimination for single-value dead ends (e.g. a field
				// that resolves to None because the record doesn't have it).
				if is_final(&result) && next_depth < rec_ctx.min_depth {
					return Err(ControlFlow::Err(anyhow::Error::new(PathEliminationSignal)));
				}

				Ok(result)
			}
		}
	})
}

/// Default recursion: keep following the path until bounds or dead end.
///
/// Returns the final value after traversing the path up to max_depth times,
/// or None if min_depth is not reached before termination.
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
		// Evaluate the path on the current value
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
				// Use get_final to preserve the value's type:
				// Array → [], Null → Null, _ → None
				Ok(get_final(&next))
			};
		}

		current = next;
	}

	// If the user did NOT specify an explicit max bound, and we exhausted
	// the system recursion limit, that means the recursion was unbounded
	// and never resolved → error.  When the user DID specify max (e.g.
	// `{..256}`), reaching that depth is a normal successful termination,
	// matching the old compute path's `if let Some(max) = rec.max` branch.
	if !user_specified_max && depth >= system_limit {
		return Err(crate::err::Error::IdiomRecursionLimitExceeded {
			limit: system_limit,
		}
		.into());
	}

	// Reached max depth
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

	// Include starting node if inclusive
	if inclusive {
		collected.push(start.clone());
		seen.insert(value_hash(start));
	}

	let mut depth = 0u32;

	while depth < max_depth && !frontier.is_empty() {
		let mut next_frontier = Vec::new();

		for value in frontier {
			let result = evaluate_physical_path(&value, path, ctx.with_value(&value)).await?;

			// Process result (may be single value or array)
			let values = match result {
				Value::Array(arr) => arr.into_iter().collect::<Vec<_>>(),
				Value::None | Value::Null => continue,
				other => vec![other],
			};

			for v in values {
				if is_final(&v) {
					continue;
				}

				let hash = value_hash(&v);
				if seen.insert(hash) {
					// Only collect nodes discovered at or beyond
					// min_depth. Nodes below the threshold still
					// need to be traversed (they are intermediaries)
					// but should not appear in the output.
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

		for current_path in active_paths {
			let current_value = current_path.last().unwrap_or(start);
			let result =
				evaluate_physical_path(current_value, path, ctx.with_value(current_value)).await?;

			let values = match result {
				Value::Array(arr) => arr.into_iter().collect::<Vec<_>>(),
				Value::None | Value::Null => {
					// Dead end - path is complete if min depth reached
					if depth >= min_depth && !current_path.is_empty() {
						completed_paths.push(Value::Array(current_path.into()));
					}
					continue;
				}
				other => vec![other],
			};

			if values.is_empty() || values.iter().all(is_final) {
				// Dead end
				if depth >= min_depth && !current_path.is_empty() {
					completed_paths.push(Value::Array(current_path.into()));
				}
			} else {
				// Extend path with each new value
				for v in values {
					if is_final(&v) {
						continue;
					}
					let mut new_path = current_path.clone();
					new_path.push(v);
					next_paths.push(new_path);
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

	// BFS with path tracking
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

			let values = match result {
				Value::Array(arr) => arr.into_iter().collect::<Vec<_>>(),
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

// ============================================================================
// Helpers
// ============================================================================

/// Helper function to create a hash for value deduplication in graph traversal.
///
/// Optimized for the common case of RecordId values, which are the primary
/// target for cycle detection in graph traversal.
pub(crate) fn value_hash(value: &Value) -> u64 {
	use std::hash::{Hash, Hasher};
	let mut hasher = std::collections::hash_map::DefaultHasher::new();

	match value {
		Value::RecordId(rid) => {
			0u8.hash(&mut hasher);
			rid.hash(&mut hasher);
		}
		Value::None => {
			1u8.hash(&mut hasher);
		}
		Value::Null => {
			2u8.hash(&mut hasher);
		}
		Value::Bool(b) => {
			3u8.hash(&mut hasher);
			b.hash(&mut hasher);
		}
		Value::String(s) => {
			4u8.hash(&mut hasher);
			s.hash(&mut hasher);
		}
		Value::Number(n) => {
			5u8.hash(&mut hasher);
			n.to_string().hash(&mut hasher);
		}
		Value::Uuid(u) => {
			6u8.hash(&mut hasher);
			u.0.hash(&mut hasher);
		}
		Value::Array(arr) => {
			7u8.hash(&mut hasher);
			arr.len().hash(&mut hasher);
			for (i, v) in arr.iter().enumerate() {
				if i >= 8 {
					break;
				}
				value_hash(v).hash(&mut hasher);
			}
		}
		Value::Object(obj) => {
			8u8.hash(&mut hasher);
			obj.len().hash(&mut hasher);
			for (i, (k, v)) in obj.iter().enumerate() {
				if i >= 8 {
					break;
				}
				k.hash(&mut hasher);
				value_hash(v).hash(&mut hasher);
			}
		}
		_ => {
			255u8.hash(&mut hasher);
			format!("{:?}", value).hash(&mut hasher);
		}
	}

	hasher.finish()
}
