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

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;

use crate::exec::parts::recurse::PhysicalRecurseInstruction;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{
	AccessMode, CardinalityHint, CombineAccessModes, ContextLevel, ExecOperator, ExecutionContext,
	FlowResult, OperatorMetrics, ValueBatch, ValueBatchStream, monitor_stream,
};
use crate::val::Value;

mod collect;
mod common;
mod default;
mod path;
mod repeat;
mod shortest;

// Re-export for use by parts::recurse (RepeatRecursePart evaluation).
pub(crate) use repeat::evaluate_repeat_recurse;

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

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::AtMostOne
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
		let system_limit = ctx.ctx().config().limits.idiom_recursion_limit as u32;
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
				repeat::evaluate_recurse_iterative(
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
						default::evaluate_recurse_default(
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
						collect::evaluate_recurse_collect(
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
						path::evaluate_recurse_path(
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
						shortest::evaluate_recurse_shortest(
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
