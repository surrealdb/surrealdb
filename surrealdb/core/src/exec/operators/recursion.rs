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

		// The parsed `{min..max}` range plus the system limit, resolved once;
		// each strategy derives its cap and limit-error behaviour from this
		// (see `RecursionBounds`).
		let bounds = common::RecursionBounds {
			min: self.min_depth,
			max: self.max_depth,
			system_limit: ctx.ctx().config.exec.idiom_recursion_limit,
		};

		let path = self.path.clone();
		let body = self.body.clone();
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
					bounds,
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
							bounds,
							eval_ctx.with_value(&value),
						)
						.await?
					}
					PhysicalRecurseInstruction::Collect => {
						collect::evaluate_recurse_collect(
							&value,
							&path,
							bounds,
							inclusive,
							eval_ctx.with_value(&value),
						)
						.await?
					}
					PhysicalRecurseInstruction::Path => {
						path::evaluate_recurse_path(
							&value,
							&path,
							bounds,
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
							bounds,
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

#[cfg(test)]
mod tests {
	use surrealdb_types::{SqlFormat, ToSql};

	use super::common::RecursionBounds;
	use super::*;
	use crate::exec::operators::test_util::{
		TestDb, collect as collect_rows, parse_idiom, physical_expr, try_collect, val,
	};
	use crate::exec::planner::Planner;
	use crate::expr::ControlFlow;
	use crate::expr::part::Part;

	// =========================================================================
	// Shared fixtures and helpers
	//
	// The strategy submodules are descendants of this module, so they reach
	// these through `super::super::tests::…`.
	// =========================================================================

	/// The system `idiom_recursion_limit` a context built by `TestDb` carries.
	pub(super) const SYSTEM_LIMIT: u32 = 256;

	/// Graph fixtures every recursion strategy test draws on.
	///
	/// Record links (`link` table, link field `next`, plus a non-record `name`
	/// field to drive the RecordId-enforcement paths):
	///
	/// ```text
	/// chain    a → b → c → d          (d is a dead end)
	/// diamond  x → [y, z], y → [w], z → [w]
	/// cycle    p → q → p
	/// spur     e → [f, g], f → [e]    (a cycle with a branch hanging off it)
	/// selfloop s → s
	/// dag      m → [n, o], n → [o]    (o is reachable at depth 1 and at depth 2)
	/// ```
	///
	/// Graph edges (`node` table, `step` edge table with fixed edge ids so the
	/// adjacency order is deterministic) mirror the chain and the self-loop.
	pub(super) const FIXTURES: &str = "
		INSERT INTO link [
			{ id: link:a, name: 'A', next: link:b },
			{ id: link:b, name: 'B', next: link:c },
			{ id: link:c, name: 'C', next: link:d },
			{ id: link:d, name: 'D' },
			{ id: link:x, name: 'X', next: [link:y, link:z] },
			{ id: link:y, name: 'Y', next: [link:w] },
			{ id: link:z, name: 'Z', next: [link:w] },
			{ id: link:w, name: 'W' },
			{ id: link:p, name: 'P', next: link:q },
			{ id: link:q, name: 'Q', next: link:p },
			{ id: link:e, name: 'E', next: [link:f, link:g] },
			{ id: link:f, name: 'F', next: [link:e] },
			{ id: link:g, name: 'G' },
			{ id: link:s, name: 'S', next: link:s },
			{ id: link:m, name: 'M', next: [link:n, link:o] },
			{ id: link:n, name: 'N', next: [link:o] },
			{ id: link:o, name: 'O' }
		];
		INSERT INTO node [
			{ id: node:a }, { id: node:b }, { id: node:c }, { id: node:d },
			{ id: node:s }
		];
		INSERT RELATION INTO step [
			{ id: step:1, in: node:a, out: node:b },
			{ id: step:2, in: node:b, out: node:c },
			{ id: step:3, in: node:c, out: node:d },
			{ id: step:4, in: node:s, out: node:s }
		];
	";

	/// Compile the tail of `src` into the physical path a `RecursionOp` holds.
	///
	/// `src` is a whole idiom whose first part is the start expression (e.g.
	/// `link:a.next`); that part is dropped and the remainder goes through
	/// `Planner::convert_parts`, which is exactly how the planner builds a
	/// recursion body — including lookup fusion and the auto-inserted flattens.
	pub(super) async fn body_path(src: &str, ctx: &ExecutionContext) -> Vec<Arc<dyn PhysicalExpr>> {
		let mut parts = parse_idiom(src).0;
		assert!(
			matches!(parts.first(), Some(Part::Start(_))),
			"{src:?} should start with a start expression, got {parts:?}"
		);
		parts.remove(0);
		let path = Planner::new(ctx.ctx(), ctx.function_registry())
			.convert_parts(parts)
			.await
			.expect("recursion body should convert");
		assert!(!path.is_empty(), "{src:?} should convert to at least one part");
		path
	}

	/// The body operator the planner extracts from a converted path: the single
	/// embedded operator chain, or `None` when the path exposes none or several.
	pub(super) fn body_operator(path: &[Arc<dyn PhysicalExpr>]) -> Option<Arc<dyn ExecOperator>> {
		let mut embedded: Vec<_> = path
			.iter()
			.flat_map(|p| p.embedded_operators())
			.map(|(_, op)| Arc::clone(op))
			.collect();
		if embedded.len() == 1 {
			embedded.pop()
		} else {
			None
		}
	}

	/// `{min..max}` bounds against an explicit system limit, so a test can reach
	/// the limit-exceeded path without walking 256 levels.
	pub(super) fn bounds(min: u32, max: Option<u32>, system_limit: u32) -> RecursionBounds {
		RecursionBounds {
			min,
			max,
			system_limit,
		}
	}

	/// The typed `exec::Error` carried by a failed `FlowResult`.
	pub(super) fn exec_error(flow: ControlFlow) -> crate::exec::Error {
		match flow {
			ControlFlow::Err(e) => match e.downcast::<crate::exec::Error>() {
				Ok(err) => err,
				Err(other) => panic!("expected an exec::Error, got {other:?}"),
			},
			other => panic!("expected an error, got {other}"),
		}
	}

	/// The control-flow signal a [`RaisePart`] raises when evaluated.
	#[derive(Debug, Clone, Copy)]
	pub(super) enum Raise {
		Break,
		Continue,
		Return,
		Error,
	}

	/// A path part that always raises a control-flow signal, standing in for a
	/// recursion body that returns early or fails.
	#[derive(Debug)]
	pub(super) struct RaisePart(pub(super) Raise);

	impl PhysicalExpr for RaisePart {
		fn name(&self) -> &'static str {
			"Raise"
		}

		fn as_any(&self) -> &dyn std::any::Any {
			self
		}

		fn required_context(&self) -> ContextLevel {
			ContextLevel::Root
		}

		fn evaluate<'a>(
			&'a self,
			_ctx: EvalContext<'a>,
		) -> crate::exec::BoxFut<'a, FlowResult<Value>> {
			Box::pin(async move {
				Err(match self.0 {
					Raise::Break => ControlFlow::Break,
					Raise::Continue => ControlFlow::Continue,
					Raise::Return => ControlFlow::Return(Value::from(7)),
					Raise::Error => ControlFlow::Err(anyhow::anyhow!("body blew up")),
				})
			})
		}

		fn access_mode(&self) -> AccessMode {
			AccessMode::ReadOnly
		}
	}

	impl ToSql for RaisePart {
		fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
			f.push_str("RAISE");
		}
	}

	/// A path of one [`RaisePart`].
	pub(super) fn raise_path(raise: Raise) -> Vec<Arc<dyn PhysicalExpr>> {
		vec![Arc::new(RaisePart(raise)) as Arc<dyn PhysicalExpr>]
	}

	/// A `+shortest` target that resolves to a record id but declares
	/// `ReadWrite`, standing in for a target whose evaluation mutates.
	#[derive(Debug)]
	struct WritingTarget;

	impl PhysicalExpr for WritingTarget {
		fn name(&self) -> &'static str {
			"WritingTarget"
		}

		fn as_any(&self) -> &dyn std::any::Any {
			self
		}

		fn required_context(&self) -> ContextLevel {
			ContextLevel::Root
		}

		fn evaluate<'a>(
			&'a self,
			_ctx: EvalContext<'a>,
		) -> crate::exec::BoxFut<'a, FlowResult<Value>> {
			Box::pin(async move { Ok(Value::None) })
		}

		fn access_mode(&self) -> AccessMode {
			AccessMode::ReadWrite
		}
	}

	impl ToSql for WritingTarget {
		fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
			f.push_str("WRITING_TARGET");
		}
	}

	// =========================================================================
	// RecursionOp — dispatch
	// =========================================================================

	/// Build the operator the planner would build for `path`, with the body
	/// operator extracted the same way.
	fn op(
		path: Vec<Arc<dyn PhysicalExpr>>,
		min: u32,
		max: Option<u32>,
		instruction: PhysicalRecurseInstruction,
		inclusive: bool,
		has_repeat_recurse: bool,
	) -> Arc<dyn ExecOperator> {
		let body = body_operator(&path);
		Arc::new(RecursionOp::new(body, path, min, max, instruction, inclusive, has_repeat_recurse))
	}

	#[tokio::test]
	async fn one_row_out_and_at_most_one_declared_so_buffering_is_skipped() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await.with_current_value(val("link:a").await);
		let plan = op(
			body_path("link:a.next", &ctx).await,
			1,
			Some(2),
			PhysicalRecurseInstruction::Default,
			false,
			false,
		);

		assert!(matches!(plan.cardinality_hint(), CardinalityHint::AtMostOne));
		let rows = collect_rows(&plan, &ctx).await;
		assert_eq!(rows, vec![val("link:c").await], "one batch carrying one recursion result");
	}

	#[tokio::test]
	async fn the_start_node_is_the_contexts_current_value() {
		let db = TestDb::new(FIXTURES).await;
		let base = db.exec_ctx().await;
		let plan = op(
			body_path("link:a.next", &base).await,
			1,
			Some(1),
			PhysicalRecurseInstruction::Default,
			false,
			false,
		);

		let from_a = base.clone().with_current_value(val("link:a").await);
		assert_eq!(collect_rows(&plan, &from_a).await, vec![val("link:b").await]);

		let from_c = base.clone().with_current_value(val("link:c").await);
		assert_eq!(collect_rows(&plan, &from_c).await, vec![val("link:d").await]);

		// No current value seeds the recursion with NONE, which is a dead end
		// at depth 1 and so yields NONE rather than failing.
		assert_eq!(collect_rows(&plan, &base).await, vec![Value::None]);
	}

	#[tokio::test]
	async fn each_instruction_dispatches_to_its_own_strategy() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await.with_current_value(val("link:x").await);
		let path = || async { body_path("link:x.next", &ctx).await };

		let default =
			op(path().await, 1, Some(2), PhysicalRecurseInstruction::Default, false, false);
		assert_eq!(
			collect_rows(&default, &ctx).await,
			// Both diamond branches reach link:w and the default strategy keeps
			// one entry per walk — it flattens, it does not deduplicate.
			vec![val("[link:w, link:w]").await],
			"default returns only the value at the final depth"
		);

		let collect =
			op(path().await, 1, Some(2), PhysicalRecurseInstruction::Collect, false, false);
		assert_eq!(
			collect_rows(&collect, &ctx).await,
			vec![val("[link:y, link:z, link:w]").await],
			"collect returns every node it visited"
		);

		let paths = op(path().await, 1, Some(2), PhysicalRecurseInstruction::Path, false, false);
		assert_eq!(
			collect_rows(&paths, &ctx).await,
			vec![val("[[link:y, link:w], [link:z, link:w]]").await],
			"path returns one array per walk"
		);

		let shortest = op(
			path().await,
			1,
			Some(2),
			PhysicalRecurseInstruction::Shortest {
				target: physical_expr("link:w", &ctx).await,
			},
			false,
			false,
		);
		assert_eq!(
			collect_rows(&shortest, &ctx).await,
			vec![val("[link:y, link:w]").await],
			"shortest returns the first walk that reaches the target"
		);
	}

	#[tokio::test]
	async fn the_repeat_recurse_flag_selects_the_two_phase_evaluator() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await.with_current_value(val("link:a").await);
		let path = || async { body_path("link:a.{ name, next: next.@ }", &ctx).await };

		let iterative =
			op(path().await, 1, Some(2), PhysicalRecurseInstruction::Default, false, true);
		assert_eq!(
			collect_rows(&iterative, &ctx).await,
			vec![val("{ name: 'A', next: { name: 'B', next: link:c } }").await]
		);

		// The `@` marker only works inside the two-phase evaluator: it needs a
		// RecursionCtx, and only that evaluator installs one.
		let without_flag =
			op(path().await, 1, Some(2), PhysicalRecurseInstruction::Default, false, false);
		let err = try_collect(&without_flag, &ctx).await.unwrap_err();
		assert!(matches!(exec_error(err), crate::exec::Error::UnsupportedRepeatRecurse));
	}

	#[tokio::test]
	async fn the_iteration_cap_comes_from_the_context_config() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await.with_current_value(val("link:p").await);
		// An unbounded recursion over a 2-cycle never terminates on its own, so
		// it stops at the system limit the operator read off the context.
		let plan = op(
			body_path("link:p.next", &ctx).await,
			1,
			None,
			PhysicalRecurseInstruction::Default,
			false,
			false,
		);

		let err = try_collect(&plan, &ctx).await.unwrap_err();
		assert!(matches!(
			exec_error(err),
			crate::exec::Error::IdiomRecursionLimitExceeded {
				limit: SYSTEM_LIMIT
			}
		));
	}

	#[tokio::test]
	async fn required_context_is_the_max_over_the_path_and_the_shortest_target() {
		// The executor validates this before execute(), so under-reporting would
		// let a record-dereferencing body run without a transaction.
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;

		let field = op(
			body_path("link:a.next", &ctx).await,
			1,
			Some(1),
			PhysicalRecurseInstruction::Default,
			false,
			false,
		);
		assert_eq!(field.required_context(), ContextLevel::Database);

		// `@` on its own needs nothing, but a Database-level shortest target
		// must still lift the operator's requirement.
		let marker =
			|| vec![Arc::new(crate::exec::parts::RepeatRecursePart) as Arc<dyn PhysicalExpr>];
		let root_only = op(marker(), 1, Some(1), PhysicalRecurseInstruction::Default, false, true);
		assert_eq!(root_only.required_context(), ContextLevel::Root);

		let from_target = op(
			marker(),
			1,
			Some(1),
			PhysicalRecurseInstruction::Shortest {
				target: physical_expr("link:a.name", &ctx).await,
			},
			false,
			true,
		);
		assert_eq!(from_target.required_context(), ContextLevel::Database);
	}

	#[tokio::test]
	async fn access_mode_combines_the_path_and_the_shortest_target() {
		// The access mode picks the transaction type and orders write barriers,
		// so a mutating shortest target must not be reported as read-only.
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		let path = || async { body_path("link:a.next", &ctx).await };

		let read_only =
			op(path().await, 1, Some(1), PhysicalRecurseInstruction::Default, false, false);
		assert_eq!(read_only.access_mode(), AccessMode::ReadOnly);

		let writing = op(
			path().await,
			1,
			Some(1),
			PhysicalRecurseInstruction::Shortest {
				target: Arc::new(WritingTarget),
			},
			false,
			false,
		);
		assert_eq!(writing.access_mode(), AccessMode::ReadWrite);
	}

	#[tokio::test]
	async fn children_expose_the_fused_body_chain_for_explain() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;

		// A graph body fuses into one operator chain, which EXPLAIN prints
		// beneath the Recurse node.
		let graph_path = body_path("node:a->step->node", &ctx).await;
		assert!(body_operator(&graph_path).is_some());
		let graph = op(graph_path, 1, Some(1), PhysicalRecurseInstruction::Default, false, false);
		assert_eq!(graph.children().len(), 1);

		// A record-link body has no embedded operator, so the Recurse node is a
		// leaf in EXPLAIN.
		let link_path = body_path("link:a.next", &ctx).await;
		assert!(body_operator(&link_path).is_none());
		let link = op(link_path, 1, Some(1), PhysicalRecurseInstruction::Default, false, false);
		assert!(link.children().is_empty());
	}

	#[tokio::test]
	async fn depth_and_instruction_attrs_are_what_explain_and_to_sql_read() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await;
		let path = || async { body_path("link:a.next", &ctx).await };

		let depth_of = |plan: &Arc<dyn ExecOperator>| {
			plan.attrs()
				.into_iter()
				.find(|(k, _)| k == "depth")
				.map(|(_, v)| v)
				.expect("depth attr")
		};

		let cases: [(u32, Option<u32>, &str); 6] = [
			(1, Some(1), "1"),
			(3, Some(3), "3"),
			(1, Some(4), "1..4"),
			(1, None, "1.."),
			(2, Some(5), "2..5"),
			(2, None, "2.."),
		];
		for (min, max, expected) in cases {
			let plan =
				op(path().await, min, max, PhysicalRecurseInstruction::Default, false, false);
			assert_eq!(depth_of(&plan), expected, "depth attr for {min}..{max:?}");
		}

		let instr_of = |plan: &Arc<dyn ExecOperator>| {
			plan.attrs()
				.into_iter()
				.find(|(k, _)| k == "instruction")
				.map(|(_, v)| v)
				.expect("instruction attr")
		};
		let target = physical_expr("link:w", &ctx).await;
		let instructions = [
			(PhysicalRecurseInstruction::Default, "default"),
			(PhysicalRecurseInstruction::Collect, "collect"),
			(PhysicalRecurseInstruction::Path, "path"),
			(
				PhysicalRecurseInstruction::Shortest {
					target,
				},
				"shortest",
			),
		];
		for (instruction, expected) in instructions {
			let plan = op(path().await, 1, Some(1), instruction, false, false);
			assert_eq!(instr_of(&plan), expected);
			// A non-repeat path carries no `pattern` attr.
			assert!(plan.attrs().iter().all(|(k, _)| k != "pattern"));
		}

		let tree = op(path().await, 1, Some(1), PhysicalRecurseInstruction::Default, false, true);
		assert!(tree.attrs().contains(&("pattern".to_string(), "tree".to_string())));
	}

	#[tokio::test]
	async fn control_flow_from_the_body_propagates_out_of_the_stream() {
		let db = TestDb::new(FIXTURES).await;
		let ctx = db.exec_ctx().await.with_current_value(val("link:a").await);

		let plan = |raise: Raise| {
			op(raise_path(raise), 1, Some(2), PhysicalRecurseInstruction::Default, false, false)
		};

		assert!(matches!(
			try_collect(&plan(Raise::Break), &ctx).await.unwrap_err(),
			ControlFlow::Break
		));
		assert!(matches!(
			try_collect(&plan(Raise::Continue), &ctx).await.unwrap_err(),
			ControlFlow::Continue
		));
		match try_collect(&plan(Raise::Return), &ctx).await.unwrap_err() {
			ControlFlow::Return(v) => assert_eq!(v, Value::from(7)),
			other => panic!("expected RETURN, got {other}"),
		}
		match try_collect(&plan(Raise::Error), &ctx).await.unwrap_err() {
			ControlFlow::Err(e) => assert_eq!(e.to_string(), "body blew up"),
			other => panic!("expected an error, got {other}"),
		}
	}
}
