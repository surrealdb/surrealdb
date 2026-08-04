//! Sequence operator with deferred planning.
//!
//! The SequencePlan operator executes a sequence of expressions (a Block)
//! in order, threading the execution context through to enable LET bindings
//! to inform subsequent expression planning. This mirrors how the top-level
//! script executor handles multiple statements.
//!
//! When planning fails with `PlannerUnsupported` or `PlannerUnimplemented`,
//! the sequence falls back to the legacy `Expr::compute` path, similar to how
//! the top-level executor handles unplanned expressions.

use std::sync::Arc;

use futures::stream;
use surrealdb_types::{SqlFormat, ToSql};

use crate::ctx::FrozenContext;
use crate::err::{EngineError, Error};
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::plan_or_compute::{
	block_required_context, collect_stream, legacy_compute, planning_txn,
};
use crate::exec::planner::try_plan_expr;
use crate::exec::{
	AccessMode, BoxFut, CardinalityHint, Error as ExecError, ExecOperator, FlowResult,
	OperatorMetrics, ValueBatch, ValueBatchStream,
};
use crate::expr::{Block, ControlFlow, ControlFlowExt, Expr};
use crate::val::{Array, Value};

/// Sequence operator with deferred planning.
///
/// Stores the original block and plans each statement just before
/// execution, threading the execution context through to enable
/// LET bindings to inform subsequent statement planning.
///
/// Example where deferred planning helps:
/// ```surql
/// {
///     LET $table = "users";
///     SELECT * FROM type::table($table);  -- Planner knows $table = "users"
/// }
/// ```
#[derive(Debug)]
pub struct SequencePlan {
	/// The original block containing Expr values
	pub block: Block,
	/// Metrics for EXPLAIN ANALYZE
	pub(crate) metrics: Arc<OperatorMetrics>,
	/// Expression-nesting depth recorded when this operator was planned. The
	/// deferred per-statement planning below is seeded with it so re-entry nodes
	/// (eval/UDF) keep counting toward `max_computation_depth`.
	plan_depth: u32,
}

impl SequencePlan {
	pub(crate) fn new(block: Block, plan_depth: u32) -> Self {
		Self {
			block,
			metrics: Arc::new(OperatorMetrics::new()),
			plan_depth,
		}
	}
}
impl ExecOperator for SequencePlan {
	fn name(&self) -> &'static str {
		"Sequence"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("statements".to_string(), self.block.0.len().to_string())]
	}

	fn required_context(&self) -> ContextLevel {
		// Derive the required context from the block's expressions
		block_required_context(&self.block)
	}

	fn access_mode(&self) -> AccessMode {
		if self.block.read_only() {
			AccessMode::ReadOnly
		} else {
			AccessMode::ReadWrite
		}
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::AtMostOne
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let block = self.block.clone();
		// Statements are re-planned one re-entry deeper than this operator, so the
		// depth count continues at `plan_depth + 1` toward `max_computation_depth`.
		let depth = self.plan_depth + 1;
		let initial_ctx = ctx.clone();

		let stream = stream::once(async move {
			let (result, _) = execute_block_with_context(&block, &initial_ctx, depth).await?;
			Ok(ValueBatch {
				values: vec![result],
			})
		});

		Ok(Box::pin(stream))
	}

	fn mutates_context(&self) -> bool {
		self.block.0.iter().any(|expr| matches!(expr, Expr::Let(_)))
	}

	fn output_context<'a>(
		&'a self,
		input: &'a ExecutionContext,
	) -> BoxFut<'a, crate::expr::FlowResult<ExecutionContext>> {
		Box::pin(async move {
			let (_result, final_ctx) =
				execute_block_with_context(&self.block, input, self.plan_depth + 1).await.map_err(
					|ctrl| match ctrl {
						ControlFlow::Break | ControlFlow::Continue | ControlFlow::Return(_) => {
							// BREAK/CONTINUE/RETURN at top-level LET binding context is invalid
							ControlFlow::Err(anyhow::Error::new(ExecError::InvalidControlFlow))
						}
						// Unchanged, not stringified: a write conflict raised in
						// here has to stay downcastable or the transactor will
						// not retry it, and a cancelled or timed-out block has
						// to stay recognisable as such.
						ControlFlow::Err(e) => ControlFlow::Err(e),
					},
				)?;
			Ok(final_ctx)
		})
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn is_scalar(&self) -> bool {
		true
	}
}

/// Execute a block and return both the result and the final execution context.
///
/// Returns `FlowResult` to allow BREAK/CONTINUE/RETURN to propagate through
/// block expressions nested inside FOR loops.
async fn execute_block_with_context(
	block: &Block,
	initial_ctx: &ExecutionContext,
	depth: u32,
) -> crate::expr::FlowResult<(Value, ExecutionContext)> {
	// Empty block returns NONE
	if block.0.is_empty() {
		return Ok((Value::None, initial_ctx.clone()));
	}

	let mut current_ctx = initial_ctx.clone();
	let mut result = Value::None;

	for expr in block.0.iter() {
		// Check for cancellation between statements
		if current_ctx.cancellation().is_cancelled() {
			return Err(ControlFlow::Err(anyhow::anyhow!(EngineError::QueryCancelled)));
		}

		let frozen_ctx = Arc::clone(current_ctx.ctx());
		let auth = current_ctx.options().map(|o| Arc::clone(&o.auth));

		// Try to plan the expression with current context, continuing the depth
		// count so re-entry nodes inside the block stay bounded. The transaction
		// lookup is fallible and, per `try_plan_expr!`, runs only if the macro
		// takes its planning branch.
		match try_plan_expr!(
			expr,
			&frozen_ctx,
			current_ctx.function_registry(),
			planning_txn(&current_ctx).map_err(|e| ControlFlow::Err(e.into()))?,
			auth,
			depth
		) {
			Ok(plan) => {
				if plan.mutates_context() {
					current_ctx = plan.output_context(&current_ctx).await?;
					result = Value::None;
				} else {
					let stream = plan.execute(&current_ctx)?;
					let values = collect_stream(stream).await?;

					result = if plan.is_scalar() {
						values.into_iter().next().unwrap_or(Value::None)
					} else {
						Value::Array(Array(values))
					};
				}
			}
			Err(Error::Exec(
				e @ (ExecError::PlannerUnsupported(_) | ExecError::PlannerUnimplemented(_)),
			)) => {
				match &e {
					ExecError::PlannerUnimplemented(msg) => {
						tracing::warn!("PlannerUnimplemented fallback in sequence: {msg}");
					}
					ExecError::PlannerUnsupported(msg) => {
						tracing::debug!("PlannerUnsupported fallback in sequence: {msg}",);
					}
					_ => {}
				}
				// Fallback to legacy compute path, continuing the depth count from
				// `depth` rather than handing it a fresh budget.
				let (opt, frozen) = legacy_context_for_fallback(&current_ctx)
					.context("Legacy compute fallback context unavailable")?;
				let opt = opt.with_dive_consumed(depth);

				if let Expr::Let(set_stmt) = expr {
					let value = legacy_compute(&set_stmt.what, &frozen, &opt, None).await?;

					// Update context with the new variable
					current_ctx = current_ctx.with_param(set_stmt.name.clone(), value.clone());
					result = Value::None;
				} else {
					result = legacy_compute(expr, &frozen, &opt, None).await?;
				}
			}
			Err(e) => return Err(ControlFlow::Err(e.into())),
		}
	}

	Ok((result, current_ctx))
}

/// Options and frozen context for [`legacy_compute`] when the planner falls back.
///
/// Always clones from [`ExecutionContext`]: LET bindings from the planned path
/// (`with_param` / `output_context`) must be visible to legacy `Expr::compute`
/// (issue #7131).
fn legacy_context_for_fallback(
	exec_ctx: &ExecutionContext,
) -> Result<(crate::dbs::Options, FrozenContext), Error> {
	let options = exec_ctx.options().ok_or_else(|| {
		EngineError::Internal("Options not available for legacy compute fallback".into())
	})?;
	// Block write side effects when this sequence is evaluated inside a
	// PERMISSIONS predicate (signalled by `skip_fetch_perms`), so a predicate
	// cannot mutate data via the legacy compute fallback (GHSA-66r2-5gwj-gxm2).
	let options = if exec_ctx.root().skip_fetch_perms {
		options.new_for_permission_predicate()
	} else {
		options.clone()
	};
	Ok((options, Arc::clone(exec_ctx.ctx())))
}

impl ToSql for SequencePlan {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.block.fmt_sql(f, fmt);
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::exec::operators::test_util::{
		TestDb, collect, drain_err, parse_expr, root_ctx, try_collect,
	};
	use crate::expr::Literal;
	use crate::kvs::TransactionType;
	#[tokio::test]
	async fn a_context_without_a_transaction_yields_an_error_not_a_panic() {
		// Each statement is planned at execute time, which needs a transaction to
		// resolve catalog definitions. The executor always attaches one; a context
		// assembled without one has to fail rather than panic.
		let plan = SequencePlan::new(Block(vec![Expr::Literal(Literal::Integer(1))]), 0);
		let err = drain_err(&plan, &root_ctx()).await;
		assert!(
			format!("{err}").contains("requires a transaction"),
			"expected a missing-transaction error, got: {err}"
		);
	}
	/// Build a `SequencePlan` over the given statements the way the planner does:
	/// the block is stored unplanned and each statement is planned just before it
	/// runs, so a `LET` can inform the planning of what follows.
	fn plan(statements: &[&str]) -> Arc<dyn ExecOperator> {
		let block = Block(statements.iter().copied().map(parse_expr).collect());
		Arc::new(SequencePlan::new(block, 0))
	}

	/// A database-level context over a read transaction.
	///
	/// `SequencePlan` re-plans each statement at execute time, and the planner
	/// reads the transaction out of the context unconditionally, so a
	/// transaction-less root context cannot drive this operator.
	async fn db_ctx() -> ExecutionContext {
		TestDb::new("").await.exec_ctx().await
	}

	/// The message carried by a `THROW`n error.
	fn thrown(flow: ControlFlow) -> String {
		match flow {
			ControlFlow::Err(e) => match e.downcast_ref::<ExecError>() {
				Some(ExecError::Thrown(msg)) => msg.clone(),
				_ => panic!("expected a THROWn error, got {e:?}"),
			},
			other => panic!("expected an error, got {other}"),
		}
	}

	#[tokio::test]
	async fn a_sequence_emits_exactly_one_row_carrying_the_last_statements_value() {
		let ctx = db_ctx().await;
		// Sequence declares `is_scalar()` and `CardinalityHint::AtMostOne`: the
		// earlier statements' values are discarded, not emitted.
		assert_eq!(collect(&plan(&["1", "2", "3"]), &ctx).await, vec![Value::from(3i64)]);
	}

	#[tokio::test]
	async fn an_empty_block_emits_a_single_none_row() {
		let ctx = db_ctx().await;
		let op: Arc<dyn ExecOperator> = Arc::new(SequencePlan::new(Block(Vec::new()), 0));
		assert_eq!(collect(&op, &ctx).await, vec![Value::None]);
	}

	#[tokio::test]
	async fn statements_run_in_order_and_a_later_one_sees_an_earlier_binding() {
		let ctx = db_ctx().await;
		assert_eq!(collect(&plan(&["LET $x = 5", "$x + 1"]), &ctx).await, vec![Value::from(6i64)]);
	}

	#[tokio::test]
	async fn a_binding_is_available_to_the_planning_of_later_statements() {
		// The reason planning is deferred per statement: the table name in the
		// SELECT below is only knowable once the LET has run.
		let db = TestDb::new("CREATE users:tobie SET name = 'Tobie'").await;
		let ctx = db.exec_ctx().await;
		let rows =
			collect(&plan(&[r#"LET $t = "users""#, "SELECT name FROM type::table($t)"]), &ctx)
				.await;
		assert_eq!(rows.len(), 1, "a sequence emits one row");
		let Value::Array(selected) = &rows[0] else {
			panic!("a non-scalar last statement is wrapped in an array, got {:?}", rows[0]);
		};
		assert_eq!(selected.len(), 1, "the SELECT should have found the seeded record");
	}

	#[tokio::test]
	async fn a_binding_as_the_last_statement_leaves_the_sequence_emitting_none() {
		let ctx = db_ctx().await;
		// A context-mutating statement produces no value of its own, so it resets
		// the running result rather than carrying the previous one forward.
		assert_eq!(collect(&plan(&["1", "LET $x = 5"]), &ctx).await, vec![Value::None]);
	}

	#[tokio::test]
	async fn a_return_in_a_middle_statement_short_circuits_the_rest() {
		let ctx = db_ctx().await;
		let flow = try_collect(&plan(&["1", "RETURN 2", r#"THROW "ran past the RETURN""#]), &ctx)
			.await
			.expect_err("RETURN must propagate");
		match flow {
			ControlFlow::Return(v) => assert_eq!(v, Value::from(2i64)),
			other => panic!("expected RETURN, got {other}"),
		}
	}

	#[tokio::test]
	async fn loop_signals_propagate_out_so_an_enclosing_for_loop_sees_them() {
		let ctx = db_ctx().await;
		// A block nested inside a FOR loop must be able to BREAK or CONTINUE the
		// loop, so these signals travel out of the sequence rather than being
		// swallowed — and they stop the remaining statements.
		let flow = try_collect(&plan(&["BREAK", r#"THROW "ran past the BREAK""#]), &ctx)
			.await
			.expect_err("BREAK must propagate");
		assert!(matches!(flow, ControlFlow::Break), "got {flow}");

		let flow = try_collect(&plan(&["CONTINUE", r#"THROW "ran past the CONTINUE""#]), &ctx)
			.await
			.expect_err("CONTINUE must propagate");
		assert!(matches!(flow, ControlFlow::Continue), "got {flow}");
	}

	#[tokio::test]
	async fn an_error_in_a_middle_statement_stops_the_sequence() {
		let ctx = db_ctx().await;
		let flow =
			try_collect(&plan(&["1", r#"THROW "boom""#, r#"THROW "ran past the failure""#]), &ctx)
				.await
				.expect_err("the error must propagate");
		assert_eq!(thrown(flow), "boom");
	}

	#[tokio::test]
	async fn access_mode_is_readwrite_when_any_statement_writes() {
		// The executor picks the transaction type from the plan's access mode, so
		// a write anywhere in the block has to be reported by the whole sequence.
		assert_eq!(plan(&["1", "2"]).access_mode(), AccessMode::ReadOnly);
		assert_eq!(plan(&["1", "CREATE foo"]).access_mode(), AccessMode::ReadWrite);
		assert_eq!(plan(&["CREATE foo", "1"]).access_mode(), AccessMode::ReadWrite);
	}

	#[tokio::test]
	async fn required_context_is_the_maximum_across_statements() {
		// The executor validates the declared context level before execution, so
		// under-reporting would let a statement run without a namespace/database.
		assert_eq!(plan(&["1", "2"]).required_context(), ContextLevel::Root);
		assert_eq!(plan(&["1", "INFO FOR NS"]).required_context(), ContextLevel::Namespace);
		assert_eq!(plan(&["1", "SELECT * FROM foo"]).required_context(), ContextLevel::Database);
		assert_eq!(plan(&["SELECT * FROM foo", "1"]).required_context(), ContextLevel::Database);
	}

	#[tokio::test]
	async fn mutates_context_is_true_only_when_the_block_binds_a_parameter() {
		// This is what makes the executor ask for `output_context()` so following
		// statements in the enclosing script see the block's bindings.
		assert!(!plan(&["1", "2"]).mutates_context());
		assert!(plan(&["1", "LET $x = 5"]).mutates_context());
	}

	#[tokio::test]
	async fn output_context_publishes_the_blocks_bindings() {
		let ctx = db_ctx().await;
		let op = plan(&["LET $x = 5", "LET $y = $x + 1"]);
		let out = op.output_context(&ctx).await.expect("the block should succeed");
		assert_eq!(out.value("x"), Some(&Value::from(5i64)));
		assert_eq!(out.value("y"), Some(&Value::from(6i64)));
		// The input context is untouched; bindings only travel forward.
		assert!(ctx.value("x").is_none());
	}

	#[tokio::test]
	async fn output_context_rejects_a_control_flow_signal_from_the_block() {
		let ctx = db_ctx().await;
		// `output_context` is the binding path, where there is no loop to break and
		// no caller to return to, so any of the three signals is invalid.
		for statement in ["BREAK", "CONTINUE", "RETURN 1"] {
			let err = plan(&[statement])
				.output_context(&ctx)
				.await
				.err()
				.unwrap_or_else(|| panic!("{statement} is not valid at a binding site"));
			let ControlFlow::Err(err) = err else {
				panic!("{statement} must be reported as an error, not passed on as a signal");
			};
			assert!(
				matches!(err.downcast_ref::<ExecError>(), Some(ExecError::InvalidControlFlow)),
				"expected InvalidControlFlow for {statement}, got {err:?}"
			);
		}
	}

	#[tokio::test]
	async fn output_context_propagates_an_error_downcastable() {
		let ctx = db_ctx().await;
		// The error must not be flattened: the transactor needs to recognise a
		// write conflict, and cancellation/timeout must stay classified.
		let ctrl = plan(&[r#"THROW "boom""#])
			.output_context(&ctx)
			.await
			.expect_err("the error must propagate");
		let ControlFlow::Err(err) = ctrl else {
			panic!("a THROW is an error, not a control-flow signal");
		};
		assert!(
			matches!(err.downcast_ref::<ExecError>(), Some(ExecError::Thrown(msg)) if msg == "boom"),
			"expected the original Thrown error, got {err:?}"
		);
	}

	#[tokio::test]
	async fn the_legacy_compute_fallback_sees_bindings_made_on_the_planned_path() {
		// CREATE has no streaming plan, so the sequence falls back to legacy
		// compute for it. The fallback context is cloned from the execution
		// context so the preceding LET, applied on the planned path, is visible.
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
		let rows = collect(&plan(&["LET $n = 42", "CREATE ONLY foo:1 SET n = $n"]), &ctx).await;
		assert_eq!(rows.len(), 1);
		let Value::Object(created) = &rows[0] else {
			panic!("CREATE ONLY yields a single object, got {:?}", rows[0]);
		};
		assert_eq!(created.get("n"), Some(&Value::from(42i64)));
	}

	#[tokio::test]
	async fn the_legacy_compute_fallback_cannot_write_from_inside_a_permission_predicate() {
		// SECURITY: a stored PERMISSIONS expression runs with enforcement disabled
		// (`skip_fetch_perms`), so the fallback must strip write capability —
		// otherwise a permission check could mutate data unauthorized.
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;

		// Sanity: the same statement succeeds when this is not a predicate.
		let rows = collect(&plan(&["CREATE ONLY foo:1 SET n = 1"]), &ctx).await;
		assert_eq!(rows.len(), 1);

		let ExecutionContext::Database(mut inner) = ctx else {
			panic!("exec_ctx_as builds a Database context");
		};
		inner.ns_ctx.root.skip_fetch_perms = true;
		let predicate_ctx = ExecutionContext::Database(inner);

		let flow = try_collect(&plan(&["CREATE ONLY foo:2 SET n = 1"]), &predicate_ctx)
			.await
			.expect_err("a write inside a permission predicate must be refused");
		let ControlFlow::Err(e) = flow else {
			panic!("expected an error, got {flow}");
		};
		assert!(
			matches!(e.downcast_ref::<ExecError>(), Some(ExecError::PermissionPredicateSideEffect)),
			"expected PermissionPredicateSideEffect, got {e:?}"
		);
	}
}
