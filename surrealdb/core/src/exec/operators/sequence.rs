//! Block operator: a sequence of compiled statements, run in order.
//!
//! [`SequencePlan`] runs each statement against a context threaded from the
//! previous one, so a `LET` binds for every statement that follows it, and
//! yields the last statement's value. This mirrors how the top-level executor
//! handles a script's statements. The bindings stay inside the block: a
//! block is a value to whatever encloses it, never a binding step.

use std::sync::Arc;

use futures::stream;
use surrealdb_types::{SqlFormat, ToSql};

use crate::err::EngineError;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::collect_shaped_value;
use crate::exec::{
	AccessMode, CardinalityHint, CombineAccessModes, ExecOperator, FlowResult, OperatorMetrics,
	OutputShape, ValueBatch, ValueBatchStream,
};
use crate::expr::ControlFlow;
use crate::val::Value;

/// A block `{ s1; s2; … }`: its statements compiled up front, run in order.
///
/// Every statement is compiled when the block is, against the bindings that
/// are in scope at that point; a `LET` inside the block is a runtime binding
/// the later statements read through `$name`, never a plan-time substitution.
/// The context produced by a context-mutating statement is the context the
/// following statements run in, and it is dropped with the block: the block
/// does not mutate the context it was given, so a `LET` inside it is invisible
/// to the enclosing block, `FOR` body or statement, and a block nested in a
/// block is a value step of the outer one like any other statement.
///
/// Emits exactly one row: the last statement's value, or `NONE` for an empty
/// block or a block whose last statement is a binding.
#[derive(Debug)]
pub struct SequencePlan {
	/// The compiled statements, in order.
	pub stmts: Vec<Arc<dyn ExecOperator>>,
	/// Metrics for EXPLAIN ANALYZE
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl SequencePlan {
	pub(crate) fn new(stmts: Vec<Arc<dyn ExecOperator>>) -> Self {
		Self {
			stmts,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

impl ExecOperator for SequencePlan {
	fn name(&self) -> &'static str {
		"Sequence"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("statements".to_string(), self.stmts.len().to_string())]
	}

	fn required_context(&self) -> ContextLevel {
		self.stmts.iter().map(|s| s.required_context()).max().unwrap_or(ContextLevel::Root)
	}

	fn access_mode(&self) -> AccessMode {
		self.stmts.iter().map(|s| s.access_mode()).combine_all()
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::AtMostOne
	}

	/// Run the block and emit its value.
	///
	/// A control-flow signal raised by a statement travels out unchanged: a
	/// `BREAK` or `CONTINUE` belongs to the loop enclosing the block, and a
	/// `RETURN` to the statement boundary above it. Errors likewise pass
	/// through with their type intact, so a write conflict stays retryable and
	/// a cancelled or timed-out block stays recognisable as such.
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let stmts = self.stmts.clone();
		let initial_ctx = ctx.clone();

		let stream = stream::once(async move {
			let result = run_sequence(&stmts, &initial_ctx).await?;
			Ok(ValueBatch::new(vec![result]))
		});

		Ok(Box::pin(stream))
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		self.stmts.iter().collect()
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn output_shape(&self) -> OutputShape {
		OutputShape::Scalar
	}
}

/// Run the statements in order and return the last value.
///
/// A statement that mutates the context (`LET`) contributes its context to the
/// statements after it and resets the running value to `NONE`; any other
/// statement — a nested block included — runs in the current context and its
/// value, shaped by its declared [`OutputShape`], becomes the running value.
/// The context the block ends in goes no further than this call. Control flow
/// signals propagate to the caller.
async fn run_sequence(
	stmts: &[Arc<dyn ExecOperator>],
	initial_ctx: &ExecutionContext,
) -> FlowResult<Value> {
	let mut current_ctx = initial_ctx.clone();
	let mut result = Value::None;

	for stmt in stmts {
		if current_ctx.cancellation().is_cancelled() {
			return Err(ControlFlow::Err(anyhow::anyhow!(EngineError::QueryCancelled)));
		}

		if stmt.mutates_context() {
			current_ctx = stmt.output_context(&current_ctx).await?;
			result = Value::None;
		} else {
			let shape = stmt.output_shape();
			let stream = stmt.execute(&current_ctx)?;
			result = collect_shaped_value(stream, shape).await?;
		}
	}

	Ok(result)
}

impl ToSql for SequencePlan {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("{ ");
		for (i, stmt) in self.stmts.iter().enumerate() {
			if i > 0 {
				f.push_str("; ");
			}
			f.push_str(stmt.name());
		}
		f.push_str(" }");
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::Error as ExecError;
	use crate::exec::operators::test_util::{TestDb, collect, plan_statement, try_collect};
	use crate::kvs::TransactionType;

	/// Build a `SequencePlan` over the given statements the way the planner
	/// does: every statement is compiled up front, against `ctx`.
	async fn plan(statements: &[&str], ctx: &ExecutionContext) -> Arc<dyn ExecOperator> {
		let mut stmts = Vec::with_capacity(statements.len());
		for src in statements {
			stmts.push(plan_statement(src, ctx).await);
		}
		Arc::new(SequencePlan::new(stmts))
	}

	/// A database-level context over a read transaction.
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
		assert_eq!(
			collect(&plan(&["1", "2", "3"], &ctx).await, &ctx).await,
			vec![Value::from(3i64)]
		);
	}

	#[tokio::test]
	async fn an_empty_block_emits_a_single_none_row() {
		let ctx = db_ctx().await;
		let op: Arc<dyn ExecOperator> = Arc::new(SequencePlan::new(Vec::new()));
		assert_eq!(collect(&op, &ctx).await, vec![Value::None]);
	}

	#[tokio::test]
	async fn statements_run_in_order_and_a_later_one_sees_an_earlier_binding() {
		let ctx = db_ctx().await;
		assert_eq!(
			collect(&plan(&["LET $x = 5", "$x + 1"], &ctx).await, &ctx).await,
			vec![Value::from(6i64)]
		);
	}

	#[tokio::test]
	async fn a_binding_is_read_at_runtime_by_a_later_statement() {
		// `$t` is bound by the block itself, so the SELECT is compiled with it
		// as a runtime lookup; the source resolves it at execution time from
		// the context the LET produced.
		let db = TestDb::new("CREATE users:tobie SET name = 'Tobie'").await;
		let ctx = db.exec_ctx().await;
		let rows = collect(
			&plan_statement(r#"{ LET $t = "users"; SELECT name FROM type::table($t) }"#, &ctx)
				.await,
			&ctx,
		)
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
		assert_eq!(collect(&plan(&["1", "LET $x = 5"], &ctx).await, &ctx).await, vec![Value::None]);
	}

	#[tokio::test]
	async fn a_return_in_a_middle_statement_short_circuits_the_rest() {
		let ctx = db_ctx().await;
		let flow = try_collect(
			&plan(&["1", "RETURN 2", r#"THROW "ran past the RETURN""#], &ctx).await,
			&ctx,
		)
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
		let flow =
			try_collect(&plan(&["BREAK", r#"THROW "ran past the BREAK""#], &ctx).await, &ctx)
				.await
				.expect_err("BREAK must propagate");
		assert!(matches!(flow, ControlFlow::Break), "got {flow}");

		let flow =
			try_collect(&plan(&["CONTINUE", r#"THROW "ran past the CONTINUE""#], &ctx).await, &ctx)
				.await
				.expect_err("CONTINUE must propagate");
		assert!(matches!(flow, ControlFlow::Continue), "got {flow}");
	}

	#[tokio::test]
	async fn an_error_in_a_middle_statement_stops_the_sequence() {
		let ctx = db_ctx().await;
		let flow = try_collect(
			&plan(&["1", r#"THROW "boom""#, r#"THROW "ran past the failure""#], &ctx).await,
			&ctx,
		)
		.await
		.expect_err("the error must propagate");
		assert_eq!(thrown(flow), "boom");
	}

	#[tokio::test]
	async fn access_mode_is_readwrite_when_any_statement_writes() {
		// The executor picks the transaction type from the plan's access mode, so
		// a write anywhere in the block has to be reported by the whole sequence.
		// A write statement has no logical form, so the write is a call to a
		// function whose body writes.
		let db = TestDb::new("DEFINE FUNCTION fn::w() { CREATE foo; };").await;
		let ctx = db.exec_ctx().await;
		assert_eq!(plan(&["1", "2"], &ctx).await.access_mode(), AccessMode::ReadOnly);
		assert_eq!(plan(&["1", "fn::w()"], &ctx).await.access_mode(), AccessMode::ReadWrite);
		assert_eq!(plan(&["fn::w()", "1"], &ctx).await.access_mode(), AccessMode::ReadWrite);
	}

	#[tokio::test]
	async fn required_context_is_the_maximum_across_statements() {
		// The executor validates the declared context level before execution, so
		// under-reporting would let a statement run without a namespace/database.
		let ctx = db_ctx().await;
		assert_eq!(plan(&["1", "2"], &ctx).await.required_context(), ContextLevel::Root);
		assert_eq!(
			plan(&["1", "INFO FOR NS"], &ctx).await.required_context(),
			ContextLevel::Namespace
		);
		assert_eq!(
			plan(&["1", "SELECT * FROM foo"], &ctx).await.required_context(),
			ContextLevel::Database
		);
		assert_eq!(
			plan(&["SELECT * FROM foo", "1"], &ctx).await.required_context(),
			ContextLevel::Database
		);
	}

	#[tokio::test]
	async fn a_block_never_publishes_its_bindings() {
		// An enclosing block, FOR body or statement threads a new context only
		// through an operator whose `mutates_context()` is true. A block keeps
		// it false whatever it contains, which is what confines its LETs to
		// the block and makes a nested block a value step of the outer one.
		let ctx = db_ctx().await;
		assert!(!plan(&["1", "2"], &ctx).await.mutates_context());
		assert!(!plan(&["1", "LET $x = 5"], &ctx).await.mutates_context());
		let out = plan(&["LET $x = 5", "LET $y = $x + 1"], &ctx)
			.await
			.output_context(&ctx)
			.await
			.expect("the block should succeed");
		assert!(out.value("x").is_none());
		assert!(out.value("y").is_none());
	}

	#[tokio::test]
	async fn a_nested_block_is_a_value_step_whose_bindings_stay_inside_it() {
		let ctx = db_ctx().await;
		// The inner block's value is the running value of the outer one, and
		// the `$x` it bound is unbound for the statement after it.
		let inner = plan(&["LET $x = 1", "$x + 1"], &ctx).await;
		let outer: Arc<dyn ExecOperator> = Arc::new(SequencePlan::new(vec![Arc::clone(&inner)]));
		assert_eq!(collect(&outer, &ctx).await, vec![Value::from(2i64)]);
		let outer: Arc<dyn ExecOperator> =
			Arc::new(SequencePlan::new(vec![inner, plan_statement("$x", &ctx).await]));
		assert_eq!(collect(&outer, &ctx).await, vec![Value::None]);
		// A `LET` in the inner block does not rebind the outer block's `$y`.
		let inner = plan(&["LET $y = 6"], &ctx).await;
		let outer: Arc<dyn ExecOperator> = Arc::new(SequencePlan::new(vec![
			plan_statement("LET $y = 5", &ctx).await,
			inner,
			plan_statement("$y", &ctx).await,
		]));
		assert_eq!(collect(&outer, &ctx).await, vec![Value::from(5i64)]);
	}

	#[tokio::test]
	async fn an_error_propagates_downcastable() {
		let ctx = db_ctx().await;
		// The error must not be flattened: the transactor needs to recognise a
		// write conflict, and cancellation/timeout must stay classified.
		let ctrl = try_collect(&plan(&["LET $x = 1", r#"THROW "boom""#], &ctx).await, &ctx)
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
	async fn a_writing_call_sees_bindings_made_earlier_in_the_block() {
		// A write statement has no logical form, so a block writes through a
		// function whose body writes. The call is evaluated against the same
		// `FrozenContext` the streaming bindings are made in, so a LET earlier
		// in the block is visible to it, and the write it performs is visible
		// to the statements after it in the same transaction.
		let db = TestDb::new(
			"DEFINE TABLE person SCHEMALESS;
			 DEFINE FUNCTION fn::create_person($name: string) { CREATE person:tobie SET name = $name; };",
		)
		.await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
		let rows = collect(
			&plan(
				&[
					"LET $name = 'Tobie'",
					"fn::create_person($name)",
					"SELECT VALUE name FROM ONLY person:tobie",
				],
				&ctx,
			)
			.await,
			&ctx,
		)
		.await;
		assert_eq!(rows, vec![Value::from("Tobie")]);
	}
}
