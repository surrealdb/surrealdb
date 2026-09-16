//! ProjectValue operator for SELECT VALUE expressions.
//!
//! The ProjectValue operator evaluates a single expression for each input record
//! and returns the raw values (not wrapped in objects).
//!
//! OMIT is applied internally before expression evaluation so that omitted
//! fields resolve to NONE.  This mirrors the legacy `Results::project_value`
//! path and avoids the ordering hazards of a separate Project-for-OMIT operator.

use std::sync::Arc;

use futures::StreamExt;

use super::project::omit_field_sync;
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, EvalContext, ExecOperator, ExecutionContext,
	FlowResult, OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream, buffer_stream,
	monitor_stream,
};
use crate::expr::ControlFlow;
use crate::expr::logical::LogicalExpr;
use crate::val::Value;

/// ProjectValue operator - evaluates a single expression for each input record.
///
/// Unlike the regular Project operator which produces objects with named fields,
/// ProjectValue returns the raw value of the expression for each record.
/// This is used for `SELECT VALUE expr FROM ...`.
#[derive(Debug, Clone)]
pub struct ProjectValue {
	/// The input plan to project from
	pub input: Arc<dyn ExecOperator>,
	/// The expression to evaluate for each record
	pub expr: Arc<dyn PhysicalExpr>,
	/// Paths to strip before evaluating (SELECT VALUE … OMIT).
	omit: Arc<[LogicalExpr]>,
	/// Whether evaluating the projected expression a second time would expose
	/// anything: it stages no write, advances no counter, reaches nothing
	/// outside the transaction and runs no body the plan cannot see.
	///
	/// This operator holds **one** expression, so unlike its siblings it has
	/// no second column to interleave with and nothing to reorder. What the
	/// flag guards here is a repetition, not an order: the batch shape
	/// evaluates the expression for every row in one call, and a row that
	/// signals `RETURN` sends it back to the first row, since a failed
	/// `evaluate_batch` does not say how far it got — so whatever the rows it
	/// did reach already did happens again. `false`, the default, evaluates a
	/// row at a time, which makes each row's effects happen once.
	///
	/// So the planner sets this where the expression has no effect to
	/// repeat, and also where it cannot signal at all: an expression no
	/// `RETURN` can reach out of is never evaluated a second time, so
	/// repeating is not on the table however much it dereferences. That
	/// second answer is what keeps a graph traversal — effectful, since a
	/// fetch derives the target's `COMPUTED` fields — on the batch shape its
	/// rows need to overlap on.
	pub(crate) repeat_is_free: bool,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl ProjectValue {
	/// Create a new ProjectValue operator with fresh metrics.
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		expr: Arc<dyn PhysicalExpr>,
		omit: Vec<LogicalExpr>,
	) -> Self {
		Self {
			input,
			expr,
			omit: omit.into(),
			repeat_is_free: false,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	/// Declare the projected expression free to evaluate again; see
	/// [`Self::repeat_is_free`]. Only the planner, which holds the expression
	/// as written, can answer this.
	pub(crate) fn with_repeat_is_free(mut self) -> Self {
		self.repeat_is_free = true;
		self
	}
}
impl ExecOperator for ProjectValue {
	fn name(&self) -> &'static str {
		"ProjectValue"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("expr".to_string(), self.expr.to_sql())]
	}

	fn required_context(&self) -> ContextLevel {
		self.expr.required_context().max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		self.input.access_mode().combine(self.expr.access_mode())
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		self.input.cardinality_hint()
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		vec![("expr", &self.expr)]
	}

	fn output_ordering(&self) -> crate::exec::OutputOrdering {
		self.input.output_ordering()
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
		let expr = Arc::clone(&self.expr);
		let omit = Arc::clone(&self.omit);
		let repeat_is_free = self.repeat_is_free;
		let ctx = ctx.clone();

		let projected = input_stream.then(move |batch_result| {
			let expr = Arc::clone(&expr);
			let omit = Arc::clone(&omit);
			let ctx = ctx.clone();

			async move {
				let batch = batch_result?;
				let mut values = batch.into_values();

				if !omit.is_empty() {
					for value in &mut values {
						for field in omit.iter() {
							omit_field_sync(value, field);
						}
					}
				}

				let eval_ctx = EvalContext::from_exec_ctx(&ctx);

				// One batch call is the faster shape — it lets an I/O-bound
				// expression (a subquery, a lookup) overlap its rows — but a
				// batch that a row ends with `RETURN` is evaluated again from
				// its first row, since a failed `evaluate_batch` does not say
				// how far it got. That repeats whatever the rows it did reach
				// already did, which is invisible only where the expression
				// has no effect to repeat; `repeat_is_free` answers that, and
				// where it may, the batch is evaluated a row at a time so each
				// row's effects happen once.
				if repeat_is_free {
					match expr.evaluate_batch(eval_ctx.clone(), &values).await {
						Ok(projected_values) => Ok(ValueBatch::new(projected_values)),
						Err(ControlFlow::Return(_)) => {
							Ok(ValueBatch::new(evaluate_rows(&expr, &eval_ctx, &values).await?))
						}
						Err(e) => Err(e),
					}
				} else {
					Ok(ValueBatch::new(evaluate_rows(&expr, &eval_ctx, &values).await?))
				}
			}
		});

		Ok(monitor_stream(Box::pin(projected), "ProjectValue", &self.metrics))
	}
}

/// Evaluate `expr` for every row of `values` in order, taking the value a row
/// signals `RETURN` with as that row's projection.
///
/// The row is bound as both the current value and the document root, which is
/// what a nested expression — a method's arguments, a dynamic index — is
/// evaluated against, so a row reports the same here as in one batch call.
async fn evaluate_rows<'a>(
	expr: &Arc<dyn PhysicalExpr>,
	ctx: &EvalContext<'a>,
	values: &'a [Value],
) -> Result<Vec<Value>, ControlFlow> {
	let mut projected = Vec::with_capacity(values.len());
	for value in values {
		match expr.evaluate(ctx.with_value_and_doc(value)).await {
			Ok(result) => projected.push(result),
			Err(ControlFlow::Return(v)) => projected.push(v),
			Err(e) => return Err(e),
		}
	}
	Ok(projected)
}
