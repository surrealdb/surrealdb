//! ProjectValue operator for SELECT VALUE expressions.
//!
//! The ProjectValue operator evaluates a single expression for each input record
//! and returns the raw values (not wrapped in objects).

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, EvalContext, ExecOperator, ExecutionContext,
	FlowResult, OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream, buffer_stream,
	monitor_stream,
};
use crate::expr::ControlFlow;

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
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl ProjectValue {
	/// Create a new ProjectValue operator with fresh metrics.
	pub(crate) fn new(input: Arc<dyn ExecOperator>, expr: Arc<dyn PhysicalExpr>) -> Self {
		Self {
			input,
			expr,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for ProjectValue {
	fn name(&self) -> &'static str {
		"ProjectValue"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("expr".to_string(), self.expr.to_sql())]
	}

	fn required_context(&self) -> ContextLevel {
		// Combine the expression's context with the input's context
		self.expr.required_context().max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		// Combine input's mode with expression's mode
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
			ctx.ctx().config().limits.operator_buffer_size,
		);
		let expr = self.expr.clone();
		let ctx = ctx.clone();

		let projected = input_stream.then(move |batch_result| {
			let expr = expr.clone();
			let ctx = ctx.clone();

			async move {
				let batch = batch_result?;
				let eval_ctx = EvalContext::from_exec_ctx(&ctx);

				// Try batch evaluation first for better throughput.
				// Falls back to per-row evaluation when RETURN signals
				// are encountered (rare -- only from explicit RETURN
				// statements in function bodies).
				match expr.evaluate_batch(eval_ctx.clone(), &batch.values).await {
					Ok(projected_values) => Ok(ValueBatch {
						values: projected_values,
					}),
					Err(ControlFlow::Return(_)) => {
						// Batch hit a RETURN signal; re-evaluate per-row
						// so we can catch RETURN as a value.
						let mut projected_values = Vec::with_capacity(batch.values.len());
						for value in &batch.values {
							match expr.evaluate(eval_ctx.with_value(value)).await {
								Ok(result) => projected_values.push(result),
								Err(ControlFlow::Return(v)) => projected_values.push(v),
								Err(e) => return Err(e),
							}
						}
						Ok(ValueBatch {
							values: projected_values,
						})
					}
					Err(e) => Err(e),
				}
			}
		});

		Ok(monitor_stream(Box::pin(projected), "ProjectValue", &self.metrics))
	}
}
