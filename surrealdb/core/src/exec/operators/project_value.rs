//! ProjectValue operator for SELECT VALUE expressions.
//!
//! The ProjectValue operator evaluates a single expression for each input record
//! and returns the raw values (not wrapped in objects).

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::exec::{
	AccessMode, ContextLevel, EvalContext, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream, monitor_stream,
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
		// ProjectValue needs the same context as its input, plus whatever the expression needs
		self.input.required_context()
	}

	fn access_mode(&self) -> AccessMode {
		// Combine input's mode with expression's mode
		self.input.access_mode().combine(self.expr.access_mode())
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = self.input.execute(ctx)?;
		let expr = self.expr.clone();
		let ctx = ctx.clone();

		let projected = input_stream.then(move |batch_result| {
			let expr = expr.clone();
			let ctx = ctx.clone();

			async move {
				let batch = batch_result?;
				let mut projected_values = Vec::with_capacity(batch.values.len());

				for value in batch.values {
					let eval_ctx = EvalContext::from_exec_ctx(&ctx).with_value(&value);

					match expr.evaluate(eval_ctx).await {
						Ok(result) => projected_values.push(result),
						Err(ControlFlow::Return(v)) => projected_values.push(v),
						Err(e) => return Err(e),
					}
				}

				Ok(ValueBatch {
					values: projected_values,
				})
			}
		});

		Ok(monitor_stream(Box::pin(projected), "ProjectValue", &self.metrics))
	}
}
