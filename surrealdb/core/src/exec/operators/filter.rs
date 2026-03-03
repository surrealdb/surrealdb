//! Filter operator for WHERE clause processing.
//!
//! Applies a predicate expression to each row in the input stream,
//! retaining only rows for which the predicate evaluates to a truthy value.
//! Uses batch evaluation for efficiency.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use tracing::instrument;

use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, EvalContext, ExecOperator, ExecutionContext,
	FlowResult, OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream, buffer_stream,
	monitor_stream,
};

/// Filters a stream of values based on a predicate.
///
/// Requires database-level context for expression evaluation, and also
/// inherits the context requirements of its input plan.
#[derive(Debug, Clone)]
pub struct Filter {
	pub(crate) input: Arc<dyn ExecOperator>,
	pub(crate) predicate: Arc<dyn PhysicalExpr>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl Filter {
	/// Create a new Filter operator with fresh metrics.
	pub(crate) fn new(input: Arc<dyn ExecOperator>, predicate: Arc<dyn PhysicalExpr>) -> Self {
		Self {
			input,
			predicate,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for Filter {
	fn name(&self) -> &'static str {
		"Filter"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("predicate".to_string(), self.predicate.to_sql())]
	}

	fn required_context(&self) -> ContextLevel {
		// Combine predicate expression context with child operator context
		self.predicate.required_context().max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		// Combine input's access mode with predicate's access mode
		// Predicate could contain a mutation subquery!
		self.input.access_mode().combine(self.predicate.access_mode())
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		self.input.cardinality_hint()
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		vec![("predicate", &self.predicate)]
	}

	fn output_ordering(&self) -> crate::exec::OutputOrdering {
		self.input.output_ordering()
	}

	#[instrument(name = "Filter::execute", level = "trace", skip_all)]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.ctx().config().limits.operator_buffer_size,
		);
		let predicate = Arc::clone(&self.predicate);

		// Clone all necessary data for the async move closure
		let ctx = ctx.clone();

		let filtered = input_stream.filter_map(move |batch_result| {
			let predicate = predicate.clone();

			let exec_ctx = ctx.clone();

			async move {
				// Handle errors in the input batch
				let mut batch = match batch_result {
					Ok(b) => b,
					Err(e) => return Some(Err(e)),
				};

				if let Err(err) = filter_batch_in_place(&mut batch, &*predicate, &exec_ctx).await {
					return Some(Err(err));
				}

				// Only emit non-empty batches
				if batch.values.is_empty() {
					None
				} else {
					Some(Ok(batch))
				}
			}
		});

		Ok(monitor_stream(Box::pin(filtered), "Filter", &self.metrics))
	}
}

#[instrument(level = "trace", skip_all)]
async fn filter_batch_in_place(
	batch: &mut ValueBatch,
	predicate: &dyn PhysicalExpr,
	exec_ctx: &ExecutionContext,
) -> FlowResult<()> {
	let eval_ctx = EvalContext::from_exec_ctx(exec_ctx);
	let results = predicate.evaluate_batch(eval_ctx, &batch.values).await?;

	let mut write_idx = 0;
	for (read_idx, result) in results.into_iter().enumerate() {
		if result.is_truthy() {
			if write_idx != read_idx {
				batch.values.swap(write_idx, read_idx);
			}
			write_idx += 1;
		}
	}
	batch.values.truncate(write_idx);
	Ok(())
}
