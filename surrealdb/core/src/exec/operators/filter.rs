use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use tracing::instrument;

use crate::exec::{
	AccessMode, ContextLevel, EvalContext, ExecOperator, ExecutionContext, FlowResult,
	PhysicalExpr, ValueBatch, ValueBatchStream, instrument_stream,
};

/// Filters a stream of values based on a predicate.
///
/// Requires database-level context for expression evaluation, and also
/// inherits the context requirements of its input plan.
#[derive(Debug, Clone)]
pub struct Filter {
	pub(crate) input: Arc<dyn ExecOperator>,
	pub(crate) predicate: Arc<dyn PhysicalExpr>,
}

#[async_trait]
impl ExecOperator for Filter {
	fn name(&self) -> &'static str {
		"Filter"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("predicate".to_string(), self.predicate.to_sql())]
	}

	fn required_context(&self) -> ContextLevel {
		// Filter needs Database for expression evaluation, but also
		// inherits child requirements (take the maximum)
		ContextLevel::Database.max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		// Combine input's access mode with predicate's access mode
		// Predicate could contain a mutation subquery!
		self.input.access_mode().combine(self.predicate.access_mode())
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	#[instrument(name = "Filter::execute", level = "trace", skip_all)]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		// Get database context - we declared Database level, so this should succeed
		// let db_ctx = ctx.database()?;

		let input_stream = self.input.execute(ctx)?;
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

		Ok(instrument_stream(Box::pin(filtered), "Filter"))
	}
}

#[instrument(level = "trace", skip_all)]
async fn filter_batch_in_place(
	batch: &mut ValueBatch,
	predicate: &dyn PhysicalExpr,
	exec_ctx: &ExecutionContext,
) -> FlowResult<()> {
	let mut write_idx = 0;
	for read_idx in 0..batch.values.len() {
		let keep = {
			let eval_ctx = EvalContext::from_exec_ctx(exec_ctx).with_value(&batch.values[read_idx]);
			predicate.evaluate(eval_ctx).await?.is_truthy()
		};

		if keep {
			if write_idx != read_idx {
				batch.values.swap(write_idx, read_idx);
			}
			write_idx += 1;
		}
	}
	batch.values.truncate(write_idx);
	Ok(())
}
