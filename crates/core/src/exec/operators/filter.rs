use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::err::Error;
use crate::exec::{
	AccessMode, ContextLevel, EvalContext, ExecutionContext, OperatorPlan, PhysicalExpr,
	ValueBatch, ValueBatchStream,
};

/// Filters a stream of values based on a predicate.
///
/// Requires database-level context for expression evaluation, and also
/// inherits the context requirements of its input plan.
#[derive(Debug, Clone)]
pub struct Filter {
	pub(crate) input: Arc<dyn OperatorPlan>,
	pub(crate) predicate: Arc<dyn PhysicalExpr>,
}

#[async_trait]
impl OperatorPlan for Filter {
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

	fn children(&self) -> Vec<&Arc<dyn OperatorPlan>> {
		vec![&self.input]
	}

	fn execute(&self, ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
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

				// In-place filtering using swap-and-truncate (zero allocation)
				let mut write_idx = 0;
				for read_idx in 0..batch.values.len() {
					// Scope the borrow so it ends before the swap
					let keep = {
						let eval_ctx = EvalContext::from_exec_ctx(&exec_ctx)
							.with_value(&batch.values[read_idx]);
						match predicate.evaluate(eval_ctx).await {
							Ok(pred) => pred.is_truthy(),
							Err(e) => {
								use crate::expr::ControlFlow;
								return Some(Err(ControlFlow::Err(anyhow::anyhow!(
									"Filter predicate error: {}",
									e
								))));
							}
						}
					};

					if keep {
						if write_idx != read_idx {
							batch.values.swap(write_idx, read_idx);
						}
						write_idx += 1;
					}
				}
				batch.values.truncate(write_idx);

				// Only emit non-empty batches
				if batch.values.is_empty() {
					None
				} else {
					Some(Ok(batch))
				}
			}
		});

		Ok(Box::pin(filtered))
	}
}
