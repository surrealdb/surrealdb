use std::sync::Arc;

use futures::StreamExt;

use crate::err::Error;
use crate::exec::{
	EvalContext, ExecutionContext, ExecutionPlan, PhysicalExpr, ValueBatch, ValueBatchStream,
};

/// Filters a stream of values based on a predicate
#[derive(Debug, Clone)]
pub struct Filter {
	pub(crate) input: Arc<dyn ExecutionPlan>,
	pub(crate) predicate: Arc<dyn PhysicalExpr>,
}

impl ExecutionPlan for Filter {
	fn execute(&self, ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
		let input_stream = self.input.execute(ctx)?;
		let predicate = self.predicate.clone();
		let params = ctx.params.clone();
		let ns = ctx.ns.clone();
		let db = ctx.db.clone();
		let txn = ctx.txn.clone();

		let filtered = input_stream.filter_map(move |batch_result| {
			let predicate = predicate.clone();
			let params = params.clone();
			let ns = ns.clone();
			let db = db.clone();
			let txn = txn.clone();

			async move {
				// Handle errors in the input batch
				let batch = match batch_result {
					Ok(b) => b,
					Err(e) => return Some(Err(e)),
				};

				// Create evaluation context
				let eval_ctx =
					EvalContext::scalar(&params, Some(&ns), Some(&db), Some(&txn.as_ref()));

				let mut kept = Vec::new();
				for value in batch.values {
					// Create per-row context
					let row_ctx = eval_ctx.with_value(&value);

					// Evaluate predicate
					match predicate.evaluate(&row_ctx).await {
						Ok(result) => {
							// Check if result is truthy
							if is_truthy(&result) {
								kept.push(value);
							}
						}
						Err(e) => {
							use crate::expr::ControlFlow;
							return Some(Err(ControlFlow::Err(anyhow::anyhow!(
								"Filter predicate error: {}",
								e
							))));
						}
					}
				}

				// Only emit non-empty batches
				if kept.is_empty() {
					None
				} else {
					Some(Ok(ValueBatch {
						values: kept,
					}))
				}
			}
		});

		Ok(Box::pin(filtered))
	}
}

/// Check if a value is truthy
fn is_truthy(value: &crate::val::Value) -> bool {
	use crate::val::Value;

	match value {
		Value::None | Value::Null => false,
		Value::Bool(b) => *b,
		Value::Number(n) => !n.is_zero(),
		Value::String(s) => !s.is_empty(),
		Value::Array(a) => !a.is_empty(),
		Value::Object(o) => !o.is_empty(),
		_ => true,
	}
}
