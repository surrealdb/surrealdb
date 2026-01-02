use std::sync::Arc;

use futures::StreamExt;

use crate::err::Error;
use crate::exec::{
	ContextLevel, EvalContext, ExecutionContext, ExecutionPlan, PhysicalExpr, ValueBatch,
	ValueBatchStream,
};

/// Filters a stream of values based on a predicate.
///
/// Requires database-level context for expression evaluation, and also
/// inherits the context requirements of its input plan.
#[derive(Debug, Clone)]
pub struct Filter {
	pub(crate) input: Arc<dyn ExecutionPlan>,
	pub(crate) predicate: Arc<dyn PhysicalExpr>,
}

impl ExecutionPlan for Filter {
	fn required_context(&self) -> ContextLevel {
		// Filter needs Database for expression evaluation, but also
		// inherits child requirements (take the maximum)
		ContextLevel::Database.max(self.input.required_context())
	}

	fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
		vec![&self.input]
	}

	fn execute(&self, ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
		// Get database context - we declared Database level, so this should succeed
		let db_ctx = ctx.database()?;

		let input_stream = self.input.execute(ctx)?;
		let predicate = self.predicate.clone();

		// Clone all necessary data for the async move closure
		let params = db_ctx.ns_ctx.root.params.clone();
		let ns = Arc::clone(&db_ctx.ns_ctx.ns);
		let db = Arc::clone(&db_ctx.db);
		let txn = db_ctx.ns_ctx.root.txn.clone();
		let auth = db_ctx.ns_ctx.root.auth.clone();
		let auth_enabled = db_ctx.ns_ctx.root.auth_enabled;

		let filtered = input_stream.filter_map(move |batch_result| {
			let predicate = predicate.clone();
			let params = params.clone();
			let txn = txn.clone();
			let ns = ns.clone();
			let db = db.clone();
			let auth = auth.clone();

			async move {
				// Handle errors in the input batch
				let batch = match batch_result {
					Ok(b) => b,
					Err(e) => return Some(Err(e)),
				};

				let mut kept = Vec::new();
				for value in batch.values {
					// Build execution context for expression evaluation
					let exec_ctx = ExecutionContext::Database(crate::exec::DatabaseContext {
						ns_ctx: crate::exec::NamespaceContext {
							root: crate::exec::RootContext {
								datastore: None,
								params: params.clone(),
								cancellation: tokio_util::sync::CancellationToken::new(),
								auth: auth.clone(),
								auth_enabled,
								txn: txn.clone(),
							},
							ns: ns.clone(),
						},
						db: db.clone(),
					});
					let eval_ctx = EvalContext::from_exec_ctx(&exec_ctx).with_value(&value);

					// Evaluate predicate
					match predicate.evaluate(eval_ctx).await {
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
