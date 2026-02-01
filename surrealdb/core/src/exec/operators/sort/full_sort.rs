//! Sort operator - applies ORDER BY to a stream.
//!
//! This is the standard in-memory sort operator that collects all input,
//! sorts it, and emits the sorted results. It uses parallel sorting via
//! rayon on non-WASM platforms for better performance on large datasets.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
#[cfg(not(target_family = "wasm"))]
use rayon::prelude::ParallelSliceMut;
#[cfg(not(target_family = "wasm"))]
use tokio::task::spawn_blocking;

use super::common::{OrderByField, SortDirection, compare_keys};
use crate::exec::{
	AccessMode, CombineAccessModes, ContextLevel, EvalContext, ExecutionContext, FlowResult,
	OperatorPlan, ValueBatch, ValueBatchStream,
};
use crate::val::Value;

/// Sorts the input stream by the specified ORDER BY fields.
///
/// This is a blocking operator - it must collect all input before
/// producing any output, since sorting requires seeing all values.
///
/// On non-WASM platforms, this uses parallel sorting via rayon and
/// executes the sort in a blocking task to avoid blocking the async executor.
#[derive(Debug, Clone)]
pub struct Sort {
	pub(crate) input: Arc<dyn OperatorPlan>,
	pub(crate) order_by: Vec<OrderByField>,
}

#[async_trait]
impl OperatorPlan for Sort {
	fn name(&self) -> &'static str {
		"Sort"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let order_str = self
			.order_by
			.iter()
			.map(|f| {
				let dir = match f.direction {
					SortDirection::Asc => "ASC",
					SortDirection::Desc => "DESC",
				};
				format!("{} {}", f.expr.to_sql(), dir)
			})
			.collect::<Vec<_>>()
			.join(", ");
		vec![("order_by".to_string(), order_str)]
	}

	fn required_context(&self) -> ContextLevel {
		// Sort needs Database for expression evaluation
		ContextLevel::Database.max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		// Combine input's access mode with all ORDER BY expressions
		let expr_mode = self.order_by.iter().map(|f| f.expr.access_mode()).combine_all();
		self.input.access_mode().combine(expr_mode)
	}

	fn children(&self) -> Vec<&Arc<dyn OperatorPlan>> {
		vec![&self.input]
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = self.input.execute(ctx)?;
		let order_by = self.order_by.clone();
		let ctx = ctx.clone();

		// Sort requires collecting all input first, then sorting, then emitting
		let sorted_stream = futures::stream::once(async move {
			// Collect all values from input
			let mut all_values: Vec<Value> = Vec::new();
			futures::pin_mut!(input_stream);
			while let Some(batch_result) = input_stream.next().await {
				match batch_result {
					Ok(batch) => all_values.extend(batch.values),
					Err(e) => return Err(e),
				}
			}

			if all_values.is_empty() {
				return Ok(ValueBatch {
					values: vec![],
				});
			}

			// Pre-compute sort keys for each value
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);
			let mut keyed: Vec<(Vec<Value>, Value)> = Vec::with_capacity(all_values.len());

			for value in all_values {
				let row_ctx = eval_ctx.clone().with_value(&value);
				let mut keys = Vec::with_capacity(order_by.len());

				for field in &order_by {
					let key = field.expr.evaluate(row_ctx.clone()).await.map_err(|e| {
						crate::expr::ControlFlow::Err(anyhow::anyhow!(
							"Sort key evaluation error: {}",
							e
						))
					})?;
					keys.push(key);
				}

				keyed.push((keys, value));
			}

			// Sort the keyed values
			let sorted = sort_keyed_values(keyed, order_by).await?;

			Ok(ValueBatch {
				values: sorted,
			})
		});

		// Filter out empty batches
		let filtered = sorted_stream.filter_map(|result| async move {
			match result {
				Ok(batch) if batch.values.is_empty() => None,
				other => Some(other),
			}
		});

		Ok(Box::pin(filtered))
	}
}

/// Sort keyed values using parallel sort on non-WASM, single-threaded on WASM.
#[cfg(not(target_family = "wasm"))]
async fn sort_keyed_values(
	mut keyed: Vec<(Vec<Value>, Value)>,
	order_by: Vec<OrderByField>,
) -> Result<Vec<Value>, crate::expr::ControlFlow> {
	spawn_blocking(move || {
		keyed.par_sort_unstable_by(|(keys_a, _), (keys_b, _)| {
			compare_keys(keys_a, keys_b, &order_by)
		});
		keyed.into_iter().map(|(_, v)| v).collect()
	})
	.await
	.map_err(|e| crate::expr::ControlFlow::Err(anyhow::anyhow!("Sort error: {}", e)))
}

/// Sort keyed values using single-threaded sort on WASM.
#[cfg(target_family = "wasm")]
async fn sort_keyed_values(
	mut keyed: Vec<(Vec<Value>, Value)>,
	order_by: Vec<OrderByField>,
) -> Result<Vec<Value>, crate::expr::ControlFlow> {
	keyed.sort_by(|(keys_a, _), (keys_b, _)| compare_keys(keys_a, keys_b, &order_by));
	Ok(keyed.into_iter().map(|(_, v)| v).collect())
}
