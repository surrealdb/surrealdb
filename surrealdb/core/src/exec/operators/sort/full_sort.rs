//! Sort operator - applies ORDER BY to a stream.
//!
//! This is the standard in-memory sort operator that collects all input,
//! sorts it, and emits the sorted results. It uses parallel sorting via
//! rayon on non-WASM platforms for better performance on large datasets.
//!
//! Two modes are supported:
//! - `Sort`: Legacy mode that evaluates expressions for each row
//! - `SortByKey`: New mode that references pre-computed fields by name

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
#[cfg(not(target_family = "wasm"))]
use rayon::prelude::ParallelSliceMut;
#[cfg(not(target_family = "wasm"))]
use tokio::task::spawn_blocking;

use super::common::{OrderByField, SortDirection, SortKey, compare_keys, compare_records_by_keys};
use crate::exec::{
	AccessMode, CardinalityHint, CombineAccessModes, ContextLevel, EvalContext, ExecOperator,
	ExecutionContext, FlowResult, OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream,
	buffer_stream, monitor_stream,
};
#[cfg(not(target_family = "wasm"))]
use crate::expr::ControlFlowExt;
use crate::val::Value;

/// Sorts the input stream by the specified ORDER BY fields.
///
/// This is a blocking operator - it must collect all input before
/// producing any output, since sorting requires seeing all values.
///
/// On non-WASM platforms, this uses parallel sorting via rayon and
/// executes the sort in a blocking task to avoid blocking the async executor.
///
/// **Note**: This operator evaluates expressions for each row. For the
/// consolidated approach where expressions are pre-computed by a Compute
/// operator, use `SortByKey` instead.
#[derive(Debug, Clone)]
pub struct Sort {
	pub(crate) input: Arc<dyn ExecOperator>,
	pub(crate) order_by: Vec<OrderByField>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl Sort {
	/// Create a new Sort operator.
	pub(crate) fn new(input: Arc<dyn ExecOperator>, order_by: Vec<OrderByField>) -> Self {
		Self {
			input,
			order_by,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for Sort {
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
		// Combine order-by expression contexts with child operator context
		let order_ctx = self
			.order_by
			.iter()
			.map(|f| f.expr.required_context())
			.max()
			.unwrap_or(ContextLevel::Root);
		order_ctx.max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		// Combine input's access mode with all ORDER BY expressions
		let expr_mode = self.order_by.iter().map(|f| f.expr.access_mode()).combine_all();
		self.input.access_mode().combine(expr_mode)
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
		self.order_by.iter().map(|f| ("order_by", &f.expr)).collect()
	}

	fn output_ordering(&self) -> crate::exec::OutputOrdering {
		use crate::exec::ordering::SortProperty;
		crate::exec::OutputOrdering::Sorted(
			self.order_by
				.iter()
				.map(|f| {
					// Try to extract a FieldPath from the expression's SQL representation.
					// This is best-effort -- complex expressions won't match.
					let sql = f.expr.to_sql();
					let path = crate::exec::field_path::FieldPath::field(sql);
					SortProperty {
						path,
						direction: f.direction,
						collate: f.collate,
						numeric: f.numeric,
					}
				})
				.collect(),
		)
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.ctx().config().limits.operator_buffer_size,
		);
		let order_by = self.order_by.clone();
		let ctx = ctx.clone();

		// Sort requires collecting all input first, then sorting, then emitting
		let sorted_stream = futures::stream::once(async move {
			// Collect all values from input
			let mut all_values: Vec<Value> = Vec::new();
			futures::pin_mut!(input_stream);
			while let Some(batch_result) = input_stream.next().await {
				// Check for cancellation between batches
				if ctx.cancellation().is_cancelled() {
					return Err(crate::expr::ControlFlow::Err(anyhow::anyhow!(
						crate::err::Error::QueryCancelled
					)));
				}
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

			// Pre-compute sort keys using per-field batch evaluation
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);

			// Evaluate each sort key expression across all rows in one batch call
			let num_fields = order_by.len();
			let num_values = all_values.len();
			let mut key_columns: Vec<Vec<Value>> = Vec::with_capacity(num_fields);
			for field in &order_by {
				let keys = field.expr.evaluate_batch(eval_ctx.clone(), &all_values).await?;
				key_columns.push(keys);
			}

			// Transpose from column-oriented to row-oriented keyed tuples,
			// consuming the column vectors to avoid cloning values.
			let mut key_iters: Vec<std::vec::IntoIter<Value>> =
				key_columns.into_iter().map(|col| col.into_iter()).collect();
			let mut keyed: Vec<(Vec<Value>, Value)> = Vec::with_capacity(num_values);
			for value in all_values {
				let keys: Vec<Value> = key_iters
					.iter_mut()
					.map(|iter| iter.next().expect("key column length matches batch size"))
					.collect();
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

		Ok(monitor_stream(Box::pin(filtered), "Sort", &self.metrics))
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
	.context("Sort error")
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

// ============================================================================
// SortByKey - New consolidated approach
// ============================================================================

/// Sorts the input stream by pre-computed field values.
///
/// This is the new consolidated approach where:
/// 1. Complex expressions are computed once by a Compute operator
/// 2. Sort references those computed values by field name
/// 3. No duplicate expression evaluation occurs
///
/// This is a blocking operator that collects all input, sorts it, then emits.
///
/// On non-WASM platforms, this uses parallel sorting via rayon.
#[derive(Debug, Clone)]
pub struct SortByKey {
	pub(crate) input: Arc<dyn ExecOperator>,
	pub(crate) sort_keys: Vec<SortKey>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl SortByKey {
	/// Create a new SortByKey operator.
	pub(crate) fn new(input: Arc<dyn ExecOperator>, sort_keys: Vec<SortKey>) -> Self {
		Self {
			input,
			sort_keys,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for SortByKey {
	fn name(&self) -> &'static str {
		"SortByKey"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let order_str = self
			.sort_keys
			.iter()
			.map(|k| {
				let dir = match k.direction {
					SortDirection::Asc => "ASC",
					SortDirection::Desc => "DESC",
				};
				format!("{} {}", k.path, dir)
			})
			.collect::<Vec<_>>()
			.join(", ");
		vec![("sort_keys".to_string(), order_str)]
	}

	fn required_context(&self) -> ContextLevel {
		// SortByKey doesn't evaluate expressions, just compares values
		// But we still inherit child requirements
		self.input.required_context()
	}

	fn access_mode(&self) -> AccessMode {
		// SortByKey is pure comparison - inherits input's access mode
		self.input.access_mode()
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

	fn output_ordering(&self) -> crate::exec::OutputOrdering {
		use crate::exec::ordering::SortProperty;
		crate::exec::OutputOrdering::Sorted(
			self.sort_keys
				.iter()
				.map(|k| SortProperty {
					path: k.path.clone(),
					direction: k.direction,
					collate: k.collate,
					numeric: k.numeric,
				})
				.collect(),
		)
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.ctx().config().limits.operator_buffer_size,
		);
		let sort_keys = self.sort_keys.clone();
		let cancellation = ctx.cancellation().clone();

		// Sort requires collecting all input first, then sorting, then emitting
		let sorted_stream = futures::stream::once(async move {
			// Collect all values from input
			let mut all_values: Vec<Value> = Vec::new();
			futures::pin_mut!(input_stream);
			while let Some(batch_result) = input_stream.next().await {
				// Check for cancellation between batches
				if cancellation.is_cancelled() {
					return Err(crate::expr::ControlFlow::Err(anyhow::anyhow!(
						crate::err::Error::QueryCancelled
					)));
				}
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

			// Sort by extracting field values (no expression evaluation)
			let sorted = sort_by_keys(all_values, sort_keys).await?;

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

		Ok(monitor_stream(Box::pin(filtered), "SortByKey", &self.metrics))
	}
}

/// Sort values by extracting fields and comparing.
#[cfg(not(target_family = "wasm"))]
async fn sort_by_keys(
	mut values: Vec<Value>,
	sort_keys: Vec<SortKey>,
) -> Result<Vec<Value>, crate::expr::ControlFlow> {
	spawn_blocking(move || {
		values.par_sort_unstable_by(|a, b| compare_records_by_keys(a, b, &sort_keys));
		values
	})
	.await
	.context("Sort error")
}

/// Sort values by extracting fields and comparing (WASM version).
#[cfg(target_family = "wasm")]
async fn sort_by_keys(
	mut values: Vec<Value>,
	sort_keys: Vec<SortKey>,
) -> Result<Vec<Value>, crate::expr::ControlFlow> {
	values.sort_by(|a, b| compare_records_by_keys(a, b, &sort_keys));
	Ok(values)
}
