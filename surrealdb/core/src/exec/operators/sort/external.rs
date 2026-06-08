//! ExternalSort operator - disk-based external merge sort.
//!
//! This operator is used when the TEMPFILES keyword is specified in a query.
//! It writes values to temporary files and uses external merge sort to handle
//! datasets that don't fit in memory.
//!
//! This module is only available with the `storage` feature.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use ext_sort::{ExternalSorter, ExternalSorterBuilder, LimitedBufferBuilder};
use futures::StreamExt;
use tempfile::Builder;
use tokio::task::spawn_blocking;

use super::common::{OrderByField, SortDirection, compare_keys};
use super::external_common::{KeyedValue, KeyedValueExternalChunk, TempFileReader, TempFileWriter};
use crate::err::Error;
use crate::exec::{
	AccessMode, CardinalityHint, CombineAccessModes, ContextLevel, EvalContext, ExecOperator,
	ExecutionContext, FlowResult, OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream,
	buffer_stream, monitor_stream,
};
use crate::expr::ControlFlowExt;
use crate::val::Value;

/// External merge sort operator for disk-based sorting.
///
/// This operator writes all input values to temporary files, then uses
/// external merge sort to produce sorted output. This is suitable for
/// large datasets that don't fit in memory.
///
/// Requires the `storage` feature and is activated by the TEMPFILES keyword.
#[derive(Debug, Clone)]
pub struct ExternalSort {
	pub(crate) input: Arc<dyn ExecOperator>,
	pub(crate) order_by: Vec<OrderByField>,
	pub(crate) temp_dir: PathBuf,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl ExternalSort {
	/// Create a new ExternalSort operator.
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		order_by: Vec<OrderByField>,
		temp_dir: PathBuf,
	) -> Self {
		Self {
			input,
			order_by,
			temp_dir,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}
impl ExecOperator for ExternalSort {
	fn name(&self) -> &'static str {
		"ExternalSort"
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
		// `temp_dir` is intentionally omitted from the rendered attrs: the
		// concrete path is platform-dependent (`/tmp` on Linux,
		// `/var/folders/...` on macOS) and adds no diagnostic value beyond
		// "this sort spills to disk", which the operator name already says.
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
			ctx.root().ctx.config.operator_buffer_size,
		);
		let order_by = Arc::new(self.order_by.clone());
		let temp_dir = self.temp_dir.clone();
		let ctx = ctx.clone();

		let sorted_stream = futures::stream::once(async move {
			// Create temp directory for this sort operation
			let dir = Builder::new()
				.prefix("SURREAL_SORT")
				.tempdir_in(&temp_dir)
				.context("Failed to create temp directory")?;

			// Collect all values and compute sort keys, writing to temp files
			let mut writer =
				TempFileWriter::new(&dir).context("Failed to create temp file writer")?;

			let eval_ctx = EvalContext::from_exec_ctx(&ctx);
			let mut count = 0usize;

			futures::pin_mut!(input_stream);
			while let Some(batch_result) = input_stream.next().await {
				// Check for cancellation between batches
				if ctx.cancellation().is_cancelled() {
					return Err(crate::expr::ControlFlow::Err(anyhow::anyhow!(
						Error::QueryCancelled
					)));
				}
				let batch = match batch_result {
					Ok(b) => b,
					Err(e) => return Err(e),
				};

				// Batch evaluate sort key expressions per-field
				let num_fields = order_by.len();
				let mut key_columns: Vec<Vec<Value>> = Vec::with_capacity(num_fields);
				for field in order_by.iter() {
					let keys = field.expr.evaluate_batch(eval_ctx.clone(), &batch.values).await?;
					key_columns.push(keys);
				}

				// Transpose column-oriented keys to per-row, then write to temp files
				let mut key_iters: Vec<std::vec::IntoIter<Value>> =
					key_columns.into_iter().map(|col| col.into_iter()).collect();

				for value in batch.values {
					let keys: Vec<Value> = key_iters
						.iter_mut()
						.map(|iter| iter.next().expect("key column length matches batch size"))
						.collect();

					// Write keyed value to temp file
					let keyed = KeyedValue {
						keys,
						value,
					};

					// Use spawn_blocking for file I/O
					let mut w = writer;
					w = spawn_blocking(move || {
						w.push(&keyed)?;
						Ok::<TempFileWriter, Error>(w)
					})
					.await
					.context("Write task join error")?
					.context("Write error")?;
					writer = w;

					count += 1;
				}
			}

			if count == 0 {
				return Ok(ValueBatch {
					values: vec![],
				});
			}

			// Flush and prepare for reading
			writer.flush().context("Flush error")?;

			let reader =
				TempFileReader::new(count, &dir).context("Failed to create temp file reader")?;

			// Create sort directory
			let sort_dir = dir.path().join("sort");

			// Perform external sort
			let order_by_clone = Arc::clone(&order_by);
			let sorted = spawn_blocking(move || {
				fs::create_dir(&sort_dir)?;

				let sorter: ExternalSorter<
					KeyedValue,
					Error,
					LimitedBufferBuilder,
					KeyedValueExternalChunk,
				> = ExternalSorterBuilder::new()
					.with_tmp_dir(&sort_dir)
					.with_buffer(LimitedBufferBuilder::new(
						ctx.root().ctx.config.external_sorting_buffer_limit,
						true,
					))
					.build()?;

				let sorted = sorter
					.sort_by(reader, |a, b| compare_keys(&a.keys, &b.keys, &order_by_clone))?;

				// Collect sorted values
				let values: Vec<Value> =
					sorted.map(|r| r.map(|kv| kv.value)).collect::<Result<Vec<_>, _>>()?;

				Ok::<Vec<Value>, Error>(values)
			})
			.await
			.context("Sort task join error")?
			.context("Sort error")?;

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

		Ok(monitor_stream(Box::pin(filtered), "ExternalSort", &self.metrics))
	}
}
