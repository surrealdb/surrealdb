//! `ExternalSortByKey` — disk-backed sort for the consolidated SELECT path.
//!
//! Sibling of [`super::external::ExternalSort`]. The disk format is identical;
//! the only difference is key extraction: `ExternalSort` evaluates a
//! `PhysicalExpr` per row, while this operator extracts pre-computed columns
//! via [`crate::exec::field_path::FieldPath`]. The consolidated path
//! ([`crate::exec::planner::select::pipeline::Planner::plan_sort_consolidated`])
//! has already materialised any complex sort expression as a row column via the
//! `Compute` operator, so we don't pay the per-row eval cost a second time.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use ext_sort::{ExternalSorter, ExternalSorterBuilder, LimitedBufferBuilder};
use futures::StreamExt;
use tempfile::Builder;
use tokio::task::spawn_blocking;

use super::common::{SortDirection, SortKey, compare_keys_by_sort_key};
use super::external_common::{KeyedValue, KeyedValueExternalChunk, TempFileReader, TempFileWriter};
use crate::err::Error;
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::expr::ControlFlowExt;
use crate::val::Value;

/// External merge sort keyed on pre-extracted `FieldPath` values.
#[derive(Debug, Clone)]
pub struct ExternalSortByKey {
	pub(crate) input: Arc<dyn ExecOperator>,
	pub(crate) sort_keys: Vec<SortKey>,
	pub(crate) temp_dir: PathBuf,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl ExternalSortByKey {
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		sort_keys: Vec<SortKey>,
		temp_dir: PathBuf,
	) -> Self {
		Self {
			input,
			sort_keys,
			temp_dir,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}
impl ExecOperator for ExternalSortByKey {
	fn name(&self) -> &'static str {
		"ExternalSortByKey"
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
		// `temp_dir` is intentionally omitted from the rendered attrs: the
		// concrete path is platform-dependent and adds no diagnostic value
		// beyond "this sort spills to disk".
		vec![("sort_keys".to_string(), order_str)]
	}

	fn required_context(&self) -> ContextLevel {
		// Pure field-path extraction adds no requirement of its own — inherit
		// from the input operator only.
		self.input.required_context()
	}

	fn access_mode(&self) -> AccessMode {
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
			ctx.root().ctx.config.operator_buffer_size,
		);
		let sort_keys = Arc::new(self.sort_keys.clone());
		let temp_dir = self.temp_dir.clone();
		let ctx = ctx.clone();

		let sorted_stream = futures::stream::once(async move {
			let dir = Builder::new()
				.prefix("SURREAL_SORT")
				.tempdir_in(&temp_dir)
				.context("Failed to create temp directory")?;

			let mut writer =
				TempFileWriter::new(&dir).context("Failed to create temp file writer")?;
			let mut count = 0usize;

			futures::pin_mut!(input_stream);
			while let Some(batch_result) = input_stream.next().await {
				if ctx.cancellation().is_cancelled() {
					return Err(crate::expr::ControlFlow::Err(anyhow::anyhow!(
						Error::QueryCancelled
					)));
				}
				let batch = match batch_result {
					Ok(b) => b,
					Err(e) => return Err(e),
				};

				for value in batch.values {
					let keys: Vec<Value> =
						sort_keys.iter().map(|k| k.path.extract(&value)).collect();

					let keyed = KeyedValue {
						keys,
						value,
					};

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

			writer.flush().context("Flush error")?;

			let reader =
				TempFileReader::new(count, &dir).context("Failed to create temp file reader")?;
			let sort_dir = dir.path().join("sort");

			let sort_keys_clone = Arc::clone(&sort_keys);
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

				let sorted = sorter.sort_by(reader, |a, b| {
					compare_keys_by_sort_key(&a.keys, &b.keys, &sort_keys_clone)
				})?;

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

		let filtered = sorted_stream.filter_map(|result| async move {
			match result {
				Ok(batch) if batch.values.is_empty() => None,
				other => Some(other),
			}
		});

		Ok(monitor_stream(Box::pin(filtered), "ExternalSortByKey", &self.metrics))
	}
}
