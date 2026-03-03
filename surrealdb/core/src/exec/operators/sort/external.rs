//! ExternalSort operator - disk-based external merge sort.
//!
//! This operator is used when the TEMPFILES keyword is specified in a query.
//! It writes values to temporary files and uses external merge sort to handle
//! datasets that don't fit in memory.
//!
//! This module is only available with the `storage` feature.

use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Take, Write};
use std::mem;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use ext_sort::{ExternalChunk, ExternalSorter, ExternalSorterBuilder, LimitedBufferBuilder};
use futures::StreamExt;
use revision::{DeserializeRevisioned, SerializeRevisioned};
use tempfile::{Builder, TempDir};
use tokio::task::spawn_blocking;

use super::common::{OrderByField, SortDirection, compare_keys};
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

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
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
		vec![
			("order_by".to_string(), order_str),
			("temp_dir".to_string(), self.temp_dir.display().to_string()),
		]
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
			ctx.ctx().config().limits.operator_buffer_size,
		);
		let order_by = Arc::new(self.order_by.clone());
		let temp_dir = self.temp_dir.clone();
		let external_sorting_buffer_limit = ctx.ctx().config().limits.external_sorting_buffer_limit;
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
						w.push(keyed)?;
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
			let order_by_clone = order_by.clone();
			let sorted = spawn_blocking(move || {
				fs::create_dir(&sort_dir)?;

				let sorter: ExternalSorter<
					KeyedValue,
					Error,
					LimitedBufferBuilder,
					KeyedValueExternalChunk,
				> = ExternalSorterBuilder::new()
					.with_tmp_dir(&sort_dir)
					.with_buffer(LimitedBufferBuilder::new(external_sorting_buffer_limit, true))
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

/// A value with pre-computed sort keys for external sorting.
#[derive(Debug, Clone)]
struct KeyedValue {
	keys: Vec<Value>,
	value: Value,
}

const USIZE_SIZE: usize = mem::size_of::<usize>();

/// Writer for temporary files during external sort.
struct TempFileWriter {
	records: BufWriter<File>,
}

impl TempFileWriter {
	const RECORDS_FILE_NAME: &'static str = "records";

	fn new(dir: &TempDir) -> Result<Self, Error> {
		let records = OpenOptions::new()
			.create_new(true)
			.append(true)
			.open(dir.path().join(Self::RECORDS_FILE_NAME))?;
		Ok(Self {
			records: BufWriter::new(records),
		})
	}

	fn write_usize<W: Write>(writer: &mut W, u: usize) -> Result<(), Error> {
		let buf = u.to_be_bytes();
		writer.write_all(&buf)?;
		Ok(())
	}

	fn write_value<W: Write>(writer: &mut W, value: &Value) -> Result<usize, Error> {
		let mut val = Vec::new();
		SerializeRevisioned::serialize_revisioned(value, &mut val)?;
		Self::write_usize(writer, val.len())?;
		writer.write_all(&val)?;
		Ok(val.len())
	}

	fn push(&mut self, keyed: KeyedValue) -> Result<(), Error> {
		// Write number of keys
		Self::write_usize(&mut self.records, keyed.keys.len())?;
		// Write each key
		for key in &keyed.keys {
			Self::write_value(&mut self.records, key)?;
		}
		// Write the value
		Self::write_value(&mut self.records, &keyed.value)?;
		Ok(())
	}

	fn flush(mut self) -> Result<(), Error> {
		self.records.flush()?;
		Ok(())
	}
}

/// Reader for temporary files during external sort.
struct TempFileReader {
	len: usize,
	records_path: PathBuf,
}

impl TempFileReader {
	fn new(len: usize, dir: &TempDir) -> Result<Self, Error> {
		Ok(Self {
			len,
			records_path: dir.path().join(TempFileWriter::RECORDS_FILE_NAME),
		})
	}
}

impl IntoIterator for TempFileReader {
	type Item = Result<KeyedValue, Error>;
	type IntoIter = TempFileIterator;

	fn into_iter(self) -> Self::IntoIter {
		TempFileIterator::new(self.records_path, self.len)
	}
}

/// Iterator over temporary file records.
struct TempFileIterator {
	path: PathBuf,
	reader: Option<BufReader<File>>,
	len: usize,
	pos: usize,
}

impl TempFileIterator {
	fn new(path: PathBuf, len: usize) -> Self {
		Self {
			path,
			reader: None,
			len,
			pos: 0,
		}
	}

	fn check_reader(&mut self) -> Result<(), Error> {
		if self.reader.is_none() {
			let f = OpenOptions::new().read(true).open(&self.path)?;
			self.reader = Some(BufReader::new(f));
		}
		Ok(())
	}

	fn read_usize<R: Read>(reader: &mut R) -> Result<usize, std::io::Error> {
		let mut buf = vec![0u8; USIZE_SIZE];
		reader.read_exact(&mut buf)?;
		Ok(usize::from_be_bytes(buf.try_into().expect("buffer size matches usize")))
	}

	fn read_value<R: Read>(reader: &mut R) -> Result<Value, Error> {
		let len = Self::read_usize(reader)?;
		let mut buf = vec![0u8; len];
		reader.read_exact(&mut buf)?;
		let val: Value = DeserializeRevisioned::deserialize_revisioned(&mut buf.as_slice())?;
		Ok(val)
	}

	fn read_keyed_value<R: Read>(reader: &mut R) -> Result<KeyedValue, Error> {
		let num_keys = Self::read_usize(reader)?;
		let mut keys = Vec::with_capacity(num_keys);
		for _ in 0..num_keys {
			keys.push(Self::read_value(reader)?);
		}
		let value = Self::read_value(reader)?;
		Ok(KeyedValue {
			keys,
			value,
		})
	}
}

impl Iterator for TempFileIterator {
	type Item = Result<KeyedValue, Error>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.pos == self.len {
			return None;
		}
		if let Err(e) = self.check_reader() {
			return Some(Err(e));
		}
		if let Some(reader) = &mut self.reader {
			match Self::read_keyed_value(reader) {
				Ok(val) => {
					self.pos += 1;
					Some(Ok(val))
				}
				Err(e) => Some(Err(e)),
			}
		} else {
			None
		}
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		(self.len - self.pos, Some(self.len - self.pos))
	}
}

impl ExactSizeIterator for TempFileIterator {
	fn len(&self) -> usize {
		self.len - self.pos
	}
}

/// External chunk implementation for KeyedValue.
struct KeyedValueExternalChunk {
	reader: Take<BufReader<File>>,
}

impl ExternalChunk<KeyedValue> for KeyedValueExternalChunk {
	type SerializationError = Error;
	type DeserializationError = Error;

	fn new(reader: Take<BufReader<File>>) -> Self {
		Self {
			reader,
		}
	}

	fn dump(
		chunk_writer: &mut BufWriter<File>,
		items: impl IntoIterator<Item = KeyedValue>,
	) -> Result<(), Self::SerializationError> {
		for item in items {
			// Write number of keys
			TempFileWriter::write_usize(chunk_writer, item.keys.len())?;
			// Write each key
			for key in &item.keys {
				TempFileWriter::write_value(chunk_writer, key)?;
			}
			// Write the value
			TempFileWriter::write_value(chunk_writer, &item.value)?;
		}
		Ok(())
	}
}

impl Iterator for KeyedValueExternalChunk {
	type Item = Result<KeyedValue, Error>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.reader.limit() == 0 {
			None
		} else {
			match TempFileIterator::read_keyed_value(&mut self.reader) {
				Ok(val) => Some(Ok(val)),
				Err(err) => Some(Err(err)),
			}
		}
	}
}
